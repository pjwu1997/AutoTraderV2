import os
import websocket
import json
import threading
import atexit
import time
from datetime import datetime, timedelta
from websocket_controller import DistributedWebSocketController
from apscheduler.triggers.cron import CronTrigger
import logging
from logging.handlers import RotatingFileHandler

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler('liquidation_websocket.log', maxBytes=10*1024*1024, backupCount=5),  # Rotate logs at 10MB
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger('liquidation_websocket')

# Custom JSON formatter for structured logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.msg,
            'logger': record.name,
            'symbol': getattr(record, 'symbol', None),  # Add symbol as metadata if available
            'operation': getattr(record, 'operation', None),  # Add operation type if available
            'slave_id': getattr(record, 'slave_id', None),  # Add slave ID if available
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

class DistributedLiquidationWebSocket(DistributedWebSocketController):
    def __init__(self, symbols: list = None, slave_id: str = None):
        # Initialize parent with distributed system configuration
        super().__init__(symbols, slave_id)
        
        # Read liquidation-specific environment variables
        self.liquidation_ws_url = os.getenv("LIQUIDATION_WS_URL", "wss://fstream.binance.com/ws/!forceOrder@arr")
        self.cleanup_minutes = int(os.getenv("LIQUIDATION_CLEANUP_MINUTES", "5"))
        self.reconnect_interval = int(os.getenv("LIQUIDATION_RECONNECT_INTERVAL", 60 * 60 * 23 + 50 * 60))  # 23h50m default

        # Data aggregation for 1-minute precision
        self.aggregated_data = {symbol: {} for symbol in self.symbols}
        
        # WebSocket connection management
        self.ws = None
        self.running = False
        self.reconnect_thread = None
        
        # Register cleanup on exit
        atexit.register(self.stop)

        logger.info(f"Slave {self.slave_id} initialized DistributedLiquidationWebSocket with symbols: {self.symbols}", 
                   extra={'operation': 'init', 'slave_id': self.slave_id})
        logger.info(f"Slave {self.slave_id} WebSocket URL: {self.liquidation_ws_url}, Cleanup interval: {self.cleanup_minutes} minutes", 
                   extra={'operation': 'init', 'slave_id': self.slave_id})

    def get_uris(self):
        """Return WebSocket URI for liquidation stream - used by parent class for connection tracking"""
        return [(self.liquidation_ws_url, "liquidation")]

    def default_liquidation(self):
        """Default liquidation data structure"""
        return {"total_quantity": 0, "total_dollars": 0, "event_count": 0}

    def on_message(self, ws, message):
        """Process liquidation messages with 1-minute precision aggregation"""
        try:
            logger.debug(f"Raw WebSocket message: {message}", extra={'operation': 'on_message', 'slave_id': self.slave_id})
            data = json.loads(message)
            order = data["o"]

            symbol = order["s"]
            
            # Only process symbols assigned to this slave
            if symbol not in self.symbols:
                return

            # Extract liquidation details
            side = order["S"].lower()
            price = float(order["p"])
            qty = float(order["q"])
            total_dollars = round(price * qty, 2)

            # Create 1-minute precision timestamp
            trade_time_ms = order["T"]
            trade_time = datetime.utcfromtimestamp(trade_time_ms / 1000)
            minute_start_utc = trade_time.replace(second=0, microsecond=0)
            minute_ts = int(minute_start_utc.timestamp())

            # Initialize data structure for this minute if not exists
            if minute_ts not in self.aggregated_data[symbol]:
                self.aggregated_data[symbol][minute_ts] = {
                    "buy_liquidations": self.default_liquidation(),
                    "sell_liquidations": self.default_liquidation(),
                }

            # Aggregate liquidation data
            group = self.aggregated_data[symbol][minute_ts]["buy_liquidations" if side == "buy" else "sell_liquidations"]
            group["total_quantity"] += qty
            group["total_dollars"] += total_dollars
            group["event_count"] += 1

            logger.debug(f"Slave {self.slave_id} updated {side} liquidation for symbol {symbol} at {minute_start_utc}: qty={qty}, dollars={total_dollars}", 
                        extra={'symbol': symbol, 'operation': 'on_message', 'slave_id': self.slave_id})
                        
        except Exception as e:
            logger.error(f"Slave {self.slave_id} error processing liquidation message: {e}. Raw message: {message}",
                        extra={'operation': 'on_message', 'slave_id': self.slave_id})
    def save_data(self):
        """Save aggregated liquidation data to MongoDB with 1-minute precision"""
        try:
            current_time = datetime.utcnow()
            prev_minute = (current_time - timedelta(minutes=1)).replace(second=0, microsecond=0)
            prev_ts = int(prev_minute.timestamp())
            cleanup_threshold = int((current_time - timedelta(minutes=self.cleanup_minutes)).timestamp())

            for symbol in self.symbols:
                # Get data for the previous minute
                data = self.aggregated_data[symbol].pop(prev_ts, None)
                
                # Prepare update data with default values
                update_data = {
                    "liquidations": {
                        "buy_liquidations": self.default_liquidation(),
                        "sell_liquidations": self.default_liquidation(),
                    },
                    # Add distributed system metadata
                    "collector_info": {
                        "slave_id": self.slave_id,
                        "collection_method": "websocket_liquidation",
                        "data_precision": "1m",
                        "timestamp": current_time.isoformat()
                    }
                }

                # Use actual data if available
                if data:
                    update_data["liquidations"]["buy_liquidations"] = data.get(
                        "buy_liquidations", self.default_liquidation()
                    )
                    update_data["liquidations"]["sell_liquidations"] = data.get(
                        "sell_liquidations", self.default_liquidation()
                    )

                # Update MongoDB
                result = self.collections[symbol].update_one(
                    {"timestamp": prev_minute, "symbol": symbol},
                    {"$set": update_data},
                    upsert=True,
                )
                
                if data:  # Only log if there was actual data
                    logger.info(f"Slave {self.slave_id} saved liquidation data for {symbol} at {prev_minute}: matched={result.matched_count}, modified={result.modified_count}", 
                               extra={'symbol': symbol, 'operation': 'save_data', 'slave_id': self.slave_id})

                # Clean up expired data
                expired_keys = [ts for ts in self.aggregated_data[symbol] if ts < cleanup_threshold]
                for ts in expired_keys:
                    self.aggregated_data[symbol].pop(ts, None)
                    
                if expired_keys:
                    logger.info(f"Slave {self.slave_id} cleared {len(expired_keys)} expired entries for symbol {symbol}", 
                               extra={'symbol': symbol, 'operation': 'save_data', 'slave_id': self.slave_id})

            # Send heartbeat to master
            self.send_heartbeat_to_master()
            
        except Exception as e:
            logger.error(f"Slave {self.slave_id} error saving liquidation data: {e}", 
                        extra={'operation': 'save_data', 'slave_id': self.slave_id})

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"Slave {self.slave_id} WebSocket error: {error}", 
                    extra={'operation': 'on_error', 'slave_id': self.slave_id})
        self.running = False

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        logger.info(f"Slave {self.slave_id} WebSocket closed: status={close_status_code}, msg={close_msg}", 
                   extra={'operation': 'on_close', 'slave_id': self.slave_id})
        self.running = False

    def on_open(self, ws):
        """Handle WebSocket open"""
        logger.info(f"Slave {self.slave_id} liquidation WebSocket connection opened", 
                   extra={'operation': 'on_open', 'slave_id': self.slave_id})
        self.running = True
        self.start_scheduler()
        self.schedule_reconnect()

    def schedule_reconnect(self):
        """Schedule periodic reconnection to avoid 24h disconnection"""
        def delayed_reconnect():
            logger.info(f"Slave {self.slave_id} reconnect timer started, will reconnect in {self.reconnect_interval // 60} minutes", 
                       extra={'operation': 'schedule_reconnect', 'slave_id': self.slave_id})
            time.sleep(self.reconnect_interval)
            if self.running and self.ws:
                logger.info(f"Slave {self.slave_id} reconnect timer triggered, closing WebSocket to force reconnect", 
                           extra={'operation': 'schedule_reconnect', 'slave_id': self.slave_id})
                self.ws.close()

        self.reconnect_thread = threading.Thread(target=delayed_reconnect, daemon=True)
        self.reconnect_thread.start()

    def connect(self):
        """Start liquidation WebSocket connection with auto-reconnect"""
        self.running = True
        logger.info(f"Slave {self.slave_id} starting liquidation WebSocket service", 
                   extra={'operation': 'connect', 'slave_id': self.slave_id})
        
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.liquidation_ws_url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open,
                )
                self.ws.run_forever(ping_interval=300, ping_timeout=15)
                
                if self.running:
                    logger.info(f"Slave {self.slave_id} connection lost, retrying in 5 seconds...", 
                               extra={'operation': 'connect', 'slave_id': self.slave_id})
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Slave {self.slave_id} connection error: {e}, retrying in 5 seconds...", 
                            extra={'operation': 'connect', 'slave_id': self.slave_id})
                time.sleep(5)

    def start_scheduler(self):
        """Start scheduler to save data every minute"""
        self.scheduler.add_job(
            self.save_data,
            CronTrigger(second=0),  # Run at the start of every minute
            id=f"{self.slave_id}_{self.__class__.__name__}_save_data",
            name=f"Save liquidation data for slave {self.slave_id}",
            replace_existing=True,
        )
        logger.info(f"Slave {self.slave_id} scheduler started for liquidation data saving", 
                   extra={'operation': 'start_scheduler', 'slave_id': self.slave_id})

    def stop(self):
        """Stop liquidation WebSocket service"""
        try:
            logger.info(f"Slave {self.slave_id} stopping liquidation WebSocket service", 
                       extra={'operation': 'stop', 'slave_id': self.slave_id})
            
            self.running = False
            
            if self.ws:
                self.ws.close()
                
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
                
            self.client.close()
            
            logger.info(f"Slave {self.slave_id} DistributedLiquidationWebSocket stopped", 
                       extra={'operation': 'stop', 'slave_id': self.slave_id})
                       
        except Exception as e:
            logger.error(f"Slave {self.slave_id} error stopping DistributedLiquidationWebSocket: {e}", 
                        extra={'operation': 'stop', 'slave_id': self.slave_id})

def main():
    """Main entry point for distributed liquidation websocket service"""
    # Get configuration from environment
    slave_id = os.getenv("SLAVE_ID", "unknown-slave")

    logger.info(f"Starting Distributed Liquidation WebSocket Service for slave {slave_id}",
               extra={'operation': 'main', 'slave_id': slave_id})

    # Load symbols from file
    hostname = os.getenv("HOSTNAME")
    if not hostname:
        logger.error("HOSTNAME environment variable not set.")
        sys.exit(1)
    pod_index = hostname.split('-')[-1]
    symbols_file_path = f"/config/{pod_index}/symbols.csv"

    try:
        with open(symbols_file_path, 'r') as f:
            symbols = [symbol.strip() for symbol in f.read().split(',') if symbol.strip()]
        logger.info(f"Loaded {len(symbols)} symbols from {symbols_file_path}")
    except Exception as e:
        logger.error(f"Failed to read symbols from {symbols_file_path}: {e}")
        symbols = []
        sys.exit(1) # Exit if symbols cannot be loaded

    # Create and start the websocket service
    ws = DistributedLiquidationWebSocket(symbols=symbols, slave_id=slave_id)

    try:
        ws.connect()
    except KeyboardInterrupt:
        logger.info(f"Slave {slave_id} LiquidationWebSocket stopped by user",
                   extra={'operation': 'main', 'slave_id': slave_id})
        ws.stop()
if __name__ == "__main__":
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level))
    main()