import asyncio
import json
import os
from datetime import datetime
from websocket_controller import DistributedWebSocketController
import logging
from logging.handlers import RotatingFileHandler

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler('kline_websocket.log', maxBytes=10*1024*1024, backupCount=5),  # Rotate logs at 10MB
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger('kline_websocket')

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
            'market_type': getattr(record, 'market_type', None),  # Add market type if available
            'slave_id': getattr(record, 'slave_id', None),  # Add slave ID if available
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

class DistributedKlineWebSocket(DistributedWebSocketController):
    def __init__(self, symbols: list = None, interval: str = None, slave_id: str = None):
        super().__init__(symbols, slave_id)
        
        # 1-minute precision configuration
        self.interval = interval or os.getenv("KLINE_INTERVAL", "1m")  # Default to 1m for precision
        
        # WebSocket URLs for Binance streams
        spot_ws_url = os.getenv("KLINE_SPOT_WS_URL", "wss://stream.binance.com:9443/ws/{streams}")
        futures_ws_url = os.getenv("KLINE_FUTURES_WS_URL", "wss://fstream.binance.com/ws/{streams}")
        
        # Build WebSocket URIs with 1-minute kline streams
        self.spot_uri = spot_ws_url.format(
            streams='/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)
        )
        self.futures_uri = futures_ws_url.format(
            streams='/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)
        )

        logger.info(f"Slave {self.slave_id} initialized DistributedKlineWebSocket with symbols: {self.symbols}, interval: {self.interval}", 
                    extra={'operation': 'init', 'slave_id': self.slave_id})
        logger.info(f"Slave {self.slave_id} Spot URI: {self.spot_uri}, Futures URI: {self.futures_uri}", 
                    extra={'operation': 'init', 'slave_id': self.slave_id})

    def get_uris(self):
        """Return WebSocket URIs for both spot and futures markets"""
        return [
            (self.spot_uri, "spot"),
            (self.futures_uri, "futures")
        ]

    def calculate_metrics(self, kline):
        """Calculate CVD and volume metrics from kline data"""
        try:
            high = float(kline["h"])
            taker_buy_quote = float(kline["Q"])
            quote_asset = float(kline["q"])
            close_price = float(kline["c"])
            volume = float(kline["v"])
            
            if high == 0:
                return 0, 0
                
            # Calculate CVD (Cumulative Volume Delta)
            cvd = (taker_buy_quote - (quote_asset - taker_buy_quote)) / close_price
            
            return cvd, volume
            
        except (ValueError, KeyError, ZeroDivisionError) as e:
            logger.error(f"Slave {self.slave_id} error calculating metrics: {e}", 
                        extra={'operation': 'calculate_metrics', 'slave_id': self.slave_id})
            return 0, 0

    def on_message(self, message, market_type):
        """Process incoming WebSocket kline messages with 1-minute precision"""
        try:
            data = json.loads(message)
            logger.debug(f"Raw WebSocket message: {message}", extra={'operation': 'on_message', 'market_type': market_type, 'slave_id': self.slave_id})
            
            # Handle both single stream and combined stream formats
            if "stream" in data:  # Combined stream format
                stream_data = data["data"]
                k = stream_data["k"]
            else:  # Single stream format
                k = data["k"]
            
            symbol = k["s"]
            
            # Only process symbols assigned to this slave
            if symbol not in self.symbols:
                return

            # Create 1-minute precision timestamp
            open_time = datetime.utcfromtimestamp(k["t"] / 1000)
            timestamp = open_time.replace(second=0, microsecond=0)  # Round to minute
            
            # Calculate metrics
            cvd, vol = self.calculate_metrics(k)

            # Prepare update data for MongoDB
            update_data = {
                f"{market_type}": {
                    "open": k["o"],
                    "high": k["h"],
                    "low": k["l"],
                    "close": k["c"],
                    "volume": k["v"],
                    "quote_volume": k["q"],
                    "trade_num": k["n"],
                    "taker_buy_base": k["V"],
                    "taker_buy_quote": k["Q"],
                    "cvd": cvd,
                    "calculated_volume": vol,
                },
                "timestamp": timestamp,
                "symbol": symbol,
                # Add distributed system metadata
                "collector_info": {
                    "slave_id": self.slave_id,
                    "collection_method": "websocket_realtime",
                    "data_precision": "1m",
                    "market_type": market_type
                }
            }

            # Upsert to MongoDB (update if exists, insert if not)
            self.collections[symbol].update_one(
                {"timestamp": timestamp, "symbol": symbol},
                {"$set": update_data},
                upsert=True,
            )

            logger.debug(f"Slave {self.slave_id} processed {market_type} kline for {symbol} at {timestamp}", 
                        extra={'operation': 'on_message', 'symbol': symbol, 'market_type': market_type, 'slave_id': self.slave_id})

        except Exception as e:
            logger.error(f"Slave {self.slave_id} error processing message: {e}. Raw message: {message}",
                        extra={'operation': 'on_message', 'market_type': market_type, 'slave_id': self.slave_id})
    def save_data(self):
        """Periodic data save - mainly for logging and health checks"""
        try:
            # Send heartbeat to master
            self.send_heartbeat_to_master()
            
            # Log current status
            logger.info(f"Slave {self.slave_id} kline websocket active - processing {len(self.symbols)} symbols with {self.interval} precision", 
                       extra={'operation': 'save_data', 'slave_id': self.slave_id})
                       
        except Exception as e:
            logger.error(f"Slave {self.slave_id} error in save_data: {e}", 
                        extra={'operation': 'save_data', 'slave_id': self.slave_id})

    def start_scheduler(self, interval_seconds=60):
        """Start scheduler with 1-minute intervals for health checks"""
        super().start_scheduler(interval_seconds)

async def main():
    """Main entry point for distributed kline websocket service"""
    # Get configuration from environment
    slave_id = os.getenv("SLAVE_ID", "unknown-slave")
    interval = os.getenv("KLINE_INTERVAL", "1m")

    logger.info(f"Starting Distributed Kline WebSocket Service for slave {slave_id}",
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
    ws = DistributedKlineWebSocket(symbols=symbols, interval=interval, slave_id=slave_id)

    try:
        await ws.connect()
    except KeyboardInterrupt:
        logger.info(f"Slave {slave_id} KlineWebSocket stopped by user",
                   extra={'operation': 'main', 'slave_id': slave_id})
        ws.stop()
if __name__ == "__main__":
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=getattr(logging, log_level))
    asyncio.run(main())