import os
import websocket
import json
import threading
import atexit
import time
from datetime import datetime, timedelta
from websocket_controller import WebSocketController
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
            'operation': getattr(record, 'operation', None)  # Add operation type if available
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

class LiquidationWebSocket(WebSocketController):
    def __init__(self, symbols: list = None):
        # Read environment variables
        liquidation_ws_url = os.getenv("LIQUIDATION_WS_URL", "wss://fstream.binance.com/ws/!forceOrder@arr")
        cleanup_minutes = int(os.getenv("LIQUIDATION_CLEANUP_MINUTES", "5"))
        self.reconnect_interval = int(os.getenv("LIQUIDATION_RECONNECT_INTERVAL", 60 * 60 * 23 + 50 * 60))  # 23h50m default

        symbols_env = os.getenv("SYMBOLS")
        if symbols_env:
            self.symbols = [symbol.strip() for symbol in symbols_env.split(",")]
        else:
            self.symbols = symbols if symbols else []

        super().__init__(self.symbols)

        self.socket = liquidation_ws_url
        self.cleanup_minutes = cleanup_minutes
        self.aggregated_data = {symbol: {} for symbol in self.symbols}
        self.ws = None
        self.running = False
        self.reconnect_thread = None
        atexit.register(self.stop)

        logger.info(f"Initialized LiquidationWebSocket with symbols: {self.symbols}", extra={'operation': 'init'})
        logger.info(f"WebSocket URL: {self.socket}, Cleanup interval: {self.cleanup_minutes} minutes", extra={'operation': 'init'})

    def default_liquidation(self):
        return {"total_quantity": 0, "total_dollars": 0, "event_count": 0}

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            order = data["o"]

            symbol = order["s"]
            if symbol not in self.symbols:
                return

            side = order["S"].lower()
            price = float(order["p"])
            qty = float(order["q"])
            total_dollars = round(price * qty, 2)

            trade_time_ms = order["T"]
            trade_time = datetime.utcfromtimestamp(trade_time_ms / 1000)
            minute_start_utc = trade_time.replace(second=0, microsecond=0)
            minute_ts = int(minute_start_utc.timestamp())

            if minute_ts not in self.aggregated_data[symbol]:
                self.aggregated_data[symbol][minute_ts] = {
                    "buy_liquidations": self.default_liquidation(),
                    "sell_liquidations": self.default_liquidation(),
                }

            group = self.aggregated_data[symbol][minute_ts]["buy_liquidations" if side == "buy" else "sell_liquidations"]
            group["total_quantity"] += qty
            group["total_dollars"] += total_dollars
            group["event_count"] += 1

            logger.info(f"Updated {side} liquidation for symbol {symbol} at {minute_start_utc}: {group}", 
                       extra={'symbol': symbol, 'operation': 'on_message'})
        except Exception as e:
            logger.error(f"Error processing message: {e}", extra={'operation': 'on_message'})

    def save_data(self):
        try:
            current_time = datetime.utcnow()
            prev_minute = (current_time - timedelta(minutes=1)).replace(second=0, microsecond=0)
            prev_ts = int(prev_minute.timestamp())
            cleanup_threshold = int((current_time - timedelta(minutes=self.cleanup_minutes)).timestamp())

            for symbol in self.symbols:
                data = self.aggregated_data[symbol].pop(prev_ts, None)
                update_data = {
                    "liquidations": {
                        "buy_liquidations": self.default_liquidation(),
                        "sell_liquidations": self.default_liquidation(),
                    }
                }

                if data:
                    update_data["liquidations"]["buy_liquidations"] = data.get(
                        "buy_liquidations", self.default_liquidation()
                    )
                    update_data["liquidations"]["sell_liquidations"] = data.get(
                        "sell_liquidations", self.default_liquidation()
                    )

                result = self.collections[symbol].update_one(
                    {"timestamp": prev_minute, "symbol": symbol},
                    {"$set": update_data},
                    upsert=True,
                )
                logger.info(f"Updated data for symbol {symbol} at {prev_minute}: matched={result.matched_count}, modified={result.modified_count}", 
                           extra={'symbol': symbol, 'operation': 'save_data'})

                expired_keys = [ts for ts in self.aggregated_data[symbol] if ts < cleanup_threshold]
                for ts in expired_keys:
                    self.aggregated_data[symbol].pop(ts, None)
                if expired_keys:
                    logger.info(f"Cleared {len(expired_keys)} expired entries for symbol {symbol}", 
                               extra={'symbol': symbol, 'operation': 'save_data'})
        except Exception as e:
            logger.error(f"Error saving data: {e}", extra={'operation': 'save_data'})

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}", extra={'operation': 'on_error'})
        self.running = False

    def on_close(self, ws, close_status_code, close_msg):
        logger.info(f"WebSocket closed: status={close_status_code}, msg={close_msg}", extra={'operation': 'on_close'})
        self.running = False

    def on_open(self, ws):
        logger.info("WebSocket connection opened", extra={'operation': 'on_open'})
        self.running = True
        self.start_scheduler()
        self.schedule_reconnect()

    def schedule_reconnect(self):
        def delayed_reconnect():
            logger.info(f"Reconnect timer started, will reconnect in {self.reconnect_interval // 60} minutes", 
                       extra={'operation': 'schedule_reconnect'})
            time.sleep(self.reconnect_interval)
            if self.running and self.ws:
                logger.info("Reconnect timer triggered, closing WebSocket to force reconnect", 
                           extra={'operation': 'schedule_reconnect'})
                self.ws.close()

        self.reconnect_thread = threading.Thread(target=delayed_reconnect, daemon=True)
        self.reconnect_thread.start()

    def connect(self):
        self.running = True
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.socket,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open,
                )
                self.ws.run_forever(ping_interval=300, ping_timeout=15)
                if self.running:
                    logger.info("Connection lost, retrying in 5 seconds...", extra={'operation': 'connect'})
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Connection error: {e}, retrying in 5 seconds...", extra={'operation': 'connect'})
                time.sleep(5)

    def start_scheduler(self):
        self.scheduler.add_job(
            self.save_data,
            CronTrigger(second=0),
            id=f"{self.__class__.__name__}_save_data",
            name=f"Save data for {self.__class__.__name__}",
            replace_existing=True,
        )
        logger.info("Scheduler started for saving data", extra={'operation': 'start_scheduler'})

    def stop(self):
        try:
            self.running = False
            if self.ws:
                self.ws.close()
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            self.client.close()
            logger.info("LiquidationWebSocket stopped", extra={'operation': 'stop'})
        except Exception as e:
            logger.error(f"Error stopping LiquidationWebSocket: {e}", extra={'operation': 'stop'})


if __name__ == "__main__":
    ws = LiquidationWebSocket()
    try:
        ws.connect()
    except KeyboardInterrupt:
        ws.stop()