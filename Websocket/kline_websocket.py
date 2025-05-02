import asyncio
import json
import os
from datetime import datetime
from websocket_controller import WebSocketController
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
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

class KlineWebSocket(WebSocketController):
    def __init__(self, symbols: list = None, interval: str = None):
        super().__init__(symbols)
        self.interval = interval or os.getenv("KLINE_INTERVAL", "1m")
        spot_ws_url = os.getenv("KLINE_SPOT_WS_URL")
        futures_ws_url = os.getenv("KLINE_FUTURES_WS_URL")
        self.spot_uri = spot_ws_url.format(
            streams='/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)
        )
        self.futures_uri = futures_ws_url.format(
            streams='/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)
        )

        logger.info(f"Initialized KlineWebSocket with symbols: {self.symbols}, interval: {self.interval}", 
                    extra={'operation': 'init'})
        logger.info(f"Spot URI: {self.spot_uri}, Futures URI: {self.futures_uri}", 
                    extra={'operation': 'init'})

    def get_uris(self):
        return [
            (self.spot_uri, "spot"),
            (self.futures_uri, "futures")
        ]

    def calculate_metrics(self, kline):
        try:
            high = float(kline["h"])
            taker_buy_quote = float(kline["Q"])
            quote_asset = float(kline["q"])
            close_price = float(kline["c"])
            volume = float(kline["v"])
            if high == 0:
                return 0, 0
            cvd = (taker_buy_quote - quote_asset) / close_price
        except (ValueError, KeyError) as e:
            logger.error(f"Error calculating metrics: {e}", extra={'operation': 'calculate_metrics'})
            return 0, 0
        return cvd, volume

    def on_message(self, message, market_type):
        try:
            data = json.loads(message)
            k = data["data"]["k"]
            symbol = k["s"]
            if symbol not in self.symbols:
                return

            open_time = datetime.utcfromtimestamp(k["t"] / 1000)
            timestamp = open_time.replace(second=0, microsecond=0)
            cvd, vol = self.calculate_metrics(k)

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
            }

            self.collections[symbol].update_one(
                {"timestamp": timestamp, "symbol": symbol},
                {"$set": update_data},
                upsert=True,
            )

        except Exception as e:
            logger.error(f"Error processing message: {e}", 
                        extra={'operation': 'on_message', 'market_type': market_type})

    def save_data(self):
        pass

    def start_scheduler(self, interval_seconds=20):
        pass


if __name__ == "__main__":
    ws = KlineWebSocket()
    try:
        asyncio.run(ws.connect())
    except KeyboardInterrupt:
        logger.info("KlineWebSocket stopped by user", extra={'operation': 'main'})