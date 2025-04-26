from abc import ABC, abstractmethod
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio
import websockets
import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler('websocket_controller.log', maxBytes=10*1024*1024, backupCount=5),  # Rotate logs at 10MB
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger('websocket_controller')

# Custom JSON formatter for structured logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.msg,
            'logger': record.name,
            'operation': getattr(record, 'operation', None),  # Add operation type if available
            'market_type': getattr(record, 'market_type', None),  # Add market type if available
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

class WebSocketController(ABC):
    def __init__(self, symbols: list = None):
        mongo_uri = os.getenv("MONGO_URI")
        mongo_db_name = os.getenv("MONGO_DB_NAME")
        symbols_env = os.getenv("SYMBOLS")  # Reads a comma-separated list of symbols, e.g., "BTCUSDT,ETHUSDT"

        if not mongo_uri or not mongo_db_name:
            logger.error("MONGO_URI and MONGO_DB_NAME must be set in environment variables", extra={'operation': 'init'})
            raise ValueError("MONGO_URI and MONGO_DB_NAME must be set in environment variables")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[mongo_db_name]

        if symbols:
            self.symbols = symbols
        elif symbols_env:
            self.symbols = [s.strip() for s in symbols_env.split(",")]
        else:
            logger.error("Must provide symbols list or set SYMBOLS environment variable", extra={'operation': 'init'})
            raise ValueError("Must provide symbols list or set SYMBOLS environment variable")

        self.collections = {symbol: self.db[symbol] for symbol in self.symbols}
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.reconnect_interval = 60 * 60 * 23 + 50 * 60  # 23h50m

        logger.info(f"Initialized WebSocketController with symbols: {self.symbols}", extra={'operation': 'init'})
        logger.info(f"Connected to MongoDB: {mongo_uri}, Database: {mongo_db_name}", extra={'operation': 'init'})

    @abstractmethod
    def on_message(self, message, market_type):
        """Handle received WebSocket messages"""
        pass

    @abstractmethod
    def save_data(self):
        """Save data to MongoDB"""
        pass

    async def connect_stream(self, uri, market_type):
        while True:
            try:
                logger.info(f"Connecting to {uri} for {market_type}", extra={'operation': 'connect_stream', 'market_type': market_type})
                async with websockets.connect(uri, ping_interval=300, ping_timeout=15) as ws:
                    logger.info(f"Connected to {market_type} WebSocket", extra={'operation': 'connect_stream', 'market_type': market_type})

                    reconnect_task = asyncio.create_task(self.schedule_reconnect(ws))
                    async for message in ws:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.on_message, message, market_type
                        )
            except websockets.exceptions.ConnectionClosed as e:
                logger.info(f"Connection closed for {market_type}: {e}", extra={'operation': 'connect_stream', 'market_type': market_type})
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Connection error for {market_type}: {e}", extra={'operation': 'connect_stream', 'market_type': market_type})
                await asyncio.sleep(5)

    async def schedule_reconnect(self, ws):
        await asyncio.sleep(self.reconnect_interval)
        logger.info("Scheduled reconnect triggered", extra={'operation': 'schedule_reconnect'})
        await ws.close()

    async def connect(self):
        tasks = []
        for uri, market_type in self.get_uris():
            tasks.append(self.connect_stream(uri, market_type))
        logger.info("Starting WebSocket connections", extra={'operation': 'connect'})
        await asyncio.gather(*tasks)

    def start_scheduler(self, interval_seconds=20):
        self.scheduler.add_job(
            self.save_data,
            "interval",
            seconds=interval_seconds,
            id=f"{self.__class__.__name__}_save_data",
            name=f"Save data for {self.__class__.__name__}",
            replace_existing=True,
        )
        logger.info(f"Scheduler started for {self.__class__.__name__} with interval {interval_seconds} seconds", extra={'operation': 'start_scheduler'})

    def stop(self):
        try:
            if self.scheduler.get_job(f"{self.__class__.__name__}_save_data"):
                self.scheduler.remove_job(f"{self.__class__.__name__}_save_data")
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            self.client.close()
            logger.info("WebSocketController stopped", extra={'operation': 'stop'})
        except Exception as e:
            logger.error(f"Error stopping WebSocketController: {e}", extra={'operation': 'stop'})