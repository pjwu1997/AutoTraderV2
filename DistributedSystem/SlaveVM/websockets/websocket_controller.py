from abc import ABC, abstractmethod
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio
import websockets
import os
import logging
import json
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
            'slave_id': getattr(record, 'slave_id', None),  # Add slave ID if available
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

class DistributedWebSocketController(ABC):
    def __init__(self, symbols: list = None, slave_id: str = None):
        # Distributed system environment variables
        self.slave_id = slave_id or os.getenv("SLAVE_ID", "unknown-slave")
        self.master_url = os.getenv("MASTER_URL", "http://master-vm:8080")
        
        # MongoDB configuration
        mongo_uri = os.getenv("MONGO_URI")
        mongo_db_name = os.getenv("MONGO_DB_NAME")
        symbols_env = os.getenv("SYMBOLS")  # Reads a comma-separated list of symbols, e.g., "BTCUSDT,ETHUSDT"

        if not mongo_uri or not mongo_db_name:
            logger.error("MONGO_URI and MONGO_DB_NAME must be set in environment variables", 
                        extra={'operation': 'init', 'slave_id': self.slave_id})
            raise ValueError("MONGO_URI and MONGO_DB_NAME must be set in environment variables")

        self.client = MongoClient(mongo_uri)
        self.db = self.client[mongo_db_name]

        # Symbol configuration for distributed system
        symbols_env = os.getenv("SYMBOLS")
        if symbols_env:
            self.symbols = [s.strip() for s in symbols_env.split(',') if s.strip()]
            logger.info(f"Loaded {len(self.symbols)} symbols from SYMBOLS env var")
        else:
            symbols_file_path = os.getenv("SYMBOLS_FILE_PATH")
            if not symbols_file_path:
                hostname = os.getenv("HOSTNAME")
                if hostname:
                    pod_index = hostname.split('-')[-1]
                    symbols_file_path = f"/config/{pod_index}/symbols.csv"
                else:
                    self.symbols = []
            
            if symbols_file_path:
                try:
                    with open(symbols_file_path, 'r') as f:
                        self.symbols = [s.strip() for s in f.read().split(',') if s.strip()]
                    logger.info(f"Loaded {len(self.symbols)} symbols from {symbols_file_path}")
                except Exception as e:
                    logger.error(f"Failed to read symbols from {symbols_file_path}: {e}")
                    self.symbols = []
            else:
                self.symbols = []

        if not self.symbols:
            raise ValueError("Must provide symbols list, set SYMBOLS environment variable, or set SYMBOLS_FILE_PATH environment variable")

        # Create collections for each symbol
        self.collections = {symbol: self.db[symbol] for symbol in self.symbols}
        
        # Scheduler for periodic tasks
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        
        # WebSocket reconnection interval (23h50m to avoid 24h disconnection)
        self.reconnect_interval = 60 * 60 * 23 + 50 * 60  # 23h50m

        logger.info(f"Initialized DistributedWebSocketController for slave {self.slave_id} with symbols: {self.symbols}", 
                   extra={'operation': 'init', 'slave_id': self.slave_id})
        logger.info(f"Connected to MongoDB: {mongo_uri}, Database: {mongo_db_name}", 
                   extra={'operation': 'init', 'slave_id': self.slave_id})

    @abstractmethod
    def on_message(self, message, market_type):
        """Handle received WebSocket messages"""
        pass

    @abstractmethod
    def save_data(self):
        """Save data to MongoDB"""
        pass

    @abstractmethod
    def get_uris(self):
        """Return list of (uri, market_type) tuples for WebSocket connections"""
        pass

    async def connect_stream(self, uri, market_type):
        while True:
            try:
                logger.info(f"Slave {self.slave_id} connecting to {uri} for {market_type}", 
                           extra={'operation': 'connect_stream', 'market_type': market_type, 'slave_id': self.slave_id})
                async with websockets.connect(uri, ping_interval=300, ping_timeout=15) as ws:
                    logger.info(f"Slave {self.slave_id} connected to {market_type} WebSocket", 
                               extra={'operation': 'connect_stream', 'market_type': market_type, 'slave_id': self.slave_id})

                    # Schedule automatic reconnection
                    reconnect_task = asyncio.create_task(self.schedule_reconnect(ws))
                    
                    # Process messages
                    async for message in ws:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.on_message, message, market_type
                        )
                        
            except websockets.exceptions.ConnectionClosed as e:
                logger.info(f"Slave {self.slave_id} connection closed for {market_type}: {e}", 
                           extra={'operation': 'connect_stream', 'market_type': market_type, 'slave_id': self.slave_id})
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Slave {self.slave_id} connection error for {market_type}: {e}", 
                            extra={'operation': 'connect_stream', 'market_type': market_type, 'slave_id': self.slave_id})
                await asyncio.sleep(5)

    async def schedule_reconnect(self, ws):
        """Schedule automatic reconnection every 23h50m"""
        await asyncio.sleep(self.reconnect_interval)
        logger.info(f"Slave {self.slave_id} scheduled reconnect triggered", 
                   extra={'operation': 'schedule_reconnect', 'slave_id': self.slave_id})
        await ws.close()

    async def connect(self):
        """Start all WebSocket connections"""
        tasks = []
        for uri, market_type in self.get_uris():
            tasks.append(self.connect_stream(uri, market_type))
        logger.info(f"Slave {self.slave_id} starting WebSocket connections", 
                   extra={'operation': 'connect', 'slave_id': self.slave_id})
        await asyncio.gather(*tasks)

    def start_scheduler(self, interval_seconds=20):
        """Start periodic data saving scheduler"""
        self.scheduler.add_job(
            self.save_data,
            "interval",
            seconds=interval_seconds,
            id=f"{self.slave_id}_{self.__class__.__name__}_save_data",
            name=f"Save data for {self.__class__.__name__} - Slave {self.slave_id}",
            replace_existing=True,
        )
        logger.info(f"Slave {self.slave_id} scheduler started for {self.__class__.__name__} with interval {interval_seconds} seconds", 
                   extra={'operation': 'start_scheduler', 'slave_id': self.slave_id})

    def send_heartbeat_to_master(self):
        """Send heartbeat to master with WebSocket status"""
        try:
            import requests
            import psutil
            
            health_data = {
                "slave_id": self.slave_id,
                "status": "online",
                "timestamp": datetime.utcnow().isoformat(),
                "cpu_usage": psutil.cpu_percent(interval=0.1),
                "memory_usage": psutil.virtual_memory().percent,
                "websocket_service": self.__class__.__name__,
                "symbols_count": len(self.symbols),
                "active_connections": len(self.get_uris())
            }
            
            response = requests.post(
                f"{self.master_url}/api/heartbeat/{self.slave_id}",
                json=health_data,
                timeout=5
            )
            
            if response.status_code == 200:
                return True
            else:
                logger.warning(f"Slave {self.slave_id} heartbeat failed: HTTP {response.status_code}", 
                              extra={'operation': 'heartbeat', 'slave_id': self.slave_id})
                return False
                
        except Exception as e:
            logger.error(f"Slave {self.slave_id} error sending heartbeat: {e}", 
                        extra={'operation': 'heartbeat', 'slave_id': self.slave_id})
            return False

    def stop(self):
        """Stop all services and close connections"""
        try:
            if self.scheduler.get_job(f"{self.slave_id}_{self.__class__.__name__}_save_data"):
                self.scheduler.remove_job(f"{self.slave_id}_{self.__class__.__name__}_save_data")
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            self.client.close()
            logger.info(f"Slave {self.slave_id} DistributedWebSocketController stopped", 
                       extra={'operation': 'stop', 'slave_id': self.slave_id})
        except Exception as e:
            logger.error(f"Slave {self.slave_id} error stopping DistributedWebSocketController: {e}", 
                        extra={'operation': 'stop', 'slave_id': self.slave_id})