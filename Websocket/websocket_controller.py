from abc import ABC, abstractmethod
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import config


class WebSocketController(ABC):
    def __init__(self, symbols: list = None):
        self.client = MongoClient(config.MONGO_URI)
        self.db = self.client[config.MONGO_DB_NAME]
        self.symbols = symbols or config.SYMBOLS
        self.collections = {symbol: self.db[symbol] for symbol in self.symbols}
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    @abstractmethod
    def connect(self):
        """建立 WebSocket 連線"""
        pass

    @abstractmethod
    def on_message(self, message):
        """處理接收到的 WebSocket 訊息"""
        pass

    @abstractmethod
    def save_data(self):
        """儲存資料到 MongoDB"""
        pass

    def start_scheduler(self, interval_seconds=20):
        """啟動排程，定期儲存資料"""
        self.scheduler.add_job(
            self.save_data,
            "interval",
            seconds=interval_seconds,
            id=f"{self.__class__.__name__}_save_data",
            name=f"Save data for {self.__class__.__name__}",
            replace_existing=True,
        )

    def stop(self):
        """停止 WebSocket 和排程"""
        try:
            if self.scheduler.get_job(f"{self.__class__.__name__}_save_data"):
                self.scheduler.remove_job(f"{self.__class__.__name__}_save_data")
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            self.client.close()
        except Exception as e:
            print(f"停止錯誤: {e}")