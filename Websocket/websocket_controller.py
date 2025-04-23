from abc import ABC, abstractmethod
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
import config
import asyncio
import websockets


class WebSocketController(ABC):
    def __init__(self, symbols: list = None):
        self.client = MongoClient(config.MONGO_URI)
        self.db = self.client[config.MONGO_DB_NAME]
        self.symbols = symbols or config.SYMBOLS
        self.collections = {symbol: self.db[symbol] for symbol in self.symbols}
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.reconnect_interval = 60 * 60 * 23 + 50 * 60  # 23h50m

   

    @abstractmethod
    def on_message(self, message, market_type):
        """處理接收到的 WebSocket 訊息"""
        pass

    @abstractmethod
    def save_data(self):
        """儲存資料到 MongoDB"""
        pass

    async def connect_stream(self, uri, market_type):
        while True:
            try:
                print(f"Connecting to {uri} for {market_type}...")
                async with websockets.connect(uri, ping_interval=300, ping_timeout=15) as ws:
                    print(f"Connected to {market_type} WebSocket")

                    reconnect_task = asyncio.create_task(self.schedule_reconnect(ws))
                    async for message in ws:
                        await asyncio.get_event_loop().run_in_executor(
                            None, self.on_message, message, market_type
                        )
            except websockets.exceptions.ConnectionClosed as e:
                print(f"{market_type} connection closed: {e}")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"連線錯誤 ({market_type}): {e}")
                await asyncio.sleep(5)

    async def schedule_reconnect(self, ws):
        await asyncio.sleep(self.reconnect_interval)
        print("Scheduled reconnect triggered.")
        await ws.close()

    async def connect(self):
        tasks = []
        for uri, market_type in self.get_uris():
            tasks.append(self.connect_stream(uri, market_type))
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

    def stop(self):
        try:
            if self.scheduler.get_job(f"{self.__class__.__name__}_save_data"):
                self.scheduler.remove_job(f"{self.__class__.__name__}_save_data")
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            self.client.close()
        except Exception as e:
            print(f"停止錯誤: {e}")
