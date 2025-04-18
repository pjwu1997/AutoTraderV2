import asyncio
import websockets
import json
from datetime import datetime
from websocket_controller import WebSocketController
import config


class KlineWebSocket(WebSocketController):
    def __init__(self, symbols: list = None, interval: str = None):
        super().__init__(symbols)
        self.interval = interval or config.KLINE_INTERVAL
        self.spot_uri = config.KLINE_SPOT_WS_URL.format(
            streams='/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)
        )
        self.futures_uri = config.KLINE_FUTURES_WS_URL.format(
            streams='/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)
        )

    def calculate_metrics(self, kline):
        try:
            high = float(kline["h"])
            taker_buy_quote = float(kline["Q"])
            quote_asset = float(kline["q"])
            close_price = float(kline["c"])
            volume = float(kline["v"])
            if high == 0:
                cvd = 0
                volume = 0
            else:
                cvd = (taker_buy_quote - quote_asset) / close_price
        except (ValueError, KeyError):
            cvd = 0
            volume = 0
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
            print(f"[{symbol}] updated {market_type} at {timestamp}")
        except Exception as e:
            print(f"訊息處理錯誤: {e}")

    async def connect_stream(self, uri, market_type):
        async with websockets.connect(uri) as ws:
            async for message in ws:
                await asyncio.get_event_loop().run_in_executor(
                    None, self.on_message, message, market_type
                )

    async def connect(self):
        await asyncio.gather(
            self.connect_stream(self.spot_uri, "spot"),
            self.connect_stream(self.futures_uri, "futures"),
        )

    def save_data(self):
        pass  # K 線資料即時儲存，無需額外排程

    def start_scheduler(self, interval_seconds=20):
        pass  # K 線資料即時儲存，無需排程


if __name__ == "__main__":
    ws = KlineWebSocket()
    asyncio.run(ws.connect())