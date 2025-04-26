import asyncio
import json
import os
from datetime import datetime
from websocket_controller import WebSocketController


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
        except (ValueError, KeyError):
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
            print(f"訊息處理錯誤: {e}")

    def save_data(self):
        pass

    def start_scheduler(self, interval_seconds=20):
        pass


if __name__ == "__main__":
    ws = KlineWebSocket()
    asyncio.run(ws.connect())
