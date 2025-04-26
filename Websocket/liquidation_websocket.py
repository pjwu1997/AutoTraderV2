import os
import websocket
import json
import threading
import atexit
import time
from datetime import datetime, timedelta
from websocket_controller import WebSocketController
from apscheduler.triggers.cron import CronTrigger


class LiquidationWebSocket(WebSocketController):
    def __init__(self, symbols: list = None):
        # 讀取環境變數
        liquidation_ws_url = os.getenv("LIQUIDATION_WS_URL", "wss://fstream.binance.com/ws/!forceOrder@arr")
        cleanup_minutes = int(os.getenv("LIQUIDATION_CLEANUP_MINUTES", "5"))
        self.reconnect_interval = int(os.getenv("LIQUIDATION_RECONNECT_INTERVAL", 60 * 60 * 23 + 50 * 60))  # 23h50m 預設

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

            print(f"{symbol} {side} 更新於 {minute_start_utc}：{group}")
        except Exception as e:
            print(f"訊息處理錯誤: {e}")

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
                print(f"[{symbol}] 更新 {prev_minute}：matched={result.matched_count}, modified={result.modified_count}")

                expired_keys = [ts for ts in self.aggregated_data[symbol] if ts < cleanup_threshold]
                for ts in expired_keys:
                    self.aggregated_data[symbol].pop(ts, None)
                if expired_keys:
                    print(f"[{symbol}] 清理 {len(expired_keys)} 筆過期資料")
        except Exception as e:
            print(f"儲存資料錯誤: {e}")

    def on_error(self, ws, error):
        print(f"錯誤: {error}")
        self.running = False

    def on_close(self, ws, close_status_code, close_msg):
        print(f"連線關閉: status={close_status_code}, msg={close_msg}")
        self.running = False

    def on_open(self, ws):
        print("WebSocket 開啟")
        self.running = True
        self.start_scheduler()
        self.schedule_reconnect()

    def schedule_reconnect(self):
        def delayed_reconnect():
            print(f"[Reconnect Timer] 啟動，將於 {self.reconnect_interval // 60} 分鐘後強制重連")
            time.sleep(self.reconnect_interval)
            if self.running and self.ws:
                print("[Reconnect Timer] 時間到，關閉 WebSocket 以觸發自動重連")
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
                    print("連線斷開，5秒後重試...")
                    time.sleep(5)
            except Exception as e:
                print(f"連線錯誤: {e}，5秒後重試...")
                time.sleep(5)

    def start_scheduler(self):
        self.scheduler.add_job(
            self.save_data,
            CronTrigger(second=0),
            id=f"{self.__class__.__name__}_save_data",
            name=f"Save data for {self.__class__.__name__}",
            replace_existing=True,
        )

    def stop(self):
        try:
            self.running = False
            if self.ws:
                self.ws.close()
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)
            self.client.close()
            print("LiquidationWebSocket 已停止")
        except Exception as e:
            print(f"停止錯誤: {e}")


if __name__ == "__main__":
    ws = LiquidationWebSocket()
    try:
        ws.connect()
    except KeyboardInterrupt:
        ws.stop()
