import websocket
import json
import pytz
from pymongo import MongoClient
import schedule
import time
import threading
from datetime import datetime, timedelta

# WebSocket 設定
socket = 'wss://fstream.binance.com/ws/!forceOrder@arr'

# 過濾條件
minimum_total_dollars = 0  # 最低總美元價值
set_ticker = ['BTCUSDT']  # 只處理 BTCUSDT

# MongoDB 連接設定
client = MongoClient('mongodb://localhost:27017/')
db = client['trading_data']
collection = db['BTCUSDT']  # 統一存入 BTCUSDT collection

# 聚合數據字典：{分鐘時間戳: {'buy_liquidations': {...}, 'sell_liquidations': {...}}}
aggregated_data = {}

# 預設的 liquidation 資料（全為 0）
default_liquidation_data = {
    'total_quantity': 0,
    'total_dollars': 0,
    'event_count': 0
}

# 儲存聚合數據到 MongoDB 的函數
def save_aggregated_data():
    current_time = datetime.utcnow()
    # 計算前一分鐘的開始時間
    previous_minute = (current_time - timedelta(minutes=1)).replace(second=0, microsecond=0)
    previous_minute_ts = int(previous_minute.timestamp())
    print(f"執行 save_aggregated_data，時間: {previous_minute}")

    # 準備要更新的資料，預設為 0
    update_data = {
        'buy_liquidations': default_liquidation_data.copy(),
        'sell_liquidations': default_liquidation_data.copy()
    }

    # 檢查是否有聚合數據
    if previous_minute_ts in aggregated_data:
        print(f"找到 {previous_minute_ts} 的聚合數據: {aggregated_data[previous_minute_ts]}")
        update_data['buy_liquidations'] = aggregated_data[previous_minute_ts].get('buy_liquidations', default_liquidation_data.copy())
        update_data['sell_liquidations'] = aggregated_data[previous_minute_ts].get('sell_liquidations', default_liquidation_data.copy())
    else:
        print(f"{previous_minute_ts} 無聚合數據，使用預設值")

    # 使用 update_one 將資料存入同一個 document
    result = collection.update_one(
        {'timestamp': previous_minute},
        {'$set': update_data},
        upsert=True
    )
    print(f"MongoDB 更新結果: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")

    # 移除已儲存的數據
    if previous_minute_ts in aggregated_data:
        del aggregated_data[previous_minute_ts]

# WebSocket 訊息處理函數
def on_message(ws, message):
    data = json.loads(message)
    order_data = data['o']
    print(f"收到 WebSocket 訊息: {order_data}")

    # 將毫秒時間戳轉換為 datetime 物件
    trade_time_ms = order_data['T']  # 毫秒時間戳
    trade_time = datetime.utcfromtimestamp(trade_time_ms / 1000)

    # 將時間向下取整到分鐘（UTC）
    minute_start_utc = trade_time.replace(second=0, microsecond=0)
    minute_ts = int(minute_start_utc.timestamp())

    symbol = order_data['s']  # e.g., BTCUSDT
    side = order_data['S'].lower()  # 'buy' 或 'sell'
    price = float(order_data['p'])
    quantity = float(order_data['q'])
    total_dollars = round(price * quantity, 2)

    # 應用過濾條件，只處理 BTCUSDT
    if symbol == 'BTCUSDT' and (minimum_total_dollars is None or total_dollars >= minimum_total_dollars):
        # 初始化該分鐘的數據結構
        if minute_ts not in aggregated_data:
            aggregated_data[minute_ts] = {
                'buy_liquidations': {'total_quantity': 0, 'total_dollars': 0, 'event_count': 0},
                'sell_liquidations': {'total_quantity': 0, 'total_dollars': 0, 'event_count': 0}
            }

        # 更新聚合數據
        if side == 'buy':
            aggregated_data[minute_ts]['buy_liquidations']['total_quantity'] += quantity
            aggregated_data[minute_ts]['buy_liquidations']['total_dollars'] += total_dollars
            aggregated_data[minute_ts]['buy_liquidations']['event_count'] += 1
        elif side == 'sell':
            aggregated_data[minute_ts]['sell_liquidations']['total_quantity'] += quantity
            aggregated_data[minute_ts]['sell_liquidations']['total_dollars'] += total_dollars
            aggregated_data[minute_ts]['sell_liquidations']['event_count'] += 1

        print(f"更新 {symbol} 的 {side} 聚合數據，時間: {minute_start_utc}, aggregated_data: {aggregated_data[minute_ts]}")
    else:
        print(f"事件被過濾掉: {symbol}")

# WebSocket 錯誤處理
def on_error(ws, error):
    print('錯誤:', error)

# WebSocket 關閉處理
def on_close(ws):
    print('連線已關閉')

# WebSocket 開啟處理
def on_open(ws):
    print('連線已開啟')

# 設定排程器，每分鐘執行一次儲存
schedule.every(1).minutes.do(save_aggregated_data)

# 排程器執行函數（在獨立執行緒中運行）
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

# 主程式啟動
if __name__ == "__main__":
    # 啟動排程器執行緒
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()

    # 啟動 WebSocket
    ws = websocket.WebSocketApp(socket,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()