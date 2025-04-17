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

# 幣種設定
set_ticker = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
              'DOGEUSDT', 'DOTUSDT', 'SOLUSDT', 'MATICUSDT', 'LTCUSDT']

# MongoDB 設定
client = MongoClient('mongodb://localhost:27017/')
db = client['multikline_poc']
collections = {symbol: db[symbol] for symbol in set_ticker}

# 聚合暫存區：{symbol: {minute_ts: {...}}}
aggregated_data = {symbol: {} for symbol in set_ticker}

# 預設的清算結構
def default_liquidation():
    return {'total_quantity': 0, 'total_dollars': 0, 'event_count': 0}

# 儲存資料
def save_aggregated_data():
    current_time = datetime.utcnow()
    prev_minute = (current_time - timedelta(minutes=1)).replace(second=0, microsecond=0)
    # prev_ts 改為 datetime 格式，與 K 線程式一致
    prev_ts = prev_minute

    for symbol in set_ticker:
        # 使用 Unix 時間戳作為 key，從 aggregated_data 中取出資料
        unix_ts = int(prev_ts.timestamp())
        data = aggregated_data[symbol].pop(unix_ts, None)
        update_data = {
            'liquidations': {
                'buy_liquidations': default_liquidation(),
                'sell_liquidations': default_liquidation()
            }
        }

        if data:
            update_data['liquidations']['buy_liquidations'] = data.get('buy_liquidations', default_liquidation())
            update_data['liquidations']['sell_liquidations'] = data.get('sell_liquidations', default_liquidation())

        result = collections[symbol].update_one(
            {'timestamp': prev_ts, 'symbol': symbol},
            {'$set': update_data},
            upsert=True
        )
        print(f"[{symbol}] 更新 {prev_ts}：matched={result.matched_count}, modified={result.modified_count}")

# WebSocket 處理
def on_message(ws, message):
    try:
        data = json.loads(message)
        order = data['o']

        symbol = order['s']
        if symbol not in set_ticker:
            return

        side = order['S'].lower()
        price = float(order['p'])
        qty = float(order['q'])
        total_dollars = round(price * qty, 2)

        # 時間戳處理
        trade_time_ms = order['T']  # 毫秒時間戳
        trade_time = datetime.utcfromtimestamp(trade_time_ms / 1000)
        minute_start_utc = trade_time.replace(second=0, microsecond=0)
        minute_ts = int(minute_start_utc.timestamp())  # 用於 aggregated_data 的 key

        # 初始化
        if minute_ts not in aggregated_data[symbol]:
            aggregated_data[symbol][minute_ts] = {
                'buy_liquidations': default_liquidation(),
                'sell_liquidations': default_liquidation()
            }

        if side == 'buy':
            group = aggregated_data[symbol][minute_ts]['buy_liquidations']
        else:
            group = aggregated_data[symbol][minute_ts]['sell_liquidations']

        group['total_quantity'] += qty
        group['total_dollars'] += total_dollars
        group['event_count'] += 1

        print(f"{symbol} {side} 更新於 {minute_start_utc}：{group}")
    except Exception as e:
        print("訊息處理錯誤:", e)

def on_error(ws, error):
    print("錯誤:", error)

def on_close(ws):
    print("連線關閉")

def on_open(ws):
    print("WebSocket 開啟")

# 排程每分鐘儲存
schedule.every(1).minutes.do(save_aggregated_data)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    threading.Thread(target=run_scheduler).start()
    ws = websocket.WebSocketApp(socket,
                               on_message=on_message,
                               on_error=on_error,
                               on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()