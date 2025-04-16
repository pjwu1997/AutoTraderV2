import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime

# 連接到 MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['trading_data']

async def binance_futures_kline(symbol, interval):
    # Binance USDS-Margined Futures WebSocket URL
    uri = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_{interval}"
    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            kline = data['k']
            # 獲取開盤時間並轉換為 datetime (timestamp 是毫秒，轉為秒)
            open_time = datetime.utcfromtimestamp(kline['t'] / 1000)
            # 將 timestamp 統一為每分鐘的 0 秒
            timestamp = open_time.replace(second=0, microsecond=0)
            # 準備要存入的資料
            future_kline_data = {
                'open': kline['o'],
                'high': kline['h'],
                'low': kline['l'],
                'close': kline['c'],
                'volume': kline['v'],
                'quote_volume': kline['q'],
                'trade_num': kline['n'],
                'taker_buy_base': kline['V'],
                'taker_buy_quote': kline['Q']
            }
            # 取得對應幣別的 collection
            collection = db[symbol.upper()]
            # 更新或插入資料，使用 timestamp 作為 key
            collection.update_one(
                {'timestamp': timestamp},
                {'$set': {'kline': future_kline_data}},
                upsert=True
            )
            print(f"Stored kline data for {symbol} at {timestamp}")

# 主函數
async def main():
    symbol = 'BTCUSDT'  # 可替換為其他幣別，例如 ETHUSDT
    interval = '1m'     # 1 分鐘 Kline，可根據需要調整
    await binance_futures_kline(symbol, interval)

# 運行程式
if __name__ == "__main__":
    asyncio.run(main())