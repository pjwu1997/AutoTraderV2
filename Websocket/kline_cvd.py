import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime

# 連接到 MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['trading_data']

def calculate_metrics(kline):
    """計算 CVD 和 volume"""
    try:
        high = float(kline['h'])  # 最高價
        taker_buy_quote = float(kline['Q'])  # 主動買入報價資產量
        taker_buy_base = float(kline['V'])  # 主動買入基礎資產量
        trade_num = float(kline['n'])  # 交易次數
        
        # 避免除以零
        if high == 0:
            cvd = 0
            volume = 0
        else:
            cvd = taker_buy_quote - taker_buy_base / high
            volume = trade_num / high
    except (ValueError, KeyError):
        cvd = 0
        volume = 0
    
    return cvd, volume

async def binance_kline(symbol, interval, market_type):
    # 根據市場類型選擇 WebSocket URL
    if market_type == 'spot':
        uri = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@kline_{interval}"
    elif market_type == 'futures':
        uri = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_{interval}"
    else:
        raise ValueError("請使用 'spot' 或 'futures' 作為 market_type")

    async with websockets.connect(uri) as websocket:
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            kline = data['k']
            open_time = datetime.utcfromtimestamp(kline['t'] / 1000)
            timestamp = open_time.replace(second=0, microsecond=0)
            
            # 計算 CVD 和 volume
            cvd, volume = calculate_metrics(kline)
            
            # 準備存入的資料
            kline_data = {
                'open': kline['o'],
                'high': kline['h'],
                'low': kline['l'],
                'close': kline['c'],
                'volume': kline['v'],
                'quote_volume': kline['q'],
                'trade_num': kline['n'],
                'taker_buy_base': kline['V'],
                'taker_buy_quote': kline['Q'],
                'cvd': cvd,
                'calculated_volume': volume
            }
            
            # 存入 MongoDB，使用 timestamp 作為 key
            collection = db[symbol.upper()]
            if market_type == 'spot':
                collection.update_one(
                    {'timestamp': timestamp},
                    {'$set': {'spot': kline_data}},
                    upsert=True
                )
            elif market_type == 'futures':
                collection.update_one(
                    {'timestamp': timestamp},
                    {'$set': {'futures': kline_data}},
                    upsert=True
                )
            print(f"已更新 {market_type} 的 {symbol} 資料於 {timestamp}")

async def main():
    symbol = 'BTCUSDT'
    interval = '1m'
    # 並行處理 Spot 和 Futures
    tasks = [
        binance_kline(symbol, interval, 'spot'),
        binance_kline(symbol, interval, 'futures')
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())