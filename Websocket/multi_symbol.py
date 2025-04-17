import asyncio
import websockets
import json
from pymongo import MongoClient
from datetime import datetime

# MongoDB 設定
client = MongoClient('mongodb://localhost:27017/')
db = client['multikline_poc']

# 測試幣種
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'XRPUSDT',
           'DOGEUSDT', 'DOTUSDT', 'SOLUSDT', 'MATICUSDT', 'LTCUSDT']
INTERVAL = '1m'

def calculate_metrics(kline):
    try:
        high = float(kline['h'])
        taker_buy_quote = float(kline['Q'])
        taker_buy_base = float(kline['V'])
        trade_num = float(kline['n'])
        if high == 0:
            return 0, 0
        cvd = taker_buy_quote - taker_buy_base / high
        volume = trade_num / high
    except (ValueError, KeyError):
        cvd, volume = 0, 0
    return cvd, volume

async def binance_kline(symbols, interval, market_type):
    stream_paths = '/'.join(f"{s.lower()}@kline_{interval}" for s in symbols)
    if market_type == 'spot':
        uri = f"wss://stream.binance.com:9443/stream?streams={stream_paths}"
    elif market_type == 'futures':
        uri = f"wss://fstream.binance.com/stream?streams={stream_paths}"
    else:
        raise ValueError("market_type 必須為 'spot' 或 'futures'")

    async with websockets.connect(uri) as ws:
        async for message in ws:
            data = json.loads(message)
            k = data['data']['k']
            symbol = k['s']
            open_time = datetime.utcfromtimestamp(k['t'] / 1000)
            timestamp = open_time.replace(second=0, microsecond=0)

            cvd, vol = calculate_metrics(k)

            update_data = {
                f"{market_type}": {
                    'open': k['o'],
                    'high': k['h'],
                    'low': k['l'],
                    'close': k['c'],
                    'volume': k['v'],
                    'quote_volume': k['q'],
                    'trade_num': k['n'],
                    'taker_buy_base': k['V'],
                    'taker_buy_quote': k['Q'],
                    'cvd': cvd,
                    'calculated_volume': vol
                },
                'timestamp': timestamp,
                'symbol': symbol
            }

            # 更新對應 collection (依照 symbol 分開)，並合併 spot/futures 資料
            collection = db[symbol]
            collection.update_one(
                {'timestamp': timestamp, 'symbol': symbol},
                {'$set': update_data},
                upsert=True
            )
            print(f"[{symbol}] updated {market_type} at {timestamp}")

async def main():
    await asyncio.gather(
        binance_kline(SYMBOLS, INTERVAL, 'spot'),
        binance_kline(SYMBOLS, INTERVAL, 'futures'),
    )

if __name__ == '__main__':
    asyncio.run(main())
