import ccxt.async_support as ccxt
import asyncio
import schedule
import time
import threading
from pymongo import MongoClient
from datetime import datetime, timedelta

# MongoDB 連接設定
client = MongoClient('mongodb://localhost:27017/')
db = client['trading_data']
collection = db['BTCUSDT']

# CCXT Binance Futures 設定
SYMBOL = 'BTCUSDT'

def binance_to_ccxt(symbol):
    """
    Convert Binance trading pair format to CCXT format.
    
    Args:
        symbol (str): Binance trading pair (e.g., 'BTCUSDT').
    
    Returns:
        str: CCXT trading pair (e.g., 'BTC/USDT').
    """
    if len(symbol) > 4 and symbol[-4:] in ['USDT', 'BUSD', 'TUSD']:
        base = symbol[:-4]
        quote = symbol[-4:]
    elif len(symbol) > 3 and symbol[-3:] in ['BTC', 'ETH', 'BNB']:
        base = symbol[:-3]
        quote = symbol[-3:]
    else:
        # Fallback for pairs with less common bases/quotes
        base, quote = symbol[:len(symbol)//2], symbol[len(symbol)//2:]
    return f"{base}/{quote}:{quote}"

async def fetch_open_interest(exchange):
    current_time = datetime.utcnow().replace(second=0, microsecond=0)
    try:
        open_interest = await exchange.fetchOpenInterest(binance_to_ccxt(SYMBOL))
        return {
            'open_interest': float(open_interest['openInterest']),
            'timestamp': current_time
        }
    except Exception as e:
        print(f"獲取 Open Interest 失敗: {e}")
        return {'open_interest': 0, 'timestamp': current_time}

async def fetch_long_short_ratio(exchange):
    current_time = datetime.utcnow().replace(second=0, microsecond=0)
    try:
        long_short_data = await exchange.fetchLongShortRatio(binance_to_ccxt(SYMBOL), period='5m')
        if isinstance(long_short_data, list) and len(long_short_data) > 0:
            record = long_short_data[0]
            return {
                'long_short_ratio': {
                    'longShortRatio': float(record['longShortRatio']),
                    'longAccount': float(record['longAccount']),
                    'shortAccount': float(record['shortAccount'])
                },
                'timestamp': current_time
            }
        else:
            print(f"Long/Short Ratio 回傳無效資料: {long_short_data}")
            return {
                'long_short_ratio': {
                    'longShortRatio': 0,
                    'longAccount': 0,
                    'shortAccount': 0
                },
                'timestamp': current_time
            }
    except Exception as e:
        print(f"獲取 Long/Short Ratio 失敗: {e}")
        return {
            'long_short_ratio': {
                'longShortRatio': 0,
                'longAccount': 0,
                'shortAccount': 0
            },
            'timestamp': current_time
        }

async def save_market_data():
    current_time = datetime.utcnow().replace(second=0, microsecond=0)
    
    # 初始化 CCXT Binance Futures
    exchange = getattr(ccxt, 'binance')()
   
    
    try:
        # 獲取 Open Interest
        open_interest_data = await fetch_open_interest(exchange)
        # 獲取 Long/Short Ratio
        long_short_data = await fetch_long_short_ratio(exchange)
        
        # 準備更新資料
        update_data = {
            'open_interest': open_interest_data['open_interest'],
            'long_short_ratio': long_short_data['long_short_ratio']
        }
        
        # 存入 MongoDB
        result = collection.update_one(
            {'timestamp': current_time},
            {'$set': update_data},
            upsert=True
        )
        print(f"MongoDB 更新結果: timestamp={current_time}, matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
    finally:
        await exchange.close()

def run_async_task():
    asyncio.run(save_market_data())

# 排程器執行函數
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)

# 主程式
if __name__ == "__main__":
    # 每分鐘執行一次
    schedule.every(1).minutes.do(run_async_task)
    
    # 啟動排程器執行緒
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()
    
    # 保持主執行緒運行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程式終止")