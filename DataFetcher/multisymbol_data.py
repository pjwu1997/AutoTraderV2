import time
from pymongo import MongoClient
from datetime import datetime
from binance.client import Client
import requests
from typing import List

# MongoDB 設定
client = MongoClient('mongodb://localhost:27017/')
db = client['multikline_poc']

# 支援的交易對
SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 'BIGTIMEUSDT',
              'DOGEUSDT', 'DOTUSDT', 'SOLUSDT', 'VINEUSDT', 'FARTCOINUSDT', 'ARKUSDT', 'ALCHUSDT']


# Binance API 設定
API_KEY = 'H95sApwsCkDIUiBxicExq8eVgJIdUsGm7p9mraNwcqNGW2RS6Ryx89TcKZSlV8an'
API_SECRET = 'HsQH0Snzaw8LnmhKeWHbEfrPRmrAcUAjgqmR4Ltv1zA6JqjaZfW289Gb8CoUFMBF'

binance_client = Client(API_KEY, API_SECRET)

# 全域變數記錄最新 margin fee
latest_rate = {}  # Changed to dict to store rates for multiple assets
last_margin_fetch_hour = None

# 重試機制
def fetch_with_retries(fetch_func, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return fetch_func()
        except Exception as e:
            print(f"[錯誤] 第 {attempt+1} 次嘗試失敗: {e}")
            time.sleep(delay)
    print("[錯誤] 所有重試失敗")
    return None

# 每小時從 Binance API 拉一次 Margin Fee
def fetch_margin_fee(assets: str):
    global latest_rate, last_margin_fetch_hour
    current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    if last_margin_fetch_hour == current_hour:
        return  # 本小時已抓過，略過

    try:
        rates = binance_client.get_future_hourly_interest_rate(assets=assets, isIsolated=False)
        if rates and len(rates) > 0:
            for rate_info in rates:
                asset = rate_info['asset']
                latest_rate[asset] = float(rate_info['nextHourlyInterestRate'])
            last_margin_fetch_hour = current_hour
            print(f"[{datetime.now()}] Updated hourly interest rates: {latest_rate}")
        else:
            print(f"[{datetime.now()}] Failed to fetch margin fee (empty result)")

    except Exception as e:
        print(f"[{datetime.now()}] Error fetching margin fee: {e}")

# 取得 Open Interest
def fetch_open_interest(symbol: str):
    def _fetch():
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        params = {"symbol": symbol}
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return {
            'open_interest': float(data['openInterest']),
            'timestamp': datetime.utcnow().replace(second=0, microsecond=0)
        }
    return fetch_with_retries(_fetch)

# 取得 Long/Short Ratio
def fetch_long_short_ratio(symbol: str):
    def _fetch():
        url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
        params = {
            "symbol": symbol,
            "period": "5m",
            "limit": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data:
            record = data[0]
            return {
                'long_short_ratio': {
                    'longShortRatio': float(record['longShortRatio']),
                    'longAccount': float(record['longAccount']),
                    'shortAccount': float(record['shortAccount'])
                },
                'timestamp': datetime.utcfromtimestamp(record['timestamp'] / 1000).replace(second=0, microsecond=0)
            }
        else:
            raise ValueError("Long/Short Ratio 回傳空資料")
    return fetch_with_retries(_fetch)

# 取得 Premium Index
def fetch_premium_index(symbol: str):
    def _fetch():
        url = "https://fapi.binance.com/fapi/v1/premiumIndexKlines"
        params = {
            "symbol": symbol,
            "interval": "1m",
            "limit": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data:
            record = data[0]
            return {
                'premium_index': {
                    'fundingRate': float(record[7]),
                    'markPrice': float(record[4]),
                    'indexPrice': float(record[5])
                },
                'timestamp': datetime.utcfromtimestamp(record[0] / 1000).replace(second=0, microsecond=0)
            }
        else:
            raise ValueError("Premium Index 回傳空資料")
    return fetch_with_retries(_fetch)

# 儲存市場資料
def save_market_data(symbols: List[str]):
    for symbol in symbols:
        open_interest_data = fetch_open_interest(symbol)
        long_short_data = fetch_long_short_ratio(symbol)
        premium_index_data = fetch_premium_index(symbol)

        # Check if any data fetch failed or is missing required keys
        if not all([open_interest_data, long_short_data, premium_index_data]):
            print(f"[警告] 部分資料獲取失敗，跳過 {symbol}")
            continue
        if 'open_interest' not in open_interest_data:
            print(f"[錯誤] Open interest 資料缺少 'open_interest' 鍵，跳過 {symbol}: {open_interest_data}")
            continue
        if 'long_short_ratio' not in long_short_data:
            print(f"[錯誤] Long/short ratio 資料缺少 'long_short_ratio' 鍵，跳過 {symbol}: {long_short_data}")
            continue
        if 'premium_index' not in premium_index_data:
            print(f"[錯誤] Premium index 資料缺少 'premium_index' 鍵，跳過 {symbol}: {premium_index_data}")
            continue

        # 使用 open interest 的 timestamp 作為主時間
        timestamp = open_interest_data['timestamp']
        update_data = {
            'symbol': symbol,
            'open_interest': open_interest_data['open_interest'],
            'long_short_ratio': long_short_data['long_short_ratio'],
            'premium_index': premium_index_data['premium_index']
        }

        collection = db[symbol]
        result = collection.update_one(
            {'timestamp': timestamp},
            {'$set': update_data},
            upsert=True
        )

        print(f"[MongoDB] {symbol} 資料已更新: {timestamp}, upserted_id={result.upserted_id}")

# 儲存 margin fee（每分鐘執行，但只會每小時更新一次資料）
def save_spot_margin_fee(symbols: List[str]):
    # 提取所有基礎資產 (e.g., 'BTC' from 'BTCUSDT')
    base_assets = [symbol.replace('USDT', '') for symbol in symbols]
    # 將資產列表轉為逗號分隔的字串
    assets_str = ','.join(base_assets)
    
    # 批量獲取所有資產的 margin fee
    fetch_margin_fee(assets=assets_str)

    # 為每個 symbol 儲存 margin fee
    for symbol in symbols:
        base_asset = symbol.replace('USDT', '')
        if base_asset not in latest_rate or latest_rate[base_asset] is None:
            print(f"[警告] 尚未抓到 {base_asset} 的有效 margin fee，略過儲存")
            continue

        timestamp = datetime.utcnow().replace(second=0, microsecond=0)
        update_data = {
            "symbol": symbol,
            "spot_margin_fee": {
                "dailyInterestRate": latest_rate[base_asset]
            }
        }

        collection = db[symbol]
        result = collection.update_one(
            {"timestamp": timestamp},
            {"$set": update_data},
            upsert=True
        )

        print(f"[MongoDB] {symbol} 現貨 margin fee 已更新: {timestamp}, upserted_id={result.upserted_id}")

# 主程式入口
if __name__ == "__main__":
    while True:
        print(f"\n==== 開始擷取資料 {datetime.utcnow().isoformat()} ====")
        save_market_data(SYMBOLS)
        save_spot_margin_fee(SYMBOLS)  # 批量處理所有 symbols
        print("==== 等待下一輪擷取 ====")
        time.sleep(60)  # 每 60 秒執行一次