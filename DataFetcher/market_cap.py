import time
import pytz
import requests
from pymongo import MongoClient
from datetime import datetime
from binance.client import Client
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
latest_funding_rate = {}  # 儲存每個 symbol 的最新 funding rate

# 市值資料緩存變數
LAST_MARKET_CAPS = {}
LAST_MARKET_CAPS_HOUR = None

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

# 取得 Funding Rate 歷史紀錄（僅取最新一筆）
def fetch_funding_rate(symbol: str):
    global latest_funding_rate
    def _fetch():
        url = "https://fapi.binance.com/fapi/v1/fundingRate"
        params = {
            "symbol": symbol,
            "limit": 1
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data:
            record = data[0]
            funding_rate = float(record['fundingRate'])
            latest_funding_rate[symbol] = funding_rate
            return {
                'funding_rate': funding_rate,
                'timestamp': datetime.utcfromtimestamp(int(record['fundingTime']) / 1000).replace(second=0, microsecond=0)
            }
        else:
            raise ValueError("Funding Rate 回傳空資料")
    result = fetch_with_retries(_fetch)
    if result is None and symbol in latest_funding_rate:
        return {
            'funding_rate': latest_funding_rate[symbol],
            'timestamp': datetime.utcnow().replace(second=0, microsecond=0),
            'is_fallback': True
        }
    return result

# 將 Binance 交易對符號轉換為 CoinGecko 符號
def convert_to_coingecko_symbols(symbols):
    return [symbol.replace("USDT", "").lower() for symbol in symbols]

# 使用 CoinGecko API 獲取指定符號的市值
def fetch_market_caps(coingecko_symbols):
    def _fetch():
        symbols_str = ",".join(coingecko_symbols)
        url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&symbols={symbols_str}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        market_caps = {}
        for coin in data:
            if coin.get('market_cap') is not None:
                market_caps[coin['symbol'].upper()] = coin['market_cap']
        return market_caps
    return fetch_with_retries(_fetch)

# 每小時從 CoinGecko API 拉一次市值數據
def get_market_caps(symbols):
    global LAST_MARKET_CAPS, LAST_MARKET_CAPS_HOUR
    current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    if LAST_MARKET_CAPS_HOUR == current_hour:
        print(f"[{datetime.now()}] 使用緩存市值數據")
        return LAST_MARKET_CAPS  # 本小時已抓過，使用緩存數據

    print(f"[{datetime.now()}] 新的一小時，查詢 CoinGecko API...")
    coingecko_symbols = convert_to_coingecko_symbols(symbols)
    market_caps = fetch_market_caps(coingecko_symbols)
    
    if market_caps:
        LAST_MARKET_CAPS = market_caps
        LAST_MARKET_CAPS_HOUR = current_hour
        print(f"[{datetime.now()}] 更新緩存市值: {market_caps}")
    else:
        print(f"[{datetime.now()}] API 查詢失敗，使用緩存數據（如果存在）")
    
    return LAST_MARKET_CAPS

# 儲存市場資料
def save_market_data(symbols: List[str]):
    for symbol in symbols:
        open_interest_data = fetch_open_interest(symbol)
        long_short_data = fetch_long_short_ratio(symbol)
        premium_index_data = fetch_premium_index(symbol)
        funding_rate_data = fetch_funding_rate(symbol)

        # 檢查資料完整性
        if not all([open_interest_data, long_short_data, premium_index_data, funding_rate_data]):
            print(f"[警告] 部分資料獲取失敗，跳過 {symbol}")
            continue

        timestamp = open_interest_data['timestamp']
        update_data = {
            'symbol': symbol,
            'open_interest': open_interest_data['open_interest'],
            'long_short_ratio': long_short_data['long_short_ratio'],
            'premium_index': premium_index_data['premium_index'],
            'funding_rate': funding_rate_data['funding_rate']
        }

        collection = db[symbol]
        result = collection.update_one(
            {'timestamp': timestamp},
            {'$set': update_data},
            upsert=True
        )

        print(f"[MongoDB] {symbol} 資料已更新: {timestamp}, upserted_id={result.upserted_id}")

# 儲存 margin fee 和 market caps（每分鐘執行，但只會每小時更新一次資料）
def save_spot_margin_fee_and_market_caps(symbols: List[str]):
    base_assets = [symbol.replace('USDT', '') for symbol in symbols]
    assets_str = ','.join(base_assets)

    # 獲取 margin fee
    fetch_margin_fee(assets=assets_str)
    
    # 獲取市值數據（每小時更新一次）
    market_caps = get_market_caps(symbols)
    
    timestamp = datetime.utcnow().replace(second=0, microsecond=0)
    
    for symbol in symbols:
        base_asset = symbol.replace('USDT', '')
        update_data = {}
        
        # 加入 margin fee 數據（如果有效）
        if base_asset in latest_rate and latest_rate[base_asset] is not None:
            update_data["spot_margin_fee"] = {
                "dailyInterestRate": latest_rate[base_asset]
            }
        else:
            print(f"[警告] 尚未抓到 {base_asset} 的有效 margin fee")
        
        # 加入市值數據（如果有效）
        if market_caps and base_asset in market_caps:
            update_data["market_cap"] = market_caps[base_asset]
        
        # 只有在有數據要更新時才進行更新
        if update_data:
            update_data["symbol"] = symbol
            collection = db[symbol]
            result = collection.update_one(
                {"timestamp": timestamp},
                {"$set": update_data},
                upsert=True
            )
            print(f"[MongoDB] {symbol} margin fee 和/或市值已更新: {timestamp}, upserted_id={result.upserted_id}")

# 主程式入口
if __name__ == "__main__":
    while True:
        print(f"\n==== 開始擷取資料 {datetime.utcnow().isoformat()} ====")
        save_market_data(SYMBOLS)
        save_spot_margin_fee_and_market_caps(SYMBOLS)
        print("==== 等待下一輪擷取 ====")
        time.sleep(60)