import time
import requests
import os
import logging
import json
from logging.handlers import RotatingFileHandler
from pymongo import MongoClient
from datetime import datetime
from binance.client import Client
from typing import List

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler('multisymbol_data.log', maxBytes=10*1024*1024, backupCount=5),  # Rotate logs at 10MB
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger('multisymbol_data')

# Custom JSON formatter for structured logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'message': record.msg,
            'logger': record.name,
            'symbol': getattr(record, 'symbol', None),  # Add symbol as metadata if available
            'operation': getattr(record, 'operation', None)  # Add operation type if available
        }
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})

# Apply JSON formatter to the console handler
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(JsonFormatter())

# Fetch environment variables
MONGODB_URI = os.getenv('MONGODB_URI', '')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'trade_data')
API_KEY = os.getenv('BINANCE_API_KEY', '')
API_SECRET = os.getenv('BINANCE_API_SECRET', '')
SYMBOLS = os.getenv('SYMBOLS', '').split(',')
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', '60'))  # Fetch interval in seconds

# MongoDB setup
client = MongoClient(MONGODB_URI)
db = client[MONGO_DB_NAME]

# Binance API setup
binance_client = Client(API_KEY, API_SECRET)

# Global variables for caching margin fees and market caps
latest_rate = {}  # Dict to store rates for multiple assets
last_margin_fetch_hour = None
latest_funding_rate = {}  # Store the latest funding rate for each symbol
LAST_MARKET_CAPS = {}
LAST_MARKET_CAPS_HOUR = None

# Retry mechanism for API calls
def fetch_with_retries(fetch_func, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return fetch_func()
        except Exception as e:
            logger.error(f"Attempt {attempt+1} failed: {e}", extra={'operation': 'fetch_with_retries'})
            time.sleep(delay)
    logger.error("All retry attempts failed", extra={'operation': 'fetch_with_retries'})
    return None

# Fetch margin fee from Binance API (hourly)
def fetch_margin_fee(assets: str):
    global latest_rate, last_margin_fetch_hour
    current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    if last_margin_fetch_hour == current_hour:
        return  # Skip if already fetched this hour

    try:
        rates = binance_client.get_future_hourly_interest_rate(assets=assets, isIsolated=False)
        if rates and len(rates) > 0:
            for rate_info in rates:
                asset = rate_info['asset']
                latest_rate[asset] = float(rate_info['nextHourlyInterestRate'])
            last_margin_fetch_hour = current_hour
            logger.info(f"Updated hourly interest rates: {latest_rate}", extra={'operation': 'fetch_margin_fee'})
        else:
            logger.warning("Failed to fetch margin fee: empty result", extra={'operation': 'fetch_margin_fee'})

    except Exception as e:
        logger.error(f"Error fetching margin fee: {e}", extra={'operation': 'fetch_margin_fee'})

# Fetch open interest
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

# Fetch long/short ratio
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
            raise ValueError("Long/Short Ratio returned empty data")
    return fetch_with_retries(_fetch)

# Fetch premium index
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
            raise ValueError("Premium Index returned empty data")
    return fetch_with_retries(_fetch)

# Fetch funding rate history (latest record only)
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
            raise ValueError("Funding Rate returned empty data")
    result = fetch_with_retries(_fetch)
    if result is None and symbol in latest_funding_rate:
        return {
            'funding_rate': latest_funding_rate[symbol],
            'timestamp': datetime.utcnow().replace(second=0, microsecond=0),
            'is_fallback': True
        }
    return result

# Convert Binance symbols to CoinGecko symbols
def convert_to_coingecko_symbols(symbols):
    return [symbol.replace("USDT", "").lower() for symbol in symbols]

# Fetch market caps using CoinGecko API
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

# Fetch market caps from CoinGecko API (hourly)
def get_market_caps(symbols):
    global LAST_MARKET_CAPS, LAST_MARKET_CAPS_HOUR
    current_hour = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    if LAST_MARKET_CAPS_HOUR == current_hour:
        logger.info("Using cached market cap data", extra={'operation': 'get_market_caps'})
        return LAST_MARKET_CAPS

    logger.info("Fetching new market cap data from CoinGecko API", extra={'operation': 'get_market_caps'})
    coingecko_symbols = convert_to_coingecko_symbols(symbols)
    market_caps = fetch_market_caps(coingecko_symbols)

    if market_caps:
        LAST_MARKET_CAPS = market_caps
        LAST_MARKET_CAPS_HOUR = current_hour
        logger.info(f"Updated market caps: {market_caps}", extra={'operation': 'get_market_caps'})
    else:
        logger.warning("Failed to fetch market caps from API, using cached data if available", extra={'operation': 'get_market_caps'})

    return LAST_MARKET_CAPS

# Save market data to MongoDB
def save_market_data(symbols: List[str]):
    for symbol in symbols:
        open_interest_data = fetch_open_interest(symbol)
        long_short_data = fetch_long_short_ratio(symbol)
        premium_index_data = fetch_premium_index(symbol)
        funding_rate_data = fetch_funding_rate(symbol)

        # Check data integrity
        if not all([open_interest_data, long_short_data, premium_index_data, funding_rate_data]):
            logger.warning(f"Some data fetching failed, skipping symbol: {symbol}", extra={'symbol': symbol, 'operation': 'save_market_data'})
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

        logger.info(f"Updated market data for symbol {symbol}: timestamp={timestamp}, upserted_id={result.upserted_id}", extra={'symbol': symbol, 'operation': 'save_market_data'})

# Save spot margin fee and market caps (runs every minute but updates hourly)
def save_spot_margin_fee_and_market_caps(symbols: List[str]):
    base_assets = [symbol.replace('USDT', '') for symbol in symbols]
    assets_str = ','.join(base_assets)

    # Fetch margin fee
    fetch_margin_fee(assets=assets_str)

    # Fetch market caps (updated hourly)
    market_caps = get_market_caps(symbols)

    timestamp = datetime.utcnow().replace(second=0, microsecond=0)

    for symbol in symbols:
        base_asset = symbol.replace('USDT', '')
        update_data = {}

        # Add margin fee data if available
        if base_asset in latest_rate and latest_rate[base_asset] is not None:
            update_data["spot_margin_fee"] = {
                "dailyInterestRate": latest_rate[base_asset]
            }
        else:
            logger.warning(f"No valid margin fee data available for {base_asset}", extra={'symbol': symbol, 'operation': 'save_spot_margin_fee_and_market_caps'})

        # Add market cap data if available
        if market_caps and base_asset in market_caps:
            update_data["market_cap"] = market_caps[base_asset]

        # Update only if there is data to save
        if update_data:
            update_data["symbol"] = symbol
            collection = db[symbol]
            '''
            result = collection.update_one(
                {"timestamp": timestamp},
                {"$set": update_data},
                upsert=True
            )
            logger.info(f"Updated margin fee and/or market cap for symbol {symbol}: timestamp={timestamp}, upserted_id={result.upserted_id}", extra={'symbol': symbol, 'operation': 'save_spot_margin_fee_and_market_caps'})
            '''

# Main program entry
if __name__ == "__main__":
    logger.info("Initializing configuration", extra={'operation': 'main'})
    logger.info(f"MongoDB URI: {MONGODB_URI}", extra={'operation': 'main'})
    logger.info(f"Database: {MONGO_DB_NAME}", extra={'operation': 'main'})
    logger.info(f"Symbols: {SYMBOLS}", extra={'operation': 'main'})
    logger.info(f"Fetch interval: {FETCH_INTERVAL} seconds", extra={'operation': 'main'})

    if not API_KEY or not API_SECRET:
        logger.warning("Binance API key or secret not set", extra={'operation': 'main'})

    while True:
        logger.info(f"Starting data fetch at {datetime.utcnow().isoformat()}", extra={'operation': 'main'})
        save_market_data(SYMBOLS)
        save_spot_margin_fee_and_market_caps(SYMBOLS)
        logger.info("Waiting for next fetch cycle", extra={'operation': 'main'})
        time.sleep(FETCH_INTERVAL)