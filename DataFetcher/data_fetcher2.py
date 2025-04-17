import ccxt
import time
from pymongo import MongoClient, errors
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
from mplfinance.original_flavor import candlestick_ohlc
import requests
import re
import pandas as pd

def remove_100_pattern(text):
    """Remove all patterns of '100*' (where * is all trailing 0s) from the input text."""
    return re.sub(r'1000*', '', text)

def binance_to_ccxt(symbol):
    """
    Convert Binance trading pair format to CCXT format.
    
    Args:
        symbol (str): Binance trading pair (e.g., 'BTCUSDT').
    
    Returns:
        str: CCXT trading pair (e.g., 'BTC/USDT').
    """
    if len(symbol) > 4 and symbol[-4:] in ['USDT', 'BUSD', 'TUSD', 'USDC']:
        base = symbol[:-4]
        quote = symbol[-4:]
    elif len(symbol) > 3 and symbol[-3:] in ['BTC', 'ETH', 'BNB']:
        base = symbol[:-3]
        quote = symbol[-3:]
    else:
        base, quote = symbol[:len(symbol)//2], symbol[len(symbol)//2:]
    return f"{base}/{quote}:{quote}"

def timestamp_to_utc(timestamp_ms):
    """Convert a Unix timestamp in milliseconds to a UTC datetime object."""
    try:
        utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        return utc_time
    except (ValueError, TypeError):
        raise ValueError("Invalid timestamp format. Expected a Unix timestamp in milliseconds.")

def utc_to_timestamp(utc_time):
    """Convert a UTC datetime object to a Unix timestamp in milliseconds."""
    if not isinstance(utc_time, datetime):
        raise ValueError("Input must be a datetime object.")
    return int(utc_time.timestamp() * 1000)

class DataCollector:
    def __init__(self, exchange_name, db_uri="mongodb://localhost:27017/", db_name="TradingData", timeframe='1m'):
        """Initialize MongoDB connection and exchange."""
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.exchange = getattr(ccxt, exchange_name)()
        if not self.exchange.has["fetchOHLCV"]:
            raise ValueError(f"{exchange_name} does not support OHLCV data.")
        self.timeframe = timeframe
        self.timeframe_ms = self.timeframe_to_ms(timeframe)

    def timeframe_to_ms(self, timeframe):
        """Convert timeframe string to milliseconds."""
        timeframe_map = {
            '1m': 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '3h': 3 * 60 * 1000,
            '6h': 6 * 60 * 1000,
            '12h': 12 * 60 * 1000,
            '1d': 24 * 60 * 1000
        }
        return timeframe_map.get(timeframe, 5 * 60 * 1000)

    def fetch_ohlcv(self, symbol, start_time, end_time, period='1m', limit=100):
        """Fetch OHLCV data directly with start_time and end_time."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(binance_to_ccxt(symbol), timeframe=period, since=start_time, limit=limit)
            return [
                {
                    "timestamp": entry[0],
                    "open": entry[1],
                    "high": entry[2],
                    "low": entry[3],
                    "close": entry[4],
                    "volume": entry[5]
                }
                for entry in ohlcv if start_time <= entry[0] < end_time
            ]
        except Exception as e:
            print(f"Error fetching OHLCV for {symbol}: {e}")
            return []

    def get_spot_cvd(self, symbol, start_time, end_time, period='1m', limit=100):
        """Fetch Spot CVD data."""
        url = "https://api.binance.com/api/v3/klines"
        try:
            params = {
                'symbol': remove_100_pattern(symbol.replace("/", "")),
                'interval': period,
                'limit': limit,
                'startTime': start_time,
                'endTime': end_time
            }
            response = requests.get(url, params=params).json()
            return [
                {
                    "timestamp": entry[0],
                    "spot_cvd": float(entry[-3]) - float(entry[-2]) / float(entry[2]),
                    "spot_volume": float(entry[7]) / float(entry[2]),
                }
                for i, entry in enumerate(response) if start_time <= entry[0] < end_time
            ]
        except Exception as e:
            print(f"Error fetching Spot CVD for {symbol}: {e}")
            return []

    def get_future_cvd(self, symbol, start_time, end_time, period='1m', limit=100):
        """Fetch Future CVD data."""
        url = "https://fapi.binance.com/fapi/v1/klines"
        try:
            params = {
                'symbol': symbol.replace("/", ""),
                'interval': period,
                'limit': limit,
                'startTime': start_time,
                'endTime': end_time
            }
            response = requests.get(url, params=params)
            headers = response.headers
            if int(headers['x-mbx-used-weight-1m']) > 2000:
                print(f"Warning: weight is now {int(headers['x-mbx-used-weight-1m'])}")
                time.sleep(60)
            response = response.json()
            return [
                {
                    
                    "timestamp": entry[0],
                    "future_cvd": float(entry[-3]) - float(entry[-2]) / float(entry[2]),
                    "future_volume": float(entry[7]) / float(entry[2]),
                }
                for i, entry in enumerate(response) if start_time <= entry[0] < end_time
            ]
        except Exception as e:
            print(f"Error fetching Future CVD for {symbol}: {e}")
            return []

    def get_premium_index(self, symbol, start_time, end_time, period='1m', limit=100):
        """Fetch Premium Index data."""
        url = "https://fapi.binance.com/fapi/v1/premiumIndexKlines"
        try:
            params = {
                'symbol': symbol.replace("/", ""),
                'limit': limit,
                'startTime': start_time,
                'endTime': end_time,
                'interval': period
            }
            response = requests.get(url, params=params).json()
            return [
                {
                    "timestamp": entry[0],
                    "premium_index_open": float(entry[1]),
                    "premium_index_high": float(entry[2]),
                    "premium_index_low": float(entry[3]),
                    "premium_index_close": float(entry[4]),
                }
                for i, entry in enumerate(response) if start_time <= entry[0] < end_time
            ]
        except Exception as e:
            print(f"Error fetching Premium Index for {symbol}: {e}")
            return []

    def fetch_funding_rate(self, symbol):
        """Fetch funding rate data directly."""
        if not self.exchange.has.get("fetchFundingRate", False):
            print(f"{self.exchange.id} does not support funding rate fetching.")
            return []
        try:
            funding_rate_data = self.exchange.fetchFundingRate(
                binance_to_ccxt(symbol)
            )          
            return [{'timestamp': funding_rate_data['timestamp'], 'funding_rate': funding_rate_data['fundingRate']}]

        except Exception as e:
            print(f"Error fetching funding rate for {symbol}: {e}")
            return []

    def fetch_open_interest(self, symbol, start_time, end_time, period='5m', limit=100):
        """Fetch open interest data."""
        try:
            response = self.exchange.fetchOpenInterestHistory(binance_to_ccxt(symbol), since=start_time, limit=limit, params={"endTime": end_time, "period": period})
            print(f"Open interest response: {response}")
            return [
                {
                    "timestamp": item['timestamp'],
                    "open_interest_amount": item['openInterestAmount'],
                    "open_interest_value": item['openInterestValue']
                }
                for item in response
            ]
        except Exception as e:
            print(f"Error fetching open interest for {symbol}: {e}")
            return []

    def fetch_long_short_ratio(self, symbol, start_time, end_time, period='5m', limit=100):
        """Fetch long-short ratio data."""
        if not self.exchange.has.get("fetchLongShortRatioHistory", False):
            print(f"{self.exchange.id} does not support long-short ratio fetching.")
            return []
        try:
            response = self.exchange.fetchLongShortRatioHistory(
                binance_to_ccxt(symbol),
                period=period,
                since=start_time, limit=limit,
                params={"endTime": end_time}
            )
            print(f"Long-short ratio response: {response}")
            return [
                {
                    "timestamp": item['timestamp'],
                    "long_short_ratio": item['longShortRatio'],
                }
                for item in response
            ]
        except Exception as e:
            print(f"Error fetching long-short ratio for {symbol}: {e}")
            return []

    def get_latest_timestamp(self, symbol):
        collection_name = f"{symbol}_{self.timeframe}"
        collection = self.db[collection_name]
        latest_entry = collection.find_one({}, sort=[("timestamp", -1)])
        return latest_entry["timestamp"] if latest_entry else None

    def fetch_and_store_fixed_range(self, symbol, start_time, end_time, online=False):
        """Fetch and store data for a fixed time range in smaller chunks."""
        period_to_ms = {
            "1m": 60 * 1000,
            "3m": 3 * 60 * 1000,
            "5m": 5 * 60 * 1000,
            "15m": 15 * 60 * 1000,
            "30m": 30 * 60 * 1000,
            "1h": 60 * 60 * 1000,
            "2h": 2 * 60 * 1000,
            "4h": 4 * 60 * 1000,
            "1d": 24 * 60 * 1000,
        }
        timeframe_ms = period_to_ms.get(self.timeframe, 5 * 60 * 1000)
        limit = 300
        max_range_ms = timeframe_ms * limit

        current_start = start_time
        collection_name = f"{symbol}_{self.timeframe}"
        while current_start < end_time:
            temp_end_time = min(current_start + max_range_ms, end_time)
            print(f"Fetching data from {datetime.utcfromtimestamp(current_start / 1000)} " #.fromtimestamp(datetime.timezone.utc)
                  f"to {datetime.utcfromtimestamp(temp_end_time / 1000)}...")

            print(f"[{symbol}] Fetching data from {datetime.utcfromtimestamp(current_start / 1000)} "
                f"to {datetime.utcfromtimestamp(temp_end_time / 1000)}...")

            # Fetch OHLCV
            ohlcv = self.fetch_ohlcv(symbol, current_start, temp_end_time, period=self.timeframe, limit=limit)
            if not ohlcv:
                print(f"No OHLCV data found for {symbol} in the specified range.")
                current_start += max_range_ms
                continue

            spot_cvd = self.get_spot_cvd(symbol, current_start, temp_end_time, period=self.timeframe, limit=limit)
            future_cvd = self.get_future_cvd(symbol, current_start, temp_end_time, period=self.timeframe, limit=limit)
            long_short_ratio = self.fetch_long_short_ratio(symbol, current_start, temp_end_time, period=self.timeframe, limit=limit)
            if not long_short_ratio:
                print(f"No long-short ratio data found for {symbol} in the specified range.")
            open_interest = self.fetch_open_interest(symbol, current_start, temp_end_time, period=self.timeframe, limit=limit)
            premium_index = self.get_premium_index(symbol, current_start, temp_end_time, period=self.timeframe, limit=limit)
            if online:
                current_funding_rate = self.fetch_funding_rate(symbol)

            for i, ohlcv_data in enumerate(ohlcv):
                time_str = datetime.utcfromtimestamp(ohlcv_data["timestamp"] / 1000).strftime('%Y%m%d%H%M%S')
                spot_cvd_data = spot_cvd[i] if i < len(spot_cvd) else None
                future_cvd_data = future_cvd[i] if i < len(future_cvd) else None
                long_short_data = long_short_ratio[i] if i < len(long_short_ratio) else None
                open_interest_data = open_interest[i] if i < len(open_interest) else None
                premium_index_data = premium_index[i] if i < len(premium_index) else None

                data = {
                    "symbol": symbol,
                    "exchange": self.exchange.id,
                    "time_index": time_str,
                    "ohlcv_open": ohlcv_data["open"],
                    "ohlcv_high": ohlcv_data["high"],
                    "ohlcv_low": ohlcv_data["low"],
                    "ohlcv_close": ohlcv_data["close"],
                    "ohlcv_volume": ohlcv_data["volume"],
                    "spot_cvd_value": spot_cvd_data["spot_cvd"] if spot_cvd_data else None,
                    "spot_cvd_volume": spot_cvd_data["spot_volume"] if spot_cvd_data else None,
                    "future_cvd_value": future_cvd_data["future_cvd"] if future_cvd_data else None,
                    "future_cvd_volume": future_cvd_data["future_volume"] if future_cvd_data else None,
                    "funding_rate_value": None,
                    "premium_index_open": premium_index_data['premium_index_open'] if premium_index_data else None,
                    "premium_index_high": premium_index_data['premium_index_high'] if premium_index_data else None,
                    "premium_index_low": premium_index_data['premium_index_low'] if premium_index_data else None,
                    "premium_index_close": premium_index_data['premium_index_close'] if premium_index_data else None,
                    "long_short_ratio_value": long_short_data["long_short_ratio"] if long_short_data else None,
                    "open_interest_amount": open_interest_data["open_interest_amount"] if open_interest_data else None,
                    "open_interest_value": open_interest_data["open_interest_value"] if open_interest_data else None,
                    "liquidation_data": {},  # 初始化清算數據
                    "margin_rate": {},       # 初始化 margin_rate
                    "timestamp": timestamp_to_utc(ohlcv_data["timestamp"]),
                    "input_timestamp": datetime.utcnow()
                }
                if online:
                    data['funding_rate_value'] = current_funding_rate[0]['funding_rate']
                # Condition to check for existing document
                query = {"symbol": symbol, "timestamp": data["timestamp"]}
                try:
                    # self.db[collection_name].insert_one(data)
                    # print(f"Inserted data for {symbol} at {time_str}")
                    # Use update_one with upsert=False to avoid inserting duplicates
                    result = self.db[collection_name].update_one(query, {"$setOnInsert": data}, upsert=True)

                    if result.matched_count > 0:
                        print(f"Document with symbol {symbol} and timestamp {data['timestamp']} already exists. Insert skipped.")
                    elif result.upserted_id:
                        print(f"New document inserted with _id: {result.upserted_id}")
                except errors.DuplicateKeyError:
                    print(f"Data for {symbol} at {time_str} already exists. Skipping insert.")

            current_start = temp_end_time

        print(f"Completed fetching and storing data for {symbol} from {start_time} to {end_time}.")

    def fetch_and_store_online(self, symbol):
        """Fetch and store data for online (up-to-the-minute) data, aligned with the timeframe."""
        current_time = int(time.time() * 1000)
        latest_entry = self.db['market_data'].find({"symbol": symbol}).sort("timestamp", -1).limit(1)
        latest_timestamp = latest_entry[0]['timestamp'] if latest_entry.count() > 0 else current_time - 10 * self.timeframe_ms

        # Define the time range (start time slightly before the latest timestamp, end time = current time)
        start_time = latest_timestamp - self.timeframe_ms
        end_time = current_time
        self.fetch_and_store_fixed_range(symbol, start_time, end_time, online=True)

    def clean_db(self):
        """Clean old records (not used)."""
        cutoff_time = datetime.utcnow() - timedelta(days=30)
        result = self.db['market_data'].delete_many({"timestamp": {"$lt": cutoff_time}})
        print(f"Deleted {result.deleted_count} old records.")

    def run(self, symbol, interval=300):
        """Run the data collector periodically."""
        while True:
            self.fetch_and_store_online(symbol)
            print(f"Sleeping for {interval} seconds...")
            time.sleep(interval)

    def plot(self, symbol, field="ohlcv_close", timeframe='1m'):
        """Plot a selected field from the stored data."""
        collection_name = f"{symbol}_{timeframe}"
        cursor = self.db[collection_name].find({"symbol": symbol}).sort("timestamp", 1)
        timestamps, values = [], []
        for doc in cursor:
            timestamps.append(doc['timestamp'])
            values.append(doc[field])

        plt.figure(figsize=(12, 6))
        plt.plot(timestamps, values, label=field)
        plt.xlabel("Time")
        plt.ylabel(field.capitalize())
        plt.title(f"{field.capitalize()} Over Time for {symbol}")
        plt.legend()
        plt.grid(True)
        plt.show()

def test_fetch_vine_usdt():
    # Initialize the data collector for Binance with a 5-minute timeframe
    collector = DataCollector(exchange_name="binance", db_uri="mongodb://localhost:27017/", db_name="TradingData", timeframe="5m")

    # Set the start and end times for the range: January 1, 2025 to February 1, 2025
    start_date = datetime(2024, 12, 1, 0, 0, 0)  # January 1, 2025, 00:00:00 UTC
    end_date = datetime(2025, 2, 1, 0, 0, 0)    # February 1, 2025, 00:00:00 UTC

    # Convert start and end times to timestamps (milliseconds)
    start_timestamp = int(time.mktime(start_date.timetuple()) * 1000)
    end_timestamp = int(time.mktime(end_date.timetuple()) * 1000)
    collector.fetch_and_store_fixed_range("VINEUSDT", start_timestamp, end_timestamp)

def fetch_binance_futures_pairs():
    """Fetch all Binance futures trading pairs."""
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url).json()
        pairs = [symbol['symbol'] for symbol in response['symbols'] if symbol['contractType'] == 'PERPETUAL']
        return pairs
    except Exception as e:
        print(f"Error fetching Binance futures pairs: {e}")
        return []

def test_fetch_all_binance_futures(timeframe='5m'):
    # Initialize the data collector for Binance with a 5-minute timeframe
    collector = DataCollector(exchange_name="binance", db_uri="mongodb://pj:yolo1234@localhost:27017/", db_name="TradingData", timeframe=timeframe)

    # Calculate the start and end timestamps for the past week to now
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(weeks=4)
    start_timestamp = int(time.mktime(start_date.timetuple()) * 1000)
    end_timestamp = int(time.mktime(end_date.timetuple()) * 1000)
    trading_pairs = fetch_binance_futures_pairs()
    trading_pairs = ['ARPAUSDT']
    # trading_pairs = ['VINEUSDT']
    if not trading_pairs:
        print("No trading pairs fetched.")
        return
    for pair in trading_pairs:
        print(f"Fetching data for {pair}")
        collector.fetch_and_store_fixed_range(pair, start_timestamp, end_timestamp)

def delete_all_documents(db_uri, db_name, collection_name):
    """Delete all documents in a MongoDB collection."""
    try:
        client = MongoClient(db_uri)
        db = client[db_name]
        collection = db[collection_name]
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents from {collection_name}.")
        client.close()
    except Exception as e:
        print(f"An error occurred: {e}")

def plot_ticker_status(ticker, start_date, end_date, db_name, period='5m', mongo_uri="mongodb://pj:yolo1234@localhost:27017/"):
    """
    Plots the ETHUSDT metrics (Spot CVD, Premium Index, Long/Short Ratio, and Price)
    from the specified MongoDB collection between the given dates.

    Args:
        start_date (str): Start date in ISO format (e.g., "2025-01-25T00:00:00.000+00:00").
        end_date (str): End date in ISO format (e.g., "2025-01-27T23:59:59.999+00:00").
        db_name (str): Name of the MongoDB database.
        mongo_uri (str): MongoDB connection URI. Default is "mongodb://localhost:27017/".
    """
    # Connect to MongoDB
    collection_name = f"{ticker}_{period}"
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    query = {
        "symbol": ticker,
        "timestamp": {"$gte": start_date, "$lte": end_date}
    }
    data = list(collection.find(query))
    if not data:
        print("No data found for the given date range.")
        return
    df = pd.DataFrame(data)

    # Ensure relevant fields are present
    required_columns = ["timestamp", "spot_cvd_value", "premium_index_high", "premium_index_low", "long_short_ratio_value",
                        "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close", "ohlcv_volume", "open_interest_amount"]
    for col in required_columns:
        if col not in df.columns:
            print(f"Column '{col}' not found in the data.")
            return
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.sort_values("timestamp", inplace=True)
    df["timestamp_num"] = mdates.date2num(df["timestamp"])

    # Filter Relevant Columns
    df = df[["timestamp", "timestamp_num", "future_cvd_value", "spot_cvd_value", "premium_index_high", "premium_index_low", "long_short_ratio_value",
             "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close", "ohlcv_volume", "open_interest_amount"]]

    # Plot Data
    fig, axes = plt.subplots(8, 1, figsize=(12, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1, 1, 1, 1, 1, 1, 1]})

    # Candlestick Chart
    ax0 = axes[0]
    ohlc_data = df[["timestamp_num", "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close"]].values
    candlestick_ohlc(ax0, ohlc_data, width=0.0008, colorup='g', colordown='r')
    ax0.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    ax0.set_ylabel("Price (USDT)")
    ax0.set_title(f"{ticker} Price")
    ax0.grid()
    axes[1].plot(df["timestamp"], df["spot_cvd_value"], label="Spot CVD", color="orange")
    axes[1].set_ylabel("Spot CVD")
    axes[1].set_title("Spot CVD")
    axes[1].grid()
    axes[2].plot(df["timestamp"], df["future_cvd_value"], label="Future CVD", color="blue")
    axes[2].set_ylabel("Future CVD")
    axes[2].set_title("Future CVD")
    axes[2].grid()

    # Plot Premium Index
    axes[3].plot(df["timestamp"], df["premium_index_high"], label="Premium Index High", color="green")
    axes[3].set_ylabel("Premium Index High")
    axes[3].set_title("Premium Index High")
    axes[3].grid()

    # Plot Premium Index
    axes[4].plot(df["timestamp"], df["premium_index_low"], label="Premium Index Low", color="green")
    axes[4].set_ylabel("Premium Index Low")
    axes[4].set_title("Premium Index Low")
    axes[4].grid()

    # Plot Long/Short Ratio
    axes[5].plot(df["timestamp"], df["long_short_ratio_value"], label="Long/Short Ratio", color="red")
    axes[5].set_ylabel("Long/Short Ratio")
    axes[5].set_title("Long/Short Ratio")
    axes[5].grid()

    # Plot Volume
    axes[6].plot(df["timestamp"], df["ohlcv_volume"], label="Volume", color="purple")
    axes[6].set_ylabel("Volume")
    axes[6].set_title("Volume")
    axes[6].grid()

    # Plot Open Interest
    axes[7].plot(df["timestamp"], df["open_interest_amount"], label="OI", color="black")
    axes[7].set_ylabel("OI")
    axes[7].set_title("OI")
    axes[7].grid()

    # Formatting
    fig.autofmt_xdate()
    plt.xlabel("Timestamp")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # pass
    plot_ticker_status(
        ticker="BTCUSDT",
        start_date=datetime(2025, 1, 30),
        end_date=datetime(2025, 2, 2, 23, 59, 59),
        db_name="TradingData",
        period='5m'
    )
    # pass
    # datafetcher = DataCollector('binance')
    # datafetcher.clean_db()
    # DB_URI = "mongodb://localhost:27017/"
    # DB_NAME = "trading_data"
    # COLLECTION_NAME = "market_data"

    # delete_all_documents(DB_URI, DB_NAME, COLLECTION_NAME)
    # Call the test function
    # test_fetch_vine_usdt()
    # test_fetch_all_binance_futures()
# %%
