# %%
import time
import ccxt
import requests
import datetime
from pymongo import MongoClient
import matplotlib.pyplot as plt
import schedule

#from load_markets import load_perpetual_markets_for_binance

class DataFetcher:
    def __init__(self, exchange_name, db_uri="mongodb://mongo:27017/", db_name="trading_data", timeframe="5m"):
        # Initialize MongoDB connection
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]

        # Initialize exchange using ccxt
        self.exchange = getattr(ccxt, exchange_name)()
        if not self.exchange.has["fetchOHLCV"]:
            raise ValueError(f"{exchange_name} does not support OHLCV data.")
        self.timeframe = timeframe

    def clean_db(self):
        self.db.market_data.delete_many({})

    def fetch_ohlcv(self, symbol, since):
        """Fetch OHLCV data."""
        try:
            ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=self.timeframe, since=since)
            ohlcv_data = [{
                "timestamp": candle[0],
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[5]
            } for candle in ohlcv]
            return ohlcv_data
        except Exception as e:
            print(f"Error fetching OHLCV for {symbol}: {e}")
            return []

    def get_spot_cvd(self, symbol, since, period='15m', limit=100):
        """Get CVD for Binance spot using raw Klines API."""
        url = "https://api.binance.com/api/v3/klines"
        params = {
            'symbol': symbol.replace("/", ""),
            'interval': period,
            'limit': limit,
            'startTime': since
        }

        try:
            # Fetch data from Binance Klines API
            res = requests.get(url, params=params).json()

            if not res:
                print(f"No data returned for {symbol} on period {period}.")
                return []

            # Extract necessary data (OHLCV)
            cvd_data = []
            for candle in res:
                close_price = float(candle[4])  # Close price of the current candle
                prev_close_price = float(candle[3])  # Close price of the previous candle
                volume = float(candle[5])  # Volume of the current candle

                # Calculate CVD and spot volume
                spot_cvd = close_price - prev_close_price  # CVD approximation
                spot_volume = volume / close_price  # Normalized volume

                cvd_data.append({
                    "timestamp": candle[0],
                    "spot_cvd": spot_cvd,
                    "spot_volume": spot_volume
                })

            return cvd_data

        except Exception as e:
            print(f"Error fetching spot CVD for {symbol} from Binance: {e}")
            return []

    def fetch_cvd(self, symbol, since):
        """Calculate CVD using buy/sell volume approximation."""
        if not self.exchange.has["fetchOHLCV"]:
            print(f"{self.exchange.id} does not support OHLCV data.")
            return []

        try:
            cvd_data = []
            cvd = 0

            # Fetch OHLCV data for the given timeframe
            ohlcv = self.fetch_ohlcv(symbol, since)

            for candle in ohlcv:
                close = candle["close"]
                volume = candle["volume"]

                # Approximate buy and sell volumes based on price movement
                if close > candle["open"]:  # Close > Open (bullish)
                    buy_volume = volume
                    sell_volume = 0
                elif close < candle["open"]:  # Close < Open (bearish)
                    sell_volume = volume
                    buy_volume = 0
                else:  # Close == Open (neutral)
                    buy_volume = sell_volume = volume / 2

                # Update CVD based on the volume
                cvd += buy_volume - sell_volume
                cvd_data.append({
                    "timestamp": candle["timestamp"],
                    "cvd": cvd
                })

            return cvd_data

        except Exception as e:
            print(f"Error fetching CVD based on OHLCV for {symbol}: {e}")
            return []
    
    def fetch_funding_rate(self, symbol, since):
        """Fetch funding rate data aggregated by timeframe."""
        if not self.exchange.has.get("fetchFundingRateHistory", False):
            print(f"{self.exchange.id} does not support funding rate fetching.")
            return []

        try:
            # Fetch funding rate data using the 'since' and 'timeframe'
            # funding_rate_data = self.exchange.fetchFundingRateHistory(symbol, params={"since": since, "period": self.timeframe})
            funding_rate_data = self.exchange.fetchFundingRate(symbol)

            # Ensure the returned format is consistent with others
            return [{
                "timestamp": funding_rate_data['timestamp'],
                "funding_rate": funding_rate_data['fundingRate']
            }]

        except Exception as e:
            print(f"Error fetching funding rate for {symbol}: {e}")
            return []


    def fetch_long_short_ratio(self, symbol, since):
        """Fetch long-short ratio aggregated by timeframe."""
        if not self.exchange.has.get("fetchLongShortRatioHistory", False):
            print(f"{self.exchange.id} does not support long-short ratio fetching.")
            return []

        try:
            # Fetch long-short ratio data directly using the 'since' and 'timeframe'
            long_short_ratio_data = self.exchange.fetchLongShortRatioHistory(symbol, params={"since": since, "period": self.timeframe})

            # Ensure the returned format is consistent with others
            return [{
                "timestamp": data['timestamp'],
                "long_short_ratio": data['longShortRatio']
            } for data in long_short_ratio_data]

        except Exception as e:
            print(f"Error fetching long-short ratio for {symbol}: {e}")
            return []

    def store_data(self, collection_name, symbol, data):
        """Store or update the latest data for a symbol in MongoDB."""
        collection = self.db[collection_name]
        # Perform an upsert operation to store the latest data
        # collection.update_one(
        #     {"symbol": symbol},  # Filter by symbol
        #     {"$set": data},      # Update with the new data
        #     upsert=True          # Insert if it doesn't exist
        # )
        collection.insert_one(data)


    def fetch_and_store(self, symbol):
        """Fetch and store the latest data for a symbol, ensuring at least two candles are fetched."""
        # Ensure we fetch at least 2 candles by subtracting twice the timeframe duration
        min_candles = 2
        timeframe_minutes = int(self.timeframe[:-1])  # Extract the numeric part of the timeframe (e.g., 5m -> 5)
        current_time = datetime.datetime.utcnow()
        since = int((datetime.datetime.utcnow() - datetime.timedelta(minutes=timeframe_minutes * (min_candles + 1))).timestamp() * 1000)
        minutes = (current_time.minute // 5) * 5
        timestamp = current_time.replace(minute=minutes, second=0, microsecond=0)
        # Fetch data
        print(f"Fetching OHLCV data for {symbol}...")
        ohlcv = self.fetch_ohlcv(symbol, since)

        print(f"Fetching spot CVD for {symbol}...")
        spot_cvd = self.get_spot_cvd(symbol[:-5], since, period=self.timeframe)

        print(f"Fetching long-short ratio for {symbol}...")
        long_short_ratio = self.fetch_long_short_ratio(symbol, since)

        print(f"Calculating CVD for {symbol}...")
        cvd = self.fetch_cvd(symbol, since)

        print(f"Fetching Funding Rate for {symbol}...")
        fundings = self.fetch_funding_rate(symbol, since)

        # Handle potential None values
        data = {
            "symbol": symbol,
            "exchange": self.exchange.id,
            "ohlcv": ohlcv[-1] if ohlcv else None,
            "spot_cvd": spot_cvd[-1] if spot_cvd else None,
            "long_short_ratio": long_short_ratio[-1] if long_short_ratio else None,
            "cvd": cvd[-1] if cvd else None,
            "funding_rate": fundings[-1] if fundings else None,
            "timestamp": timestamp,
        }

        # Store data in MongoDB
        print(f"Storing data for {symbol}...")
        self.store_data("market_data", symbol, data)


    def test_fetch(self, symbol):
        """Fetch and print data for testing purposes."""
        since = int((datetime.datetime.utcnow() - datetime.timedelta(days=1)).timestamp() * 1000)

        print(f"Fetching OHLCV data for {symbol}...")
        ohlcv = self.fetch_ohlcv(symbol, since)
        print(f"OHLCV data: {ohlcv[:5]}")  # Print only the first 5 entries for brevity

        print(f"Fetching spot CVD for {symbol}...")
        spot_cvd = self.get_spot_cvd(symbol, since, period=self.timeframe)
        print(f"Spot CVD: {spot_cvd[:5]}")  # Print only the first 5 entries

        print(f"Fetching long-short ratio for {symbol}...")
        long_short_ratio = self.fetch_long_short_ratio(symbol, since)
        print(f"Long-Short Ratio: {long_short_ratio[:5]}")  # Print only the first 5 entries

        print(f"Calculating CVD for {symbol}...")
        cvd = self.fetch_cvd(symbol, since)
        print(f"CVD data: {cvd[:5]}")  # Print only the first 5 entries for brevity

    def fetch_latest(self, symbol):
        """Fetch and store the latest data for a single symbol."""
        collection = self.db["market_data"]

        # Find the latest timestamp for the symbol in the database
        latest_entry = collection.find_one(
            {"symbol": symbol},
            sort=[("ohlcv.timestamp", -1)]
        )
        since = latest_entry["ohlcv"]["timestamp"] if latest_entry else int(
            (datetime.datetime.utcnow() - datetime.timedelta(days=1)).timestamp() * 1000
        )

        print(f"Fetching data for {symbol} since {datetime.datetime.fromtimestamp(since / 1000)}...")

        # Fetch only the latest OHLCV data
        ohlcv = self.fetch_ohlcv(symbol, since)
        if not ohlcv:
            print(f"No new OHLCV data for {symbol}.")
            return

        # Fetch other data points
        spot_cvd = self.get_spot_cvd(symbol[:-5], since, period=self.timeframe)
        cvd = self.fetch_cvd(symbol, since)
        long_short_ratio = self.fetch_long_short_ratio(symbol, since)
        funding_rate = self.fetch_funding_rate(symbol, since)

        # Prepare and store the data
        data = {
            "symbol": symbol,
            "exchange": self.exchange.id,
            "ohlcv": ohlcv,
            "spot_cvd": spot_cvd,
            "cvd": cvd,
            "long_short_ratio": long_short_ratio,
            "funding_rate": funding_rate,
            "timestamp": datetime.datetime.utcnow(),
        }
        collection.insert_one(data)
        print(f"Stored data for {symbol}.")

    def plot_data(self, symbol, ohlcv, cvd, spot_cvd, long_short_ratio, funding_rate):
        """Plot OHLCV, CVD, Spot CVD, and Long-Short Ratio."""
        if not ohlcv:
            print("No OHLCV data available for plotting.")
            return

        # Convert timestamps
        ohlcv_timestamps = [datetime.datetime.fromtimestamp(x["timestamp"] / 1000) for x in ohlcv]
        cvd_timestamps = [datetime.datetime.fromtimestamp(x["timestamp"] / 1000) for x in cvd]
        spot_cvd_timestamps = [datetime.datetime.fromtimestamp(x["timestamp"] / 1000) for x in spot_cvd]
        long_short_timestamps = [datetime.datetime.fromtimestamp(x["timestamp"] / 1000) for x in long_short_ratio]
        fundings_timestamps = [datetime.datetime.fromtimestamp(x["timestamp"] / 1000) for x in funding_rate]

        # Extract OHLC data
        closes = [x["close"] for x in ohlcv]  # Closing prices

        # Extract CVD data
        cvd_values = [x["cvd"] for x in cvd]

        # Extract Spot CVD data
        spot_cvd_values = [x["spot_cvd"] for x in spot_cvd]

        # Extract Long-Short Ratio data
        long_short_values = [x["long_short_ratio"] for x in long_short_ratio]

        # Extract Funding Rate data
        fundings_values = [x["funding_rate"] for x in funding_rate]
        # Plot the data
        plt.figure(figsize=(14, 10))

        # OHLCV plot
        plt.subplot(5, 1, 1)
        plt.plot(ohlcv_timestamps, closes, label="Close Price", color="blue")
        plt.title(f"{symbol} OHLCV ({self.timeframe})")
        plt.xlabel("Time")
        plt.ylabel("Price")
        plt.legend()

        # CVD plot
        plt.subplot(5, 1, 2)
        plt.plot(cvd_timestamps, cvd_values, label="CVD", color="green")
        plt.title("Cumulative Volume Delta (CVD)")
        plt.xlabel("Time")
        plt.ylabel("CVD")
        plt.legend()

        # Spot CVD plot
        plt.subplot(5, 1, 3)
        plt.plot(spot_cvd_timestamps, spot_cvd_values, label="Spot CVD", color="red")
        plt.title("Spot CVD")
        plt.xlabel("Time")
        plt.ylabel("Spot CVD")
        plt.legend()

        # Long-Short Ratio plot
        plt.subplot(5, 1, 4)
        plt.plot(long_short_timestamps, long_short_values, label="Long-Short Ratio", color="orange")
        plt.title("Long-Short Ratio")
        plt.xlabel("Time")
        plt.ylabel("Ratio")
        plt.legend()

        # Funding plot
        plt.subplot(5, 1, 5)
        plt.plot(fundings_timestamps, fundings_values, label="Long-Short Ratio", color="orange")
        plt.title("Fundings")
        plt.xlabel("Time")
        plt.ylabel("Fundings")
        plt.legend()

        plt.tight_layout()
        plt.show()

    def plot_last_day(self, symbol):
        """Fetch and plot data for the last 1 day."""
        since = int((datetime.datetime.utcnow() - datetime.timedelta(days=1)).timestamp() * 1000)

        print(f"Fetching OHLCV data for {symbol}...")
        ohlcv = self.fetch_ohlcv(symbol, since)

        print(f"Fetching spot CVD for {symbol}...")
        spot_cvd = self.get_spot_cvd(symbol[:-5], since, period=self.timeframe)

        print(f"Fetching long-short ratio for {symbol}...")
        long_short_ratio = self.fetch_long_short_ratio(symbol, since)

        print(f"Calculating CVD for {symbol}...")
        cvd = self.fetch_cvd(symbol, since)

        print(f"Fetching fundings for {symbol}...")
        fundings = self.fetch_funding_rate(symbol, since)

        print("Plotting data...")
        self.plot_data(symbol, ohlcv, cvd, spot_cvd, long_short_ratio, fundings)

    def run(self, symbols):
        """Fetch and store data for multiple symbols every 5 minutes."""
        def fetch_for_all_symbols():
            for symbol in symbols:
                try:
                    self.fetch_and_store(symbol)
                except Exception as e:
                    print(f"Error processing {symbol}: {e}")

        # Schedule the task every 5 minutes
        schedule.every(1).minutes.do(fetch_for_all_symbols)

        print("Starting periodic fetching...")
        while True:
            schedule.run_pending()
            time.sleep(5)  # Sleep to prevent high CPU usage




if __name__ == "__main__":
    fetcher = DataFetcher("binance", timeframe="5m")
    # Test Usage
    fetcher.clean_db()
    #symbols = list(load_perpetual_markets_for_binance().keys())
    #fetcher.run(symbols)
    # Plot data for the last day
    fetcher.run(["BTC/USDT:USDT", "ETH/USDT:USDT"])
    # fetcher.plot_last_day("BTC/USDT:USDT")
    # Uncomment the following to test fetching or process multiple symbols
    # fetcher.test_fetch("BTC/USDT")
    #fetcher.run(["BTC/USDT", "ETH/USDT"])

# %%
