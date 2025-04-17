# %%
import pymongo
import pandas as pd
import matplotlib.pyplot as plt

# MongoDB connection
client = pymongo.MongoClient("mongodb://pj:yolo1234@localhost:27017/")
db = client["TradingData"]
# collection = db["market_data_5m"]  # Update this with actual collection name

# Fetch historical data from MongoDB
def fetch_data(symbol, exchange, limit=15000):
    collection = db[f"{symbol}_5m"]  # Update this with actual collection name
    cursor = collection.find({"symbol": symbol, "exchange": exchange}).sort("timestamp", 1).limit(limit)
    df = pd.DataFrame(list(cursor))
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    return df

# Backtesting function
def backtest_sma(df, short_window=10, long_window=50, stop_loss=0.02, take_profit=0.05):
    df["SMA_Short"] = df["ohlcv_close"].rolling(window=short_window).mean()
    df["SMA_Long"] = df["ohlcv_close"].rolling(window=long_window).mean()
    df.dropna(inplace=True)
    
    position = 0  # 1 for long, -1 for short, 0 for no position
    entry_price = 0
    pnl = []
    cumulative_pnl = []
    equity = 10000  # Starting equity
    
    for index, row in df.iterrows():
        if position == 0:
            if row["SMA_Short"] > row["SMA_Long"]:
                position = 1  # Enter long
                entry_price = row["ohlcv_close"]
            elif row["SMA_Short"] < row["SMA_Long"]:
                position = -1  # Enter short
                entry_price = row["ohlcv_close"]
        
        elif position == 1:  # Long position
            if row["ohlcv_close"] <= entry_price * (1 - stop_loss) or row["ohlcv_close"] >= entry_price * (1 + take_profit):
                profit = row["ohlcv_close"] - entry_price
                pnl.append(profit)
                equity += profit
                position = 0
        
        elif position == -1:  # Short position
            if row["ohlcv_close"] >= entry_price * (1 + stop_loss) or row["ohlcv_close"] <= entry_price * (1 - take_profit):
                profit = entry_price - row["ohlcv_close"]
                pnl.append(profit)
                equity += profit
                position = 0
        
        cumulative_pnl.append(sum(pnl))
    
    # Plot Cumulative PnL
    plt.plot(cumulative_pnl, label="Cumulative PnL")
    plt.xlabel("Trades")
    plt.ylabel("Profit/Loss")
    plt.title("Backtest Results")
    plt.legend()
    plt.show()

# Run backtest
data = fetch_data("BTCUSDT", "binance")
backtest_sma(data)
# %%
