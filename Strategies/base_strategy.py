# %%
import asyncio
import datetime
import logging
import matplotlib.pyplot as plt
from pymongo import MongoClient
import ccxt
# Fetch historical data from MongoDB
def fetch_data(symbol, exchange, limit=15000):
    collection = db[f"{symbol}_5m"]  # Update this with actual collection name
    cursor = collection.find({"symbol": symbol, "exchange": exchange}).sort("timestamp", 1).limit(limit)
    df = pd.DataFrame(list(cursor))
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    return df

class BaseStrategy:
    def __init__(self, strategy_name, db_uri="mongodb://pj:yolo1234localhost:27017/", db_name="trading", is_backtest=False, exchange_config=None):
        self.name = strategy_name
        self.running = True
        self.tasks = {}
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.state_collection = self.db["strategy_state"]
        self.data_collection = self.db["TradingData"]  # OHLCV and CVD stored here
        self.orders =  {}  # Track orders by symbol
        self.realized_pnl = {}  # Realized PnL by symbol
        self.unrealized_pnl = {}  # Unrealized PnL by symbol
        self.is_backtest = is_backtest
        self.current_time = None  # Current time during backtest
        self.backtest_data = []  # Stores prepared data for backtest
        self.cumulative_pnl = []  # Stores cumulative PnL
        self.price_history = []  # Stores price history

        # Logger setup
        self.logger = logging.getLogger(strategy_name)
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # CCXT integration
        if exchange_config and not is_backtest:
            self.exchange = getattr(ccxt, exchange_config["exchange_id"])({
                "apiKey": exchange_config["api_key"],
                "secret": exchange_config["api_secret"],
                "enableRateLimit": True,
            })
        else:
            self.exchange = None

    def prepare_data(self, symbol, start_time, end_time):
        """Prepare data needed for backtest."""
        self.logger.info("Preparing data for backtest...")
        self.backtest_data = list(self.data_collection[f'{symbol}_5m'].find({
            "timestamp": {"$gte": start_time, "$lte": end_time}
        }).sort("timestamp", 1))
        
    def backtest(self, symbol, start_time, end_time):
        """Run backtest over the given time range."""
        self.prepare_data(start_time, end_time)
        cumulative_pnl = 0.0
        
        for data in self.backtest_data:
            self.current_time = data["timestamp"]
            self.update_pnl(data["ohlcv_close"])
            self.calculate_action(data)
            cumulative_pnl += sum(self.realized_pnl.values())
            self.cumulative_pnl.append(cumulative_pnl)
            self.price_history.append(data["ohlcv_close"])
        
        self.plot_results()
    
    def plot_results(self):
        """Plot backtest results including price action and cumulative PnL."""
        plt.figure(figsize=(10, 5))
        plt.subplot(2, 1, 1)
        plt.plot(self.price_history, label="Price")
        plt.title("Price Action")
        plt.legend()
        
        plt.subplot(2, 1, 2)
        plt.plot(self.cumulative_pnl, label="Cumulative PnL", color='g')
        plt.title("Cumulative PnL")
        plt.legend()
        
        plt.tight_layout()
        plt.show()

    def calculate_action(self, data):
        """Determine action based on strategy logic."""
        pass  # Implement strategy-specific logic

    def update_pnl(self, current_price):
        """Calculate the PnL for a specific symbol."""
        for symbol in self.orders.keys():
            self.unrealized_pnl[symbol] = 0.0  # Reset unrealized PnL
            self.realized_pnl.setdefault(symbol, 0.0)

            for order in self.orders[symbol]:
                if order["status"] == "open":
                    if order["side"] == "buy":
                        self.unrealized_pnl[symbol] += (current_price - order["price"]) * order["amount"]
                    elif order["side"] == "sell":
                        self.unrealized_pnl[symbol] += (order["price"] - current_price) * order["amount"]

class CVDStrategy(BaseStrategy):
    def calculate_action(self, data):
        """Custom trading logic for the CVD strategy."""
        symbol = data["symbol"]
        close_price = data["ohlcv_close"]
        cvd = data["future_cvd_value"]

        if cvd > 0:
            self.place_order(symbol, "buy", amount=0.01, price=close_price)
        elif cvd < 0:
            self.place_order(symbol, "sell", amount=0.01, price=close_price)

    def place_order(self, symbol, side, amount, price):
        """Place a mock order for backtest and store in DB."""
        order = {
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "price": price,
            "timestamp": datetime.utcnow(),
            "status": "open",
        }
        result = self.db.orders.insert_one(order)
        self.logger.info(f"Mock order placed: {order}")
        return result.inserted_id  # Return order ID
    def place_order(self, symbol, side, amount, price, real=True):
        """Place a real order using Binance and store in DB."""
        try:
            if real:
                # Place order with Binance
                order = self.binance_client.create_order(
                    symbol=symbol,
                    side=side.upper(),
                    type="LIMIT",
                    timeInForce="GTC",
                    quantity=amount,
                    price=str(price)  # Binance requires price as a string
                )

            order_data = {
                "symbol": symbol,
                "side": side,
                "amount": amount,
                "price": price,
                "timestamp": datetime.utcnow(),
                "status": "open",
            }
            if real:
                order_data["binance_order_id"] = order["orderId"]
            
            # Store order in MongoDB
            result = self.db.orders.insert_one(order_data)
            self.logger.info(f"Real order placed: {order_data}")

            return result.inserted_id, order["orderId"]  # Return MongoDB ID & Binance order ID

        except Exception as e:
            self.logger.error(f"Error placing order: {e}")
            return None, None

    def add_stop_loss_order(self, symbol, original_order_id, stop_price):
        """Add a stop-loss order linked to the original order."""
        stop_loss_order = {
            "symbol": symbol,
            "side": "sell" if self.db.orders.find_one({"_id": original_order_id})["side"] == "buy" else "buy",
            "amount": self.db.orders.find_one({"_id": original_order_id})["amount"],
            "price": stop_price,
            "timestamp": datetime.utcnow(),
            "status": "open",
            "parent_order_id": original_order_id,  # Foreign key to original order
        }
        self.db.orders.insert_one(stop_loss_order)
        self.logger.info(f"Stop-loss order placed: {stop_loss_order}")
    
    def check_order_status(self):
        """Check and update order status using Binance API."""
        open_orders = self.db.orders.find({"status": "open"})
        for order in open_orders:
            binance_order = self.binance_client.get_all_orders(symbol=order["symbol"], limit=10)
            for bo in binance_order:
                if bo["price"] == str(order["price"]) and bo["status"] == "FILLED":
                    self.db.orders.update_one({"_id": order["_id"]}, {"$set": {"status": "filled"}})
                    self.logger.info(f"Order filled: {order}")

if __name__ == '__main__':
    # strategy = CVDStrategy()
    # Define backtest parameters
    start_time = datetime.datetime(2024, 1, 1)
    end_time = datetime.datetime(2024, 1, 31)

    # Initialize strategy
    strategy = CVDStrategy("CVD_Backtest", is_backtest=True)

    # Run backtest
    strategy.backtest('BTCUSDT', start_time, end_time)
# %%
