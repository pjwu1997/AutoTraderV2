import asyncio
import datetime
import logging
from pymongo import MongoClient
import ccxt


class BaseStrategy:
    def __init__(self, strategy_name, db_uri="mongodb://localhost:27017/", db_name="trading", is_backtest=False, exchange_config=None):
        self.name = strategy_name
        self.running = True
        self.tasks = {}
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.state_collection = self.db["strategy_state"]
        self.data_collection = self.db["market_data"]  # OHLCV and CVD stored here
        self.orders =  {}  # Track orders by symbol
        self.realized_pnl = {}  # Realized PnL by symbol
        self.unrealized_pnl = {}  # Unrealized PnL by symbol
        self.is_backtest = is_backtest
        self.current_time = None  # Current time during backtest

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

    async def run(self):
        """Main loop for the strategy."""
        self.logger.info("Starting strategy...")
        try:
            await self.start_task("monitoring", self.monitoring)
            await self.start_task("trading", self.trading)
            await self.start_task("pnl_updater", self.pnl_updater)
            await self.start_task("status_update", self.status_update)

            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.logger.info("Terminating strategy...")
        finally:
            await self.cleanup()
            self.logger.info("Strategy stopped.")

    async def start_task(self, task_name, task_coro):
        """Start a specific task."""
        if task_name in self.tasks:
            self.logger.warning(f"Task {task_name} is already running.")
        else:
            self.tasks[task_name] = asyncio.create_task(task_coro())
            self.logger.info(f"Task {task_name} started.")

    async def stop_task(self, task_name):
        """Stop a specific task."""
        if task_name in self.tasks:
            self.tasks[task_name].cancel()
            try:
                await self.tasks[task_name]
            except asyncio.CancelledError:
                pass
            del self.tasks[task_name]
            self.logger.info(f"Task {task_name} stopped.")

    async def pnl_updater(self):
        """Update PnL for open orders at regular intervals."""
        while self.running:
            for symbol in self.orders.keys():
                latest_data = self.data_collection.find_one(
                    {"symbol": symbol},
                    sort=[("timestamp", -1)]
                )
                if latest_data:
                    self.update_pnl(symbol, latest_data["close"])
            await self.register_status_to_db()
            await asyncio.sleep(10)

    async def monitoring(self):
        """Define monitoring logic here."""
        while self.running:
            await asyncio.sleep(2)
            self.logger.info("Monitoring...")

    async def trading(self):
        """Define trading logic here."""
        while self.running:
            await asyncio.sleep(3)
            self.logger.info("Trading...")

    async def status_update(self):
        """Send periodic status updates."""
        while self.running:
            await asyncio.sleep(5)
            await self.register_status_to_db()
            self.logger.info("Status updated.")

    async def cleanup(self):
        """Cleanup tasks when the strategy stops."""
        for task_name in list(self.tasks.keys()):
            await self.stop_task(task_name)

    async def stop(self):
        """Stop the strategy."""
        self.running = False
        await self.cleanup()

    async def register_status_to_db(self):
        """Register the strategy's current status to the database."""
        state = {
            "strategy_name": self.name,
            "timestamp": datetime.datetime.utcnow(),
            "running": self.running,
            "orders": self.orders,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
        }
        self.state_collection.update_one(
            {"strategy_name": self.name},
            {"$set": state},
            upsert=True
        )
        self.logger.info("Status registered to DB.")

    def place_order(self, symbol, side, amount, price=None):
        """Place a real or mock order."""
        if self.is_backtest:
            self.mock_place_order(symbol, side, amount, price)
        else:
            order = self.exchange.create_order(symbol, "limit", side, amount, price)
            self.logger.info(f"Real order placed for {symbol}: {order}")
            return order

    def mock_place_order(self, symbol, side, amount, price):
        """Place a mock order."""
        if symbol not in self.orders:
            self.orders[symbol] = []

        order = {
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "price": price,
            "timestamp": self.current_time or datetime.datetime.utcnow(),
            "status": "open",
        }
        self.orders[symbol].append(order)
        self.logger.info(f"Mock order placed: {order}")
        asyncio.create_task(self.register_status_to_db())

    def update_pnl(self, symbol, current_price):
        """Calculate the PnL for a specific symbol."""
        if symbol not in self.orders:
            return

        self.unrealized_pnl[symbol] = 0.0  # Reset unrealized PnL
        self.realized_pnl.setdefault(symbol, 0.0)

        for order in self.orders[symbol]:
            if order["status"] == "open":
                if order["side"] == "buy":
                    self.unrealized_pnl[symbol] += (current_price - order["price"]) * order["amount"]
                elif order["side"] == "sell":
                    self.unrealized_pnl[symbol] += (order["price"] - current_price) * order["amount"]

    def close_order(self, order, price):
        """Close an open order and update realized PnL."""
        symbol = order["symbol"]
        self.realized_pnl.setdefault(symbol, 0.0)

        try:
            if order["side"] == "buy":
                self.realized_pnl[symbol] += (price - order["price"]) * order["amount"]
            elif order["side"] == "sell":
                self.realized_pnl[symbol] += (order["price"] - price) * order["amount"]

            order["status"] = "closed"
            self.logger.info(f"Order closed: {order}")
            asyncio.create_task(self.register_status_to_db())
        except Exception as e:
            self.logger.error(f"Error closing order: {order}. Error: {e}")

    @classmethod
    async def start_from_db(cls, db_uri, strategy_name):
        """Resume strategy based on state from the database."""
        logger = logging.getLogger(strategy_name)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.info(f"Attempting to resume strategy: {strategy_name} from DB.")
        try:
            client = MongoClient(db_uri)
            db = client["trading"]
            state_collection = db["strategy_state"]

            state = state_collection.find_one({"strategy_name": strategy_name})
            if not state:
                logger.warning(f"No saved state found for strategy: {strategy_name}")
                return None

            strategy = cls(strategy_name, db_uri=db_uri, is_backtest=False)
            strategy.running = state["running"]
            strategy.orders = state["orders"]
            strategy.realized_pnl = state["realized_pnl"]
            strategy.unrealized_pnl = state["unrealized_pnl"]

            logger.info(f"Strategy {strategy_name} successfully resumed from DB.")
            await strategy.run()
            return strategy
        except Exception as e:
            logger.error(f"Error resuming strategy {strategy_name} from DB: {e}")
            return None


class CVDStrategy(BaseStrategy):
    async def trading(self):
        """Custom trading logic for the CVD strategy."""
        symbols = ["BTC/USDT", "ETH/USDT"]
        timeframe = "1m"

        tasks = []
        for symbol in symbols:
            tasks.append(self.trade_symbol(symbol, timeframe))

        await asyncio.gather(*tasks)

    async def trade_symbol(self, symbol, timeframe):
        """Trade logic for a specific symbol."""
        latest_data = self.data_collection.find_one(
            {"symbol": symbol, "timeframe": timeframe},
            sort=[("timestamp", -1)]
        )

        if not latest_data:
            return

        self.current_time = latest_data["timestamp"]
        close_price = latest_data["close"]
        cvd = latest_data["cvd"]

        if cvd > 0:
            self.place_order(symbol, "buy", amount=0.01, price=close_price)
        elif cvd < 0:
            self.place_order(symbol, "sell", amount=0.01, price=close_price)
