import asyncio
import datetime
from pymongo import MongoClient
import logging


class Backtester:
    def __init__(self, strategy_class, db_uri, db_name, symbol, timeframe, start_time, end_time):
        self.strategy_class = strategy_class
        self.db_uri = db_uri
        self.db_name = db_name
        self.symbol = symbol
        self.timeframe = timeframe
        self.start_time = start_time
        self.end_time = end_time

        # Logger setup
        self.logger = logging.getLogger("Backtester")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    async def run(self):
        """Run the backtest."""
        self.logger.info("Starting backtest...")
        client = MongoClient(self.db_uri)
        db = client[self.db_name]
        data_collection = db["market_data"]

        # Fetch historical data for the given range
        historical_data = data_collection.find(
            {
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "timestamp": {"$gte": self.start_time, "$lt": self.end_time},
            }
        ).sort("timestamp", 1)

        data = list(historical_data)
        if not data:
            self.logger.error("No data found for the given range.")
            return

        # Initialize the strategy
        strategy = self.strategy_class(
            strategy_name="CVD_Backtest",
            db_uri=self.db_uri,
            db_name=self.db_name,
            is_backtest=True
        )

        try:
            for entry in data:
                strategy.current_time = entry["timestamp"]
                strategy.data_collection = db["market_data"]

                # Simulate trading logic for the strategy
                await strategy.trade_symbol(self.symbol, self.timeframe)

                # Update unrealized PnL
                strategy.update_pnl(self.symbol, entry["close"])

                # Log strategy state
                self.logger.info(
                    f"Time: {entry['timestamp']} | Symbol: {self.symbol} | Close: {entry['close']} | "
                    f"Unrealized PnL: {strategy.unrealized_pnl.get(self.symbol, 0.0):.2f} | "
                    f"Realized PnL: {strategy.realized_pnl.get(self.symbol, 0.0):.2f}"
                )

            # Final report
            self.logger.info("Backtest complete!")
            self.logger.info(f"Final Realized PnL: {strategy.realized_pnl}")
            self.logger.info(f"Final Unrealized PnL: {strategy.unrealized_pnl}")

        except Exception as e:
            self.logger.error(f"Error during backtest: {e}")
        finally:
            await strategy.stop()


# Backtest Configuration
if __name__ == "__main__":
    db_uri = "mongodb://localhost:27017/"
    db_name = "trading"
    symbol = "BTC/USDT"
    timeframe = "1m"

    # Set the backtest range (example: last 1 hour)
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(hours=1)

    backtester = Backtester(
        strategy_class=CVDStrategy,
        db_uri=db_uri,
        db_name=db_name,
        symbol=symbol,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time,
    )

    asyncio.run(backtester.run())
