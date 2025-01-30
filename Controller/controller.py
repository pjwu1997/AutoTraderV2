import asyncio
from base_controller import BaseController

class StrategyController(BaseController):
    async def run(self):
        """Main loop to manage strategies."""
        strategies = self.load_strategies()
        for strategy in strategies:
            self.start_strategy(
                strategy_name=strategy["name"],
                symbol=strategy["symbol"],
                api_key=strategy["api_key"],
                api_secret=strategy["api_secret"],
            )

        while True:
            await asyncio.sleep(10)  # Monitor running strategies or add new logic here.

if __name__ == "__main__":
    controller = StrategyController()
    asyncio.run(controller.run())
import asyncio
from base_controller import BaseController
