from pydantic import BaseModel
from fastapi import BackgroundTasks
from util.base_app_interface import BaseAppInterface
from util.strategy_controller import StrategyController


class RegisterStrategyRequest(BaseModel):
    symbol: str
    exchange_name: str
    api_key: str
    api_secret: str


class StrategyAppInterface(BaseAppInterface):
    def __init__(self):
        super().__init__(StrategyController)

    def _register_routes(self):
        @self.app.post("/register_strategy")
        def register_strategy(request: RegisterStrategyRequest, background_tasks: BackgroundTasks):
            def start_strategy():
                self.controller.register_strategy(
                    strategy_name="long_short_strategy",
                    image="strategy_image",
                    symbol=request.symbol,
                    exchange_name=request.exchange_name,
                    api_key=request.api_key,
                    api_secret=request.api_secret,
                )
            background_tasks.add_task(start_strategy)
            return {"message": "Strategy is being registered."}

        @self.app.post("/stop_strategy")
        def stop_strategy(strategy_id: str):
            return self.controller.stop_strategy(strategy_id)

        @self.app.get("/list_strategies")
        def list_strategies():
            return self.controller.list_strategies()
