import uuid
from pymongo import MongoClient
from docker import from_env
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime
from fastapi import HTTPException


class BaseController:
    def __init__(self, db_uri="mongodb://localhost:27017/", db_name="trading"):
        self.client = MongoClient(db_uri)
        self.db = self.client[db_name]
        self.strategies = self._load_active_strategies()
        self.docker_client = from_env()
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def _load_active_strategies(self):
        strategies = {}
        for strategy in self.db.strategies.find({"status": "running"}):
            try:
                container = self.docker_client.containers.get(strategy["container_id"])
                strategies[strategy["strategy_id"]] = container
            except Exception:
                pass
        return strategies

    def register_strategy(self, strategy_name, image, symbol, exchange_name, api_key, api_secret):
        strategy_id = str(uuid.uuid4())
        container_name = f"strategy_{strategy_id}"
        try:
            container = self.docker_client.containers.run(
                image=image,
                name=container_name,
                detach=True,
                network="strategy_network",
                environment={
                    "STRATEGY_ID": strategy_id,
                    "SYMBOL": symbol,
                    "EXCHANGE_NAME": exchange_name,
                    "API_KEY": api_key,
                    "API_SECRET": api_secret,
                    "DB_URI": "mongodb://mongo:27017/",
                    "DB_NAME": "trading",
                },
            )
            self.strategies[strategy_id] = container
            self.db.strategies.insert_one(
                {"strategy_id": strategy_id, "container_id": container.id, "status": "running"}
            )
            return {"strategy_id": strategy_id, "container_id": container.id}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to start strategy: {str(e)}")

    def stop_strategy(self, strategy_id):
        if strategy_id not in self.strategies:
            raise HTTPException(status_code=404, detail="Strategy not found.")
        try:
            container = self.strategies[strategy_id]
            container.stop()
            container.remove()
            self.db.strategies.update_one(
                {"strategy_id": strategy_id}, {"$set": {"status": "stopped"}}
            )
            del self.strategies[strategy_id]
            return {"status": f"Strategy {strategy_id} stopped and removed"}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to stop strategy: {str(e)}")

    def list_strategies(self):
        return [
            {"strategy_id": strategy_id, "status": container.status, "container_id": container.id}
            for strategy_id, container in self.strategies.items()
        ]

    def _update_heartbeat(self):
        print(f"Heartbeat updated at {datetime.now()}")

    def start_heartbeat(self):
        self.scheduler.add_job(
            self._update_heartbeat,
            IntervalTrigger(seconds=30),
            id="heartbeat_job",
            name="Update heartbeat",
            replace_existing=True,
        )
