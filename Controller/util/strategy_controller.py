from util.base_controller.py import BaseController


class StrategyController(BaseController):
    def __init__(self, db_uri="mongodb://localhost:27017/", db_name="trading"):
        super().__init__(db_uri, db_name)
