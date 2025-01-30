from fastapi import FastAPI


class BaseAppInterface:
    def __init__(self, controller_class):
        self.app = FastAPI()
        self.controller = controller_class()
        self._register_routes()

    def _register_routes(self):
        raise NotImplementedError("Subclasses must implement _register_routes method.")
