import uvicorn
import os
from importlib import import_module


def main():
    # Read the controller type from the environment variable
    controller_type = os.getenv("CONTROLLER_TYPE", "StrategyController")

    # Dynamically import the appropriate app interface class
    try:
        module_name = f"util.{controller_type.lower()}_app_interface"
        class_name = f"{controller_type}AppInterface"

        module = import_module(module_name)
        app_interface_class = getattr(module, class_name)

        # Initialize the app interface
        app_interface = app_interface_class()

        # Start the heartbeat in a separate thread
        app_interface.controller.start_heartbeat()

        # Start the FastAPI app
        uvicorn.run(app_interface.app, host="0.0.0.0", port=8000)
    except (ModuleNotFoundError, AttributeError) as e:
        print(f"Error: {e}. Check that the CONTROLLER_TYPE environment variable is set correctly.")
        exit(1)


if __name__ == "__main__":
    main()
