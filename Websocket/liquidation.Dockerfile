# Base Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the necessary Python scripts
COPY liquidation_websocket.py websocket_controller.py .

# Command to run the liquidation websocket script
CMD ["python", "liquidation_websocket.py"]