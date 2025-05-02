# Base Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the necessary Python scripts
COPY kline_websocket.py websocket_controller.py .

# Command to run the kline websocket script
CMD ["python", "kline_websocket.py"]