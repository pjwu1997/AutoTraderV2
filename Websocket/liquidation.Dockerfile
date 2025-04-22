# Base Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
# Install dependencies
RUN pip install -r requirements.txt

COPY liquidation_websocket.py .

# Command to run the fetcher
CMD ["python", "liquidation_websocket.py"]
