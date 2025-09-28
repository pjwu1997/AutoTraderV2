FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY Common/ Common/
COPY SlaveVM/data_fetcher/ .

# Create logs directory
RUN mkdir -p /app/logs

ENV PYTHONPATH="/app"

# Command to run the application
CMD ["python3", "unified_collector.py"]