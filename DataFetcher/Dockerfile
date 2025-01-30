# Base Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
COPY data_fetcher.py .

# Install dependencies
RUN pip install -r requirements.txt

# Command to run the fetcher
CMD ["python", "data_fetcher.py"]
