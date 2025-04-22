# Base Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy files
COPY requirements.txt .
# Install dependencies
RUN pip install -r requirements.txt

# Copy the Python script
COPY multisymboal_data.py .

# Set default environment variables
ENV MONGODB_URI=mongodb://localhost:27017/
ENV MONGODB_DB=multikline_poc
ENV BINANCE_API_KEY=H95sApwsCkDIUiBxicExq8eVgJIdUsGm7p9mraNwcqNGW2RS6Ryx89TcKZSlV8an
ENV BINANCE_API_SECRET=HsQH0Snzaw8LnmhKeWHbEfrPRmrAcUAjgqmR4Ltv1zA6JqjaZfW289Gb8CoUFMBF
ENV SYMBOLS=BTCUSDT,ETHUSDT,BNBUSDT,ADAUSDT,BIGTIMEUSDT,DOGEUSDT,DOTUSDT,SOLUSDT,VINEUSDT,FARTCOINUSDT,ARKUSDT,ALCHUSDT
ENV FETCH_INTERVAL=60

# Command to run the fetcher
CMD ["python", "multisymboal_data.py"]