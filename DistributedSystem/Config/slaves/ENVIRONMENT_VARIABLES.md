# Slave Environment Variables Configuration

## Required Environment Variables for 1-Minute Precision WebSocket System

### Core Configuration
```bash
# Slave Identity
export SLAVE_ID="slave-001"  # Unique identifier for this slave

# Master VM Connection
export MASTER_URL="http://master-vm:8080"
export MASTER_VM_IP="10.0.1.100"

# MongoDB Connection
export MONGODB_CONNECTION_STRING="mongodb://master-vm:27017"
export DATABASE_NAME="trading_data"
export COLLECTION_NAME="market_data"
```

### Data Collection Precision
```bash
# Time Precision Settings (CRITICAL: Changed from 5m to 1m)
export TIMEFRAME="1m"                    # REST API collection interval
export KLINE_INTERVAL="1m"               # WebSocket K-line interval
export DATA_PRECISION="1m"               # Overall system precision
```

### WebSocket Configuration
```bash
# Binance WebSocket URLs
export KLINE_SPOT_WS_URL="wss://stream.binance.com:9443/ws/{streams}"
export KLINE_FUTURES_WS_URL="wss://fstream.binance.com/ws/{streams}"
export LIQUIDATION_WS_URL="wss://fstream.binance.com/ws/!forceOrder@arr"

# WebSocket Settings
export WEBSOCKET_PING_INTERVAL="30"
export WEBSOCKET_PING_TIMEOUT="10"
export WEBSOCKET_RECONNECT_DELAY="5"
export WEBSOCKET_MAX_RECONNECT_ATTEMPTS="10"
```

### Liquidation Service Configuration
```bash
# Liquidation Data Management
export LIQUIDATION_CLEANUP_MINUTES="5"      # Clean old liquidation data every 5 minutes
export LIQUIDATION_RECONNECT_INTERVAL="86100"  # Reconnect every 24 hours (86400-300)
export LIQUIDATION_AGGREGATION_WINDOW="60"  # 1-minute aggregation window
```

### Service Ports
```bash
# Service Port Configuration
export DATA_FETCHER_PORT="8081"         # REST API data fetcher
export KLINE_WEBSOCKET_PORT="8082"      # K-line WebSocket service
export LIQUIDATION_WEBSOCKET_PORT="8083" # Liquidation WebSocket service
export HEALTH_CHECKER_PORT="8084"       # Health monitoring service
```

### API Limits and Throttling
```bash
# Binance API Rate Limits
export API_REQUEST_LIMIT="1200"         # Requests per minute per IP
export API_REQUEST_DELAY="0.1"          # Delay between requests (seconds)
export MAX_CONCURRENT_REQUESTS="5"      # Maximum concurrent API calls
```

### Logging Configuration
```bash
# Logging Settings
export LOG_LEVEL="INFO"                 # DEBUG, INFO, WARNING, ERROR
export LOG_ROTATION_SIZE="50MB"         # Log file rotation size
export LOG_RETENTION_DAYS="7"           # Keep logs for 7 days
export LOG_FORMAT="json"                # Log format: json or plain
```

### Data Collection Scope
```bash
# Symbol Assignment (set by Master VM automatically)
export SYMBOLS="BTCUSDT,ETHUSDT,BNBUSDT,..."  # Assigned symbols list
export MAX_SYMBOLS_PER_SLAVE="100"     # Maximum symbols per slave

# Data Types to Collect
export COLLECT_OHLCV="true"             # 1m OHLCV data
export COLLECT_CVD="true"               # Cumulative volume delta
export COLLECT_FUNDING_RATES="true"     # Current + next funding rates  
export COLLECT_LONG_SHORT_RATIOS="true" # All 4 types of L/S ratios
export COLLECT_OPEN_INTEREST="true"     # Open interest data
export COLLECT_LIQUIDATIONS="true"      # Real-time liquidation events
```

### Health Monitoring
```bash
# Health Check Configuration
export HEALTH_CHECK_INTERVAL="30"       # Health check every 30 seconds
export HEALTH_CHECK_TIMEOUT="10"        # Health check timeout
export HEARTBEAT_INTERVAL="60"          # Send heartbeat to master every minute
```

## Usage Instructions

### 1. Create Slave-Specific Environment File
```bash
# Copy template for each slave
cp ENVIRONMENT_VARIABLES.md ../slaves/slave-1.env
cp ENVIRONMENT_VARIABLES.md ../slaves/slave-2.env
# ... etc
```

### 2. Customize Each Slave Configuration
```bash
# Edit slave-1.env
export SLAVE_ID="slave-001"
export SYMBOLS="BTCUSDT,ETHUSDT,BNBUSDT,ADAUSDT,DOTUSDT,..."

# Edit slave-2.env  
export SLAVE_ID="slave-002"
export SYMBOLS="XRPUSDT,SOLUSDT,LTCUSDT,LINKUSDT,AVAXUSDT,..."
```

### 3. Load Environment in Docker Compose
```yaml
# docker-compose.slave.yml
services:
  data-fetcher:
    env_file:
      - ../../Config/slaves/slave-1.env
    environment:
      - TIMEFRAME=1m
      - KLINE_INTERVAL=1m
      # ... additional overrides

  kline-websocket:
    env_file:
      - ../../Config/slaves/slave-1.env
    environment:
      - KLINE_INTERVAL=1m
      - WEBSOCKET_PING_INTERVAL=30
      
  liquidation-websocket:
    env_file:
      - ../../Config/slaves/slave-1.env
    environment:
      - LIQUIDATION_WS_URL=wss://fstream.binance.com/ws/!forceOrder@arr
```

## Critical Notes

⚠️ **Data Precision**: System now uses **1-minute precision** instead of 5-minute
⚠️ **WebSocket Services**: Enable all 4 containers (not just 2) for complete data collection
⚠️ **Network Ports**: Ensure ports 8081-8084 are open in network security groups
⚠️ **API Limits**: Each slave VM must have independent IP to avoid Binance rate limits

## Validation

Test your environment configuration:
```bash
# Verify all required variables are set
python3 -c "
import os
required_vars = ['SLAVE_ID', 'TIMEFRAME', 'KLINE_INTERVAL', 'MASTER_URL']
for var in required_vars:
    print(f'{var}: {os.getenv(var, \"NOT SET\")}')
"

# Test WebSocket URLs
curl -I https://stream.binance.com:9443/ws/btcusdt@kline_1m
curl -I https://fstream.binance.com/ws/btcusdt@kline_1m
```