#!/bin/bash
# Deploy Slave VM Application (Unified Data Collector)

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Check arguments
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <slave_ip> <slave_id>"
    echo "Example: $0 52.175.36.139 slave-1"
    exit 1
fi

SLAVE_IP="$1"
SLAVE_ID="$2"

echo -e "${GREEN}🚀 SLAVE VM APPLICATION DEPLOYMENT${NC}"
echo "==================================="
echo ""
echo -e "${BLUE}📋 Configuration:${NC}"
echo "   Slave IP: $SLAVE_IP"
echo "   Slave ID: $SLAVE_ID"
echo ""

# Load configuration
if [ ! -f "deployment.env" ]; then
    echo -e "${RED}❌ deployment.env not found!${NC}"
    exit 1
fi

source deployment.env

USERNAME="$VM_ADMIN_USERNAME"
PASSWORD="$VM_ADMIN_PASSWORD"
MASTER_IP="20.255.100.73"

# Check sshpass
if ! command -v sshpass &> /dev/null; then
    echo -e "${YELLOW}Installing sshpass...${NC}"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install hudochenkov/sshpass/sshpass
    fi
fi

# SSH command wrapper
ssh_exec() {
    local cmd="$1"
    sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$USERNAME@$SLAVE_IP" "$cmd"
}

# SCP file transfer wrapper
scp_file() {
    local local_file="$1"
    local remote_path="$2"
    sshpass -p "$PASSWORD" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$local_file" "$USERNAME@$SLAVE_IP:$remote_path"
}

echo -e "${BLUE}🔄 Step 1: System Setup & Updates${NC}"
echo "--------------------------------"

# Update system
ssh_exec "sudo apt-get update -y"
ssh_exec "sudo apt-get install -y python3 python3-pip git curl wget"

echo "✅ System updated"
echo ""

echo -e "${BLUE}🐍 Step 2: Python Environment Setup${NC}"
echo "-----------------------------------"

# Install Python packages
ssh_exec "pip3 install --upgrade pip"
ssh_exec "pip3 install pymongo ccxt requests python-dotenv asyncio websockets aiohttp"

echo "✅ Python packages installed"
echo ""

echo -e "${BLUE}📁 Step 3: Create Directory Structure${NC}"
echo "------------------------------------"

# Create application directories
ssh_exec "mkdir -p /home/$USERNAME/autotrader/{data_fetcher,logs,config}"

echo "✅ Directory structure created"
echo ""

echo -e "${BLUE}📄 Step 4: Deploy Unified Collector${NC}"
echo "-----------------------------------"

# Create the unified collector (using your tested version)
cat > temp_unified_collector.py << 'EOF'
#!/usr/bin/env python3
"""
Unified 1-Minute Aggregated Data Collector for Slave VMs
Collects and aggregates all trading data every minute
"""

import asyncio
import ccxt
import time
import json
import requests
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, List, Optional
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('unified_collector')

@dataclass
class CollectorConfig:
    """Configuration for the unified collector"""
    slave_id: str
    symbols: List[str]
    mongo_uri: str
    mongo_db_name: str
    timeframe: str = "1m"
    aggregation_interval: int = 60
    batch_size: int = 100

class MinuteAggregation:
    """1-minute data aggregation buffer"""
    
    def __init__(self, symbol: str, minute_start: int):
        self.symbol = symbol
        self.minute_start = minute_start
        self.minute_end = minute_start + 60000
        
        # Trade aggregation
        self.trades = []
        self.total_volume = 0.0
        self.trade_count = 0
        self.buy_volume = 0.0
        self.sell_volume = 0.0
        self.vwap_sum = 0.0
        self.vwap_volume = 0.0
        
        # Liquidation aggregation
        self.liquidations = []
        self.liquidation_count = 0
        self.liquidation_buy_volume = 0.0
        self.liquidation_sell_volume = 0.0
        self.total_liquidation_volume = 0.0
        
        # Orderbook snapshots
        self.orderbook_snapshots = []
        
        # Latest rates (updated every 10 seconds)
        self.latest_funding_rate = {}
        self.latest_long_short_ratios = {}
        self.latest_open_interest = {}
        self.latest_ticker = {}

class UnifiedCollector1M:
    """Unified 1-minute aggregated data collector"""
    
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.slave_id = config.slave_id
        
        # Initialize exchanges
        self.exchange = ccxt.binance({
            'sandbox': False,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
            'timeout': 30000
        })
        
        # 1-minute aggregation buffers
        self.minute_buffers: Dict[str, MinuteAggregation] = {}
        
        # MongoDB connection
        try:
            self.mongo_client = MongoClient(config.mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.mongo_client[config.mongo_db_name]
            # Test connection
            self.mongo_client.admin.command('ping')
            self.mongo_available = True
            logger.info("MongoDB connection established")
        except Exception as e:
            logger.warning(f"MongoDB not available: {e}")
            self.mongo_available = False
            self.mongo_client = None
    
    def get_current_minute_start(self) -> int:
        """Get the start timestamp of the current minute"""
        now = datetime.utcnow()
        minute_start = now.replace(second=0, microsecond=0)
        return int(minute_start.timestamp() * 1000)
    
    def get_or_create_minute_buffer(self, symbol: str) -> MinuteAggregation:
        """Get or create a minute aggregation buffer for a symbol"""
        current_minute = self.get_current_minute_start()
        
        # Check if we need a new buffer (new minute)
        if symbol not in self.minute_buffers or self.minute_buffers[symbol].minute_start != current_minute:
            self.minute_buffers[symbol] = MinuteAggregation(
                symbol=symbol,
                minute_start=current_minute
            )
        
        return self.minute_buffers[symbol]
    
    async def collect_real_time_data(self, symbol: str):
        """Continuously collect real-time data for 1-minute aggregation"""
        while True:
            try:
                buffer = self.get_or_create_minute_buffer(symbol)
                
                # Collect orderbook snapshot (every 5 seconds)
                orderbook = await self._fetch_orderbook_snapshot(symbol)
                if orderbook:
                    buffer.orderbook_snapshots.append({
                        'timestamp': int(time.time() * 1000),
                        'orderbook': orderbook
                    })
                
                # Collect recent trades for aggregation
                trades = await self._fetch_recent_trades_for_aggregation(symbol)
                for trade in trades:
                    self._aggregate_trade(buffer, trade)
                
                # Update latest rates (every 10 seconds)
                if len(buffer.orderbook_snapshots) % 2 == 0:  # Every 10 seconds
                    buffer.latest_funding_rate = await self._fetch_funding_rate(symbol)
                    buffer.latest_long_short_ratios = await self._fetch_long_short_ratios(symbol)
                    buffer.latest_open_interest = await self._fetch_open_interest(symbol)
                    buffer.latest_ticker = await self._fetch_ticker(symbol)
                
                await asyncio.sleep(5)  # Collect every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in real-time collection for {symbol}: {e}")
                await asyncio.sleep(5)
    
    def _aggregate_trade(self, buffer: MinuteAggregation, trade: Dict):
        """Aggregate a trade into the minute buffer"""
        amount = trade.get('amount', 0)
        price = trade.get('price', 0)
        side = trade.get('side', 'unknown')
        
        buffer.trades.append(trade)
        buffer.total_volume += amount
        buffer.trade_count += 1
        
        # Calculate VWAP components
        notional = amount * price
        buffer.vwap_sum += notional
        buffer.vwap_volume += amount
        
        # Aggregate buy/sell volumes
        if side == 'buy':
            buffer.buy_volume += amount
        elif side == 'sell':
            buffer.sell_volume += amount
    
    async def _fetch_orderbook_snapshot(self, symbol: str) -> Optional[Dict]:
        """Fetch orderbook snapshot"""
        try:
            orderbook = self.exchange.fetchOrderBook(symbol, limit=20)
            return {
                'bids': orderbook['bids'][:10],
                'asks': orderbook['asks'][:10], 
                'timestamp': orderbook['timestamp']
            }
        except Exception as e:
            logger.warning(f"Failed to fetch orderbook for {symbol}: {e}")
            return None
    
    async def _fetch_recent_trades_for_aggregation(self, symbol: str) -> List[Dict]:
        """Fetch recent trades for aggregation"""
        try:
            trades = self.exchange.fetchTrades(symbol, limit=50)
            # Filter trades from the last minute
            cutoff_time = int(time.time() * 1000) - 60000  # Last minute
            recent_trades = [t for t in trades if t['timestamp'] > cutoff_time]
            return recent_trades
        except Exception as e:
            logger.warning(f"Failed to fetch trades for {symbol}: {e}")
            return []
    
    async def _fetch_funding_rate(self, symbol: str) -> Dict:
        """Fetch current funding rate data"""
        try:
            current_funding = self.exchange.fetchFundingRate(symbol)
            
            # Enhanced funding data from Binance API
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            response = requests.get(url, params={"symbol": symbol_clean}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000,
                    "mark_price": float(data.get('markPrice', 0)),
                    "index_price": float(data.get('indexPrice', 0)),
                    "estimated_settle_price": float(data.get('estimatedSettlePrice', 0))
                }
            else:
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000
                }
                
        except Exception as e:
            logger.warning(f"Failed to fetch funding rate for {symbol}: {e}")
            return {}
    
    async def _fetch_long_short_ratios(self, symbol: str) -> Dict:
        """Fetch long/short ratio data"""
        try:
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            base_url = "https://fapi.binance.com"
            
            # Global account ratio (5m period - more reliable)
            url1 = f"{base_url}/futures/data/globalLongShortAccountRatio"
            response1 = requests.get(url1, params={"symbol": symbol_clean, "period": "5m", "limit": 1}, timeout=10)
            
            # Top trader account ratio (5m period)
            url2 = f"{base_url}/futures/data/topLongShortAccountRatio"
            response2 = requests.get(url2, params={"symbol": symbol_clean, "period": "5m", "limit": 1}, timeout=10)
            
            ratios = {}
            
            if response1.status_code == 200:
                data1 = response1.json()
                if data1:
                    ratios["global_account_ratio"] = data1[0]
            
            if response2.status_code == 200:
                data2 = response2.json()
                if data2:
                    ratios["top_trader_ratio"] = data2[0]
            
            return ratios
            
        except Exception as e:
            logger.warning(f"Failed to fetch long/short ratios for {symbol}: {e}")
            return {}
    
    async def _fetch_open_interest(self, symbol: str) -> Dict:
        """Fetch open interest data"""
        try:
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            url = "https://fapi.binance.com/fapi/v1/openInterest"
            response = requests.get(url, params={"symbol": symbol_clean}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "open_interest": float(data.get('openInterest', 0)),
                    "timestamp": int(data.get('time', time.time() * 1000))
                }
            
            return {}
            
        except Exception as e:
            logger.warning(f"Failed to fetch open interest for {symbol}: {e}")
            return {}
    
    async def _fetch_ticker(self, symbol: str) -> Dict:
        """Fetch 24h ticker data"""
        try:
            ticker = self.exchange.fetchTicker(symbol)
            return {
                "open": ticker.get('open', 0),
                "high": ticker.get('high', 0), 
                "low": ticker.get('low', 0),
                "close": ticker.get('close', 0),
                "volume": ticker.get('baseVolume', 0),
                "quote_volume": ticker.get('quoteVolume', 0),
                "change": ticker.get('change', 0),
                "percentage": ticker.get('percentage', 0),
                "vwap": ticker.get('vwap', 0)
            }
        except Exception as e:
            logger.warning(f"Failed to fetch ticker for {symbol}: {e}")
            return {}
    
    async def generate_1m_aggregated_data(self, symbol: str) -> Optional[Dict]:
        """Generate 1-minute aggregated data from buffer"""
        try:
            buffer = self.minute_buffers.get(symbol)
            if not buffer:
                return None
            
            # Calculate OHLCV from trades
            ohlcv = None
            if buffer.trades:
                prices = [t['price'] for t in buffer.trades]
                volumes = [t['amount'] for t in buffer.trades]
                timestamps = [t['timestamp'] for t in buffer.trades]
                
                ohlcv = [
                    min(timestamps),  # timestamp
                    prices[0],        # open (first price)
                    max(prices),      # high
                    min(prices),      # low
                    prices[-1],       # close (last price)
                    sum(volumes)      # volume
                ]
            
            # Calculate VWAP
            vwap = buffer.vwap_sum / buffer.vwap_volume if buffer.vwap_volume > 0 else 0
            
            # Calculate orderbook metrics
            orderbook_metrics = self._calculate_orderbook_metrics(buffer.orderbook_snapshots)
            
            # Get latest orderbook
            latest_orderbook = buffer.orderbook_snapshots[-1]['orderbook'] if buffer.orderbook_snapshots else {}
            
            # Enhanced metrics
            enhanced_metrics = {
                "cvd": buffer.buy_volume - buffer.sell_volume,  # Cumulative Volume Delta
                "buy_sell_ratio": buffer.buy_volume / buffer.sell_volume if buffer.sell_volume > 0 else 0,
                "vwap": vwap,
                "trade_count": buffer.trade_count,
                "total_volume": buffer.total_volume,
                "buy_volume": buffer.buy_volume,
                "sell_volume": buffer.sell_volume,
                "liquidation_count": buffer.liquidation_count,
                "liquidation_buy_volume": buffer.liquidation_buy_volume,
                "liquidation_sell_volume": buffer.liquidation_sell_volume,
                "total_liquidation_volume": buffer.total_liquidation_volume,
                **orderbook_metrics
            }
            
            # Compile final aggregated data
            aggregated_data = {
                "symbol": symbol,
                "timestamp": buffer.minute_start,
                "minute_end": buffer.minute_end,
                "slave_id": self.slave_id,
                "collection_type": "unified_market_data_1m",
                "ohlcv": ohlcv,
                "orderbook": latest_orderbook,
                "orderbook_metrics": orderbook_metrics,
                "trade_metrics": {
                    "count": buffer.trade_count,
                    "total_volume": buffer.total_volume,
                    "buy_volume": buffer.buy_volume,
                    "sell_volume": buffer.sell_volume,
                    "vwap": vwap,
                    "buy_sell_ratio": buffer.buy_volume / buffer.sell_volume if buffer.sell_volume > 0 else 0
                },
                "liquidation_metrics": {
                    "count": buffer.liquidation_count,
                    "buy_volume": buffer.liquidation_buy_volume,
                    "sell_volume": buffer.liquidation_sell_volume,
                    "total_volume": buffer.total_liquidation_volume
                },
                "funding_rate": buffer.latest_funding_rate,
                "long_short_ratios": buffer.latest_long_short_ratios,
                "ticker_24h": buffer.latest_ticker,
                "open_interest": buffer.latest_open_interest,
                "enhanced_metrics": enhanced_metrics
            }
            
            return aggregated_data
            
        except Exception as e:
            logger.error(f"Error generating aggregated data for {symbol}: {e}")
            return None
    
    def _calculate_orderbook_metrics(self, snapshots: List[Dict]) -> Dict:
        """Calculate orderbook depth and spread metrics"""
        if not snapshots:
            return {"avg_spread": 0, "min_spread": 0, "max_spread": 0, 
                   "avg_bid_depth": 0, "avg_ask_depth": 0, "snapshot_count": 0}
        
        spreads = []
        bid_depths = []
        ask_depths = []
        
        for snapshot in snapshots:
            ob = snapshot['orderbook']
            if ob.get('bids') and ob.get('asks'):
                # Calculate spread
                best_bid = ob['bids'][0][0] if ob['bids'] else 0
                best_ask = ob['asks'][0][0] if ob['asks'] else 0
                if best_bid > 0 and best_ask > 0:
                    spreads.append(best_ask - best_bid)
                
                # Calculate depths
                bid_depth = sum(bid[1] for bid in ob['bids'][:10])
                ask_depth = sum(ask[1] for ask in ob['asks'][:10])
                bid_depths.append(bid_depth)
                ask_depths.append(ask_depth)
        
        return {
            "avg_spread": sum(spreads) / len(spreads) if spreads else 0,
            "min_spread": min(spreads) if spreads else 0,
            "max_spread": max(spreads) if spreads else 0,
            "avg_bid_depth": sum(bid_depths) / len(bid_depths) if bid_depths else 0,
            "avg_ask_depth": sum(ask_depths) / len(ask_depths) if ask_depths else 0,
            "snapshot_count": len(snapshots)
        }
    
    async def store_aggregated_data(self, symbol: str, data: Dict):
        """Store 1-minute aggregated data to MongoDB"""
        if not self.mongo_available:
            logger.warning("MongoDB not available, skipping data storage")
            return
        
        try:
            # Create per-symbol collection
            symbol_clean = symbol.replace('/', '_').replace(':', '_')
            collection_name = f"market_data_1m_{symbol_clean}"
            collection = self.db[collection_name]
            
            # Insert data
            result = collection.insert_one(data)
            logger.info(f"Stored 1m data for {symbol}: {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"Error storing data for {symbol}: {e}")
    
    async def run_continuous_collection(self):
        """Run continuous 1-minute aggregated collection"""
        logger.info("Starting continuous 1-minute aggregated collection")
        
        # Start real-time data collection for all symbols
        tasks = []
        for symbol in self.config.symbols:
            task = asyncio.create_task(self.collect_real_time_data(symbol))
            tasks.append(task)
            logger.info(f"Started real-time collection for {symbol}")
        
        # Start 1-minute aggregation timer
        aggregation_task = asyncio.create_task(self._run_minute_aggregation())
        tasks.append(aggregation_task)
        
        # Wait for all tasks
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _run_minute_aggregation(self):
        """Run 1-minute aggregation timer"""
        while True:
            try:
                # Wait until the start of the next minute
                now = datetime.utcnow()
                seconds_to_next_minute = 60 - now.second
                await asyncio.sleep(seconds_to_next_minute)
                
                # Generate and store aggregated data for all symbols
                logger.info("🎯 Generating 1-minute aggregated data...")
                for symbol in self.config.symbols:
                    aggregated_data = await self.generate_1m_aggregated_data(symbol)
                    if aggregated_data:
                        await self.store_aggregated_data(symbol, aggregated_data)
                        logger.info(f"✅ {symbol}: {aggregated_data['trade_metrics']['count']} trades, "
                                  f"{aggregated_data['trade_metrics']['total_volume']:.3f} volume")
                
                # Wait a bit to avoid timing issues
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in minute aggregation: {e}")
                await asyncio.sleep(5)
EOF

# Upload unified collector
scp_file "temp_unified_collector.py" "/home/$USERNAME/autotrader/data_fetcher/unified_collector.py"

# Create run script
cat > temp_run_unified_collector.py << 'EOF'
#!/usr/bin/env python3
"""
Run script for Unified 1-Minute Aggregated Collector
"""

import os
import asyncio
import logging
from unified_collector import UnifiedCollector1M, CollectorConfig

# Setup logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/unified_collector.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def main():
    """Main function"""
    try:
        logger.info("🚀 Starting 1-Minute Aggregated Unified Collector")
        
        # Load configuration from environment
        config = CollectorConfig(
            slave_id=os.getenv('SLAVE_ID', 'unknown'),
            symbols=os.getenv('SYMBOLS', '').split(','),
            mongo_uri=os.getenv('MONGO_URI', 'mongodb://localhost:27017/'),
            mongo_db_name=os.getenv('MONGO_DB_NAME', 'trading_data'),
            timeframe=os.getenv('TIMEFRAME', '1m'),
            aggregation_interval=int(os.getenv('FETCH_INTERVAL', '60')),
            batch_size=int(os.getenv('BATCH_SIZE', '100'))
        )
        
        logger.info("Configuration loaded:")
        logger.info(f"  Slave ID: {config.slave_id}")
        logger.info(f"  Symbols: {len(config.symbols)} symbols")
        logger.info(f"  MongoDB: {config.mongo_db_name}")
        logger.info(f"  Timeframe: {config.timeframe}")
        
        # Initialize collector
        collector = UnifiedCollector1M(config)
        
        # Load exchange markets
        collector.exchange.load_markets()
        logger.info("✅ 1-Minute Aggregated Unified Collector started successfully")
        
        logger.info("📊 Data will be aggregated every minute with:")
        logger.info("   • Trade volumes and counts")
        logger.info("   • Liquidation amounts")
        logger.info("   • Orderbook depth metrics")
        logger.info("   • Enhanced 1-minute metrics")
        
        # Start continuous collection
        await collector.run_continuous_collection()
        
    except Exception as e:
        logger.error(f"❌ Failed to start collector: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
EOF

# Upload run script
scp_file "temp_run_unified_collector.py" "/home/$USERNAME/autotrader/data_fetcher/run_unified_collector.py"
ssh_exec "chmod +x /home/$USERNAME/autotrader/data_fetcher/unified_collector.py"
ssh_exec "chmod +x /home/$USERNAME/autotrader/data_fetcher/run_unified_collector.py"

echo "✅ Unified collector deployed"
echo ""

echo -e "${BLUE}🔧 Step 5: Get Slave Configuration${NC}"
echo "-------------------------------"

# Get slave configuration from Master VM
echo "Downloading slave configuration from Master VM..."
sshpass -p "$PASSWORD" scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$USERNAME@$MASTER_IP:/tmp/slave_configs/${SLAVE_ID}.env" "temp_${SLAVE_ID}.env" 2>/dev/null || {
    echo "⚠️  Slave config not found on master, creating default config..."
    
    # Create default configuration
    cat > "temp_${SLAVE_ID}.env" << EOF
export SLAVE_ID="$SLAVE_ID"
export SYMBOLS="BTC/USDT:USDT,ETH/USDT:USDT"
export MASTER_URL="http://$MASTER_IP:8080"
export MONGO_URI="mongodb://trader:TradingData2025!@10.0.1.100:27017/trading_data"
export MONGO_DB_NAME="trading_data"
export TIMEFRAME="1m"
export FETCH_INTERVAL="60"
export BATCH_SIZE="50"
EOF
}

# Upload slave configuration
scp_file "temp_${SLAVE_ID}.env" "/home/$USERNAME/autotrader/slave.env"

echo "✅ Slave configuration deployed"
echo ""

echo -e "${BLUE}🎛️ Step 6: Create Service File${NC}"
echo "-----------------------------"

# Create systemd service
ssh_exec "sudo tee /etc/systemd/system/autotrader-slave.service > /dev/null << EOF
[Unit]
Description=AutoTrader Slave Data Collector ($SLAVE_ID)
After=network.target

[Service]
Type=simple
User=$USERNAME
WorkingDirectory=/home/$USERNAME/autotrader/data_fetcher
EnvironmentFile=/home/$USERNAME/autotrader/slave.env
ExecStart=/usr/bin/python3 /home/$USERNAME/autotrader/data_fetcher/run_unified_collector.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF"

# Enable and start service
ssh_exec "sudo systemctl daemon-reload"
ssh_exec "sudo systemctl enable autotrader-slave.service"
ssh_exec "sudo systemctl start autotrader-slave.service"

echo "✅ Slave service created and started"
echo ""

# Clean up temp files
rm -f temp_*.py temp_*.env

echo -e "${GREEN}🎉 SLAVE VM DEPLOYMENT COMPLETE!${NC}"
echo "=================================="
echo ""
echo -e "${BLUE}📊 Slave VM Status:${NC}"
echo "   🆔 Slave ID: $SLAVE_ID"
echo "   🌐 IP Address: $SLAVE_IP"
echo "   ⚙️ Collector Service: Running"
echo "   📊 Data Collection: 1-minute aggregation"
echo ""
echo -e "${BLUE}🔧 Service Commands:${NC}"
echo "   sudo systemctl status autotrader-slave"
echo "   sudo systemctl restart autotrader-slave"
echo "   sudo systemctl logs -f autotrader-slave"
echo ""
echo -e "${GREEN}✅ Slave VM $SLAVE_ID is collecting data!${NC}"