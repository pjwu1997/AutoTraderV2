#!/usr/bin/env python3
"""
Launcher for 1-Minute Aggregated Unified Collector
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from unified_collector_1m import UnifiedCollector1M, CollectorConfig

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/unified_collector_1m.log'),
            logging.StreamHandler()
        ]
    )

def load_config_from_env() -> CollectorConfig:
    """Load collector configuration from environment variables"""
    
    # Load symbols from environment
    symbols_env = os.getenv("SYMBOLS", "")
    if not symbols_env:
        raise ValueError("SYMBOLS environment variable is required")
    
    symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
    
    # Ensure symbols are in correct format
    formatted_symbols = []
    for symbol in symbols:
        if ':' not in symbol and '/' not in symbol:
            # Convert BTCUSDT to BTC/USDT:USDT
            if symbol.endswith('USDT'):
                base = symbol[:-4]
                formatted_symbol = f"{base}/USDT:USDT"
            else:
                formatted_symbol = symbol
        else:
            formatted_symbol = symbol
        formatted_symbols.append(formatted_symbol)
    
    return CollectorConfig(
        slave_id=os.getenv("SLAVE_ID", "slave-unknown"),
        symbols=formatted_symbols,
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "trading_data"),
        timeframe=os.getenv("TIMEFRAME", "1m"),
        aggregation_interval=int(os.getenv("AGGREGATION_INTERVAL", "60")),
        batch_size=int(os.getenv("BATCH_SIZE", "15")),
        rate_limit_delay=float(os.getenv("RATE_LIMIT_DELAY", "0.1")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        enable_websocket=os.getenv("ENABLE_WEBSOCKET", "true").lower() == "true",
        enable_rest_api=os.getenv("ENABLE_REST_API", "true").lower() == "true"
    )

async def main():
    """Main execution function"""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        logger.info("🚀 Starting 1-Minute Aggregated Unified Collector")
        
        # Load configuration
        config = load_config_from_env()
        
        logger.info(f"Configuration loaded:")
        logger.info(f"  Slave ID: {config.slave_id}")
        logger.info(f"  Symbols: {len(config.symbols)} symbols")
        logger.info(f"  MongoDB: {config.mongo_uri}")
        logger.info(f"  Aggregation Interval: {config.aggregation_interval}s")
        
        # Create and start collector
        collector = UnifiedCollector1M(config)
        
        logger.info("✅ 1-Minute Aggregated Unified Collector started successfully")
        logger.info("📊 Data will be aggregated every minute with:")
        logger.info("   • Trade volumes and counts")
        logger.info("   • Liquidation amounts") 
        logger.info("   • Orderbook depth metrics")
        logger.info("   • Enhanced 1-minute metrics")
        
        await collector.start_continuous_collection()
        
    except KeyboardInterrupt:
        logger.info("⏹️  Collector stopped by user")
    except Exception as e:
        logger.error(f"❌ Error starting collector: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())