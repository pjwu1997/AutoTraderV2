#!/usr/bin/env python3
"""
Unified Collector Launcher - Replaces all individual collectors

This script serves as the single entry point for all data collection activities.
It consolidates the functionality of:
- enhanced_data_fetcher.py
- distributed_data_fetcher.py  
- All individual collectors
- WebSocket handlers
"""

import os
import sys
import asyncio
import logging
from unified_collector import UnifiedCollector, CollectorConfig

def load_config_from_env():
    """Load configuration from environment variables and slave config file"""
    
    # Get basic config from environment
    slave_id = os.getenv("SLAVE_ID", "slave-1")
    symbols_env = os.getenv("SYMBOLS", "")
    
    # Load symbols from slave config file if not in environment
    if not symbols_env:
        try:
            config_file = f"/app/slave-{slave_id.split('-')[-1]}.env"
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    for line in f:
                        if line.startswith("SYMBOLS="):
                            symbols_env = line.split("=", 1)[1].strip()
                            break
        except Exception as e:
            logging.warning(f"Could not load symbols from config file: {e}")
    
    # Parse symbols
    symbols = []
    if symbols_env:
        symbols = [s.strip() for s in symbols_env.split(",") if s.strip()]
    
    # Convert symbols to correct format
    formatted_symbols = []
    for symbol in symbols:
        if ":" not in symbol:
            # Convert BTCUSDT to BTC:USDT format
            if symbol.endswith("USDT"):
                base = symbol[:-4]
                formatted_symbols.append(f"{base}:USDT")
            else:
                formatted_symbols.append(symbol)
        else:
            formatted_symbols.append(symbol)
    
    if not formatted_symbols:
        # Default symbols for testing
        formatted_symbols = ["BTC:USDT", "ETH:USDT", "SOL:USDT"]
        logging.warning("No symbols found, using default symbols")
    
    return CollectorConfig(
        slave_id=slave_id,
        symbols=formatted_symbols,
        mongo_uri=os.getenv("MONGO_URI", "mongodb://10.0.1.100:27017/"),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "trading_data"),
        timeframe=os.getenv("TIMEFRAME", "1m"),
        fetch_interval=int(os.getenv("FETCH_INTERVAL", "60")),
        batch_size=int(os.getenv("BATCH_SIZE", "15")),
        rate_limit_delay=float(os.getenv("RATE_LIMIT_DELAY", "0.1")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        enable_websocket=os.getenv("ENABLE_WEBSOCKET", "true").lower() == "true",
        enable_rest_api=os.getenv("ENABLE_REST_API", "true").lower() == "true"
    )

async def main():
    """Main execution function"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('/app/logs/unified_collector.log') if os.path.exists('/app/logs') else logging.NullHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Load configuration
        config = load_config_from_env()
        
        logger.info("=" * 60)
        logger.info("🚀 Starting Unified Data Collector")
        logger.info("=" * 60)
        logger.info(f"Slave ID: {config.slave_id}")
        logger.info(f"Symbols: {len(config.symbols)} ({', '.join(config.symbols[:5])}{'...' if len(config.symbols) > 5 else ''})")
        logger.info(f"MongoDB: {config.mongo_uri}")
        logger.info(f"Timeframe: {config.timeframe}")
        logger.info(f"Fetch Interval: {config.fetch_interval}s")
        logger.info(f"WebSocket: {'Enabled' if config.enable_websocket else 'Disabled'}")
        logger.info(f"REST API: {'Enabled' if config.enable_rest_api else 'Disabled'}")
        logger.info("=" * 60)
        
        # Initialize and start collector
        collector = UnifiedCollector(config)
        
        # Test MongoDB connection
        try:
            collector.mongo_client.admin.command('ping')
            logger.info("✅ MongoDB connection successful")
        except Exception as e:
            logger.error(f"❌ MongoDB connection failed: {e}")
            logger.info("Continuing with local fallback...")
        
        # Start data collection
        await collector.start_continuous_collection()
        
    except KeyboardInterrupt:
        logger.info("📨 Received interrupt signal - shutting down gracefully")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            collector.stop_collection()
        except:
            pass
        logger.info("🛑 Unified collector stopped")

if __name__ == "__main__":
    # Ensure we can import the unified_collector module
    current_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, current_dir)
    
    # Run the collector
    asyncio.run(main())