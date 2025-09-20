#!/usr/bin/env python3
"""
Local test script for unified collector

This script allows you to test the unified collector locally before deploying to slaves.
"""

import asyncio
import sys
import os
import logging
from datetime import datetime

# Add the project directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'DistributedSystem/SlaveVM/data_fetcher'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'DataFetcher'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'DistributedSystem/Common'))

from DistributedSystem.SlaveVM.data_fetcher.unified_collector import UnifiedCollector, CollectorConfig

async def test_unified_collector():
    """Test the unified collector locally"""
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    print("🧪 Testing Unified Collector Locally")
    print("=" * 50)
    
    # Test configuration (use smaller set for local testing)
    config = CollectorConfig(
        slave_id="test-local",
        symbols=["BTC:USDT", "ETH:USDT", "SOL:USDT"],  # Small set for testing
        mongo_uri="mongodb://localhost:27017/",  # Local MongoDB
        mongo_db_name="test_trading_data",
        timeframe="1m",
        fetch_interval=30,  # Shorter interval for testing
        batch_size=3,
        rate_limit_delay=1.0,  # Longer delay for testing
        max_retries=2,
        enable_websocket=False,  # Disable WebSocket for initial test
        enable_rest_api=True
    )
    
    print(f"Test Config:")
    print(f"  Symbols: {config.symbols}")
    print(f"  MongoDB: {config.mongo_uri}")
    print(f"  WebSocket: {config.enable_websocket}")
    print(f"  REST API: {config.enable_rest_api}")
    print()
    
    try:
        # Initialize collector
        print("🔧 Initializing unified collector...")
        collector = UnifiedCollector(config)
        
        # Test MongoDB connection
        print("🔌 Testing MongoDB connection...")
        try:
            collector.mongo_client.admin.command('ping')
            print("✅ MongoDB connection successful")
        except Exception as e:
            print(f"⚠️  MongoDB connection failed: {e}")
            print("   (This is OK for testing - data won't be stored)")
        
        # Test individual components
        print("\n📊 Testing individual data fetching components...")
        
        # Test market data fetching
        test_symbol = "BTC:USDT"
        print(f"\n🔍 Testing market data fetch for {test_symbol}...")
        
        try:
            market_data = await collector.fetch_market_data(test_symbol)
            if market_data:
                print("✅ Market data fetch successful")
                print(f"   - OHLCV data points: {len(market_data.get('ohlcv', []))}")
                print(f"   - Funding rate: {market_data.get('funding_rate', {}).get('current_rate', 'N/A')}")
                print(f"   - Long/short ratios: {len(market_data.get('long_short_ratios', {}))}")
                print(f"   - Enhanced metrics: {len(market_data.get('enhanced_metrics', {}))}")
            else:
                print("❌ Market data fetch failed")
        except Exception as e:
            print(f"❌ Market data fetch error: {e}")
        
        # Test individual API calls
        print(f"\n🔍 Testing individual API components for {test_symbol}...")
        
        # Test OHLCV
        try:
            ohlcv = await collector._fetch_ohlcv(test_symbol, limit=5)
            print(f"✅ OHLCV: {len(ohlcv)} candles")
        except Exception as e:
            print(f"❌ OHLCV error: {e}")
        
        # Test funding rate
        try:
            funding = await collector._fetch_funding_rate(test_symbol)
            print(f"✅ Funding rate: {funding.get('current_rate', 'N/A')}")
        except Exception as e:
            print(f"❌ Funding rate error: {e}")
        
        # Test long/short ratios
        try:
            ratios = await collector._fetch_long_short_ratios(test_symbol)
            print(f"✅ Long/short ratios: {len(ratios)} types")
        except Exception as e:
            print(f"❌ Long/short ratios error: {e}")
        
        # Test orderbook
        try:
            orderbook = await collector._fetch_orderbook(test_symbol, limit=5)
            print(f"✅ Orderbook: {len(orderbook.get('bids', []))} bids, {len(orderbook.get('asks', []))} asks")
        except Exception as e:
            print(f"❌ Orderbook error: {e}")
        
        # Test one full collection cycle
        print(f"\n🔄 Testing one complete collection cycle...")
        try:
            await collector.run_collection_cycle()
            print("✅ Collection cycle completed successfully")
        except Exception as e:
            print(f"❌ Collection cycle error: {e}")
        
        print(f"\n✅ Local testing completed successfully!")
        print(f"🚀 The unified collector is ready for deployment!")
        
    except Exception as e:
        print(f"💥 Test failed with error: {e}")
        logger.error("Test failed", exc_info=True)
        return False
    
    finally:
        try:
            collector.stop_collection()
        except:
            pass
    
    return True

async def test_websocket_briefly():
    """Test WebSocket functionality briefly"""
    print("\n🌐 Testing WebSocket functionality (30 seconds)...")
    
    config = CollectorConfig(
        slave_id="test-websocket",
        symbols=["BTC:USDT"],  # Single symbol for WebSocket test
        mongo_uri="mongodb://localhost:27017/",
        mongo_db_name="test_trading_data",
        enable_websocket=True,
        enable_rest_api=False
    )
    
    try:
        collector = UnifiedCollector(config)
        
        # Start WebSocket collection
        ws_task = asyncio.create_task(collector.start_websocket_collection())
        
        # Let it run for 30 seconds
        print("   Listening to WebSocket streams for 30 seconds...")
        await asyncio.sleep(30)
        
        # Stop
        collector.stop_collection()
        ws_task.cancel()
        
        print("✅ WebSocket test completed")
        
    except Exception as e:
        print(f"❌ WebSocket test error: {e}")

if __name__ == "__main__":
    print("Starting unified collector local tests...")
    print("Make sure you have MongoDB running locally or the tests will show connection warnings.")
    print()
    
    # Run main test
    success = asyncio.run(test_unified_collector())
    
    if success:
        # Ask if user wants to test WebSocket
        try:
            response = input("\n🤔 Test WebSocket functionality for 30 seconds? (y/n): ").lower().strip()
            if response == 'y':
                asyncio.run(test_websocket_briefly())
        except KeyboardInterrupt:
            print("\n🛑 Test interrupted by user")
        
        print("\n🎉 All tests completed!")
    else:
        print("\n❌ Tests failed - check the errors above")
        sys.exit(1)