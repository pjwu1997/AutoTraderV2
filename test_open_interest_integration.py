#!/usr/bin/env python3
"""Test open interest integration in unified collector"""

import asyncio
import sys
import os

# Add path to the unified collector
sys.path.append('/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher')

from unified_collector import UnifiedCollector, CollectorConfig

async def test_open_interest_integration():
    """Test that open interest is properly included in unified collector"""
    
    print("🔍 Testing Open Interest Integration in Unified Collector")
    print("=" * 60)
    
    # Create config
    config = CollectorConfig(
        slave_id="test-slave",
        symbols=["BTC/USDT:USDT"],
        mongo_uri="mongodb://localhost:27017/",
        mongo_db_name="test_db",
        timeframe="1m"
    )
    
    # Create collector
    collector = UnifiedCollector(config)
    
    try:
        print("\n📊 Testing fetch_market_data with open interest...")
        data = await collector.fetch_market_data("BTC/USDT:USDT")
        
        print("\n✅ Market data fetched successfully!")
        print(f"   Symbol: {data.get('symbol')}")
        print(f"   Timestamp: {data.get('timestamp')}")
        print(f"   Slave ID: {data.get('slave_id')}")
        
        # Check if open interest is included
        open_interest = data.get('open_interest', {})
        if open_interest:
            print(f"\n🎯 Open Interest Data:")
            print(f"   Open Interest: {open_interest.get('open_interest', 'N/A')}")
            print(f"   Timestamp: {open_interest.get('timestamp', 'N/A')}")
            print("   ✅ Open interest successfully integrated!")
        else:
            print("\n❌ Open interest not found in data")
            
        # Show other data fields
        print(f"\n📋 Available data fields:")
        for key in data.keys():
            if key not in ['symbol', 'timestamp', 'slave_id']:
                value = data[key]
                if isinstance(value, dict):
                    print(f"   {key}: {len(value)} fields")
                elif isinstance(value, list):
                    print(f"   {key}: {len(value)} items")
                else:
                    print(f"   {key}: {type(value).__name__}")
                    
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_open_interest_integration())