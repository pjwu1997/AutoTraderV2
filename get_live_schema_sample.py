#!/usr/bin/env python3
"""Get live schema sample from unified collector"""

import asyncio
import json
import sys
import os

# Add path to the unified collector
sys.path.append('/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher')

from unified_collector import UnifiedCollector, CollectorConfig

async def get_live_schema_sample():
    """Get actual live data sample from unified collector"""
    
    print("📊 LIVE UNIFIED COLLECTOR DATA SAMPLE")
    print("=" * 60)
    
    # Create config
    config = CollectorConfig(
        slave_id="sample-slave",
        symbols=["BTC/USDT:USDT"],
        mongo_uri="mongodb://localhost:27017/",
        mongo_db_name="sample_db",
        timeframe="1m"
    )
    
    # Create collector
    collector = UnifiedCollector(config)
    
    try:
        print("\n🔄 Fetching LIVE market data for BTC/USDT:USDT...")
        data = await collector.fetch_market_data("BTC/USDT:USDT")
        
        print("\n✅ SUCCESS! Here's the complete live schema:")
        print("=" * 60)
        
        # Pretty print with proper formatting
        formatted_data = json.dumps(data, indent=2, default=str)
        
        # Truncate very long arrays for readability
        lines = formatted_data.split('\n')
        output_lines = []
        in_long_array = False
        array_count = 0
        
        for line in lines:
            if '"ohlcv": [' in line:
                output_lines.append(line)
                in_long_array = True
                array_count = 0
                continue
            elif in_long_array:
                if array_count < 3:  # Show first 3 items
                    output_lines.append(line)
                    if line.strip().startswith('[') and line.strip().endswith('],'):
                        array_count += 1
                elif array_count == 3:
                    output_lines.append('    // ... (97 more OHLCV candles)')
                    array_count += 1
                elif line.strip() == '],':
                    output_lines.append(line)
                    in_long_array = False
                continue
            else:
                output_lines.append(line)
        
        print('\n'.join(output_lines[:100]))  # Show first 100 lines
        
        if len(output_lines) > 100:
            print("\n... (truncated for readability)")
        
        # Print summary statistics
        print(f"\n📋 DATA SUMMARY:")
        print(f"• Symbol: {data.get('symbol')}")
        print(f"• Collection time: {data.get('timestamp')}")
        print(f"• OHLCV candles: {len(data.get('ohlcv', []))}")
        print(f"• Orderbook bids: {len(data.get('orderbook', {}).get('bids', []))}")
        print(f"• Orderbook asks: {len(data.get('orderbook', {}).get('asks', []))}")
        print(f"• Recent trades: {len(data.get('trades', []))}")
        print(f"• Funding rate: {'✅' if data.get('funding_rate') else '❌'}")
        print(f"• Long/short ratios: {len(data.get('long_short_ratios', {}))}")
        print(f"• 24h ticker: {'✅' if data.get('ticker_24h') else '❌'}")
        print(f"• Open interest: {'✅' if data.get('open_interest') else '❌'}")
        print(f"• Enhanced metrics: {len(data.get('enhanced_metrics', {}))}")
        
        # Show specific field examples
        if data.get('open_interest'):
            oi = data['open_interest']
            print(f"\n💡 OPEN INTEREST SAMPLE:")
            print(f"• Amount: {oi.get('open_interest')} BTC")
            print(f"• Timestamp: {oi.get('timestamp')}")
        
        if data.get('funding_rate'):
            fr = data['funding_rate']
            print(f"\n💡 FUNDING RATE SAMPLE:")
            print(f"• Current rate: {fr.get('current_rate')}%")
            print(f"• Mark price: ${fr.get('mark_price')}")
        
        if data.get('enhanced_metrics'):
            em = data['enhanced_metrics']
            print(f"\n💡 ENHANCED METRICS SAMPLE:")
            print(f"• CVD: {em.get('cvd')}")
            print(f"• Spread: ${em.get('spread')}")
            print(f"• Volatility: {em.get('volatility')}")
            
    except Exception as e:
        print(f"❌ Error fetching live data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(get_live_schema_sample())