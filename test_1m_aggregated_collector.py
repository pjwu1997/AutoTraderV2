#!/usr/bin/env python3
"""Test 1-minute aggregated unified collector"""

import asyncio
import json
import sys
import os
from datetime import datetime

# Add path to the unified collector
sys.path.append('/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher')

from unified_collector import UnifiedCollector1M, CollectorConfig

async def test_1m_aggregated_collector():
    """Test the 1-minute aggregated collector"""
    
    print("🔄 Testing 1-Minute Aggregated Unified Collector")
    print("=" * 60)
    
    # Create config
    config = CollectorConfig(
        slave_id="test-slave-1m",
        symbols=["BTC/USDT:USDT"],
        mongo_uri="mongodb://localhost:27017/",
        mongo_db_name="test_1m_aggregated",
        timeframe="1m",
        aggregation_interval=60
    )
    
    # Create collector
    collector = UnifiedCollector1M(config)
    
    try:
        print("\n📊 Testing minute buffer creation...")
        
        # Test minute buffer creation
        symbol = "BTC/USDT:USDT"
        buffer = collector.get_or_create_minute_buffer(symbol)
        
        print(f"✅ Created minute buffer for {symbol}")
        print(f"   Minute start: {buffer.minute_start}")
        print(f"   Current time: {datetime.utcnow()}")
        
        print("\n🔄 Testing real-time data collection (30 seconds)...")
        
        # Test real-time data collection for 30 seconds
        start_time = asyncio.get_event_loop().time()
        end_time = start_time + 30  # 30 seconds
        
        collection_task = asyncio.create_task(collector.collect_real_time_data(symbol))
        
        # Wait for 30 seconds then cancel
        while asyncio.get_event_loop().time() < end_time:
            await asyncio.sleep(1)
            
            # Check buffer status every 5 seconds
            if int(asyncio.get_event_loop().time() - start_time) % 5 == 0:
                current_buffer = collector.minute_buffers.get(symbol)
                if current_buffer:
                    print(f"   Buffer status: {current_buffer.trade_count} trades, "
                          f"{len(current_buffer.orderbook_snapshots)} orderbook snapshots, "
                          f"{current_buffer.liquidation_count} liquidations")
        
        # Cancel the collection task
        collection_task.cancel()
        
        try:
            await collection_task
        except asyncio.CancelledError:
            pass
        
        print("\n📋 Testing 1-minute aggregation...")
        
        # Test aggregation
        final_buffer = collector.minute_buffers.get(symbol)
        if final_buffer:
            aggregated_data = await collector.generate_1m_aggregated_data(symbol)
            
            if aggregated_data:
                print("✅ Successfully generated 1-minute aggregated data!")
                
                # Show summary
                print(f"\n📊 AGGREGATED DATA SUMMARY:")
                print(f"• Symbol: {aggregated_data.get('symbol')}")
                print(f"• Minute start: {aggregated_data.get('timestamp')}")
                print(f"• Minute end: {aggregated_data.get('minute_end')}")
                
                # Trade metrics
                trade_metrics = aggregated_data.get('trade_metrics', {})
                print(f"\n💱 TRADE METRICS:")
                print(f"• Total trades: {trade_metrics.get('count', 0)}")
                print(f"• Total volume: {trade_metrics.get('total_volume', 0):.6f}")
                print(f"• Buy volume: {trade_metrics.get('buy_volume', 0):.6f}")
                print(f"• Sell volume: {trade_metrics.get('sell_volume', 0):.6f}")
                print(f"• VWAP: ${trade_metrics.get('vwap', 0):.2f}")
                print(f"• Buy/Sell ratio: {trade_metrics.get('buy_sell_ratio', 0):.3f}")
                
                # Liquidation metrics
                liq_metrics = aggregated_data.get('liquidation_metrics', {})
                print(f"\n🔥 LIQUIDATION METRICS:")
                print(f"• Total liquidations: {liq_metrics.get('count', 0)}")
                print(f"• Buy liquidations: {liq_metrics.get('buy_volume', 0):.6f}")
                print(f"• Sell liquidations: {liq_metrics.get('sell_volume', 0):.6f}")
                print(f"• Total liquidation volume: {liq_metrics.get('total_volume', 0):.6f}")
                
                # Orderbook metrics
                ob_metrics = aggregated_data.get('orderbook_metrics', {})
                print(f"\n📊 ORDERBOOK METRICS:")
                print(f"• Avg spread: ${ob_metrics.get('avg_spread', 0):.2f}")
                print(f"• Avg bid depth: {ob_metrics.get('avg_bid_depth', 0):.3f}")
                print(f"• Avg ask depth: {ob_metrics.get('avg_ask_depth', 0):.3f}")
                print(f"• Snapshots: {ob_metrics.get('snapshot_count', 0)}")
                
                # Enhanced metrics
                enhanced = aggregated_data.get('enhanced_metrics', {})
                print(f"\n🧮 ENHANCED METRICS:")
                print(f"• CVD: {enhanced.get('cvd', 0):.6f}")
                print(f"• Spread volatility: ${enhanced.get('spread_volatility', 0):.4f}")
                print(f"• Depth imbalance: {enhanced.get('depth_imbalance', 0):.3f}")
                
                # Check if funding rate and other data exists
                funding = aggregated_data.get('funding_rate', {})
                open_interest = aggregated_data.get('open_interest', {})
                
                print(f"\n💰 OTHER DATA:")
                print(f"• Funding rate: {'✅' if funding else '❌'}")
                print(f"• Open interest: {'✅' if open_interest else '❌'}")
                print(f"• Long/short ratios: {'✅' if aggregated_data.get('long_short_ratios') else '❌'}")
                
                print(f"\n🎯 1-MINUTE AGGREGATION STRUCTURE:")
                print("✅ All data is now aggregated per minute:")
                print("   • Trades → Volume, count, VWAP per minute")
                print("   • Liquidations → Total liquidation amounts per minute")
                print("   • Orderbook → Average spreads and depths per minute") 
                print("   • Enhanced metrics → Calculated from minute data")
                print("   • Rates → Latest values within the minute")
                
            else:
                print("❌ Failed to generate aggregated data")
        else:
            print("❌ No buffer found for symbol")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_1m_aggregated_collector())