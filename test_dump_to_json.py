#!/usr/bin/env python3
"""
Test slave VM by dumping all collected data to a local JSON file
"""

import asyncio
import json
import sys
import os
from datetime import datetime
from pathlib import Path

# Add path to the unified collector
sys.path.append('/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher')

from unified_collector import UnifiedCollector1M, CollectorConfig

async def collect_and_dump_data():
    """Collect data and dump to JSON file"""
    
    print("📊 COLLECTING DATA AND DUMPING TO JSON")
    print("=" * 60)
    
    # Create config
    config = CollectorConfig(
        slave_id="json-dump-test",
        symbols=["BTC/USDT:USDT", "ETH/USDT:USDT"],  # Test with 2 symbols
        mongo_uri="mongodb://localhost:27017/",
        mongo_db_name="json_dump_test",
        timeframe="1m",
        aggregation_interval=60
    )
    
    # Create collector
    collector = UnifiedCollector1M(config)
    
    try:
        print(f"🔧 Initialized collector at {datetime.utcnow()}")
        print(f"   Symbols: {config.symbols}")
        print(f"   MongoDB available: {collector.mongo_available}")
        
        # Collect data for each symbol
        all_data = {}
        
        for symbol in config.symbols:
            print(f"\n📡 Collecting data for {symbol}...")
            
            # Create minute buffer and collect real-time data for 30 seconds
            buffer = collector.get_or_create_minute_buffer(symbol)
            print(f"   Buffer created for minute: {datetime.fromtimestamp(buffer.minute_start/1000)}")
            
            # Start collection task
            collection_task = asyncio.create_task(collector.collect_real_time_data(symbol))
            
            # Let it collect for 30 seconds
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 30:
                await asyncio.sleep(5)
                current_buffer = collector.minute_buffers.get(symbol)
                if current_buffer:
                    elapsed = int(asyncio.get_event_loop().time() - start_time)
                    print(f"   [{elapsed:2d}s] {current_buffer.trade_count} trades, "
                          f"{len(current_buffer.orderbook_snapshots)} snapshots, "
                          f"{current_buffer.liquidation_count} liquidations")
            
            # Cancel collection
            collection_task.cancel()
            try:
                await collection_task
            except asyncio.CancelledError:
                pass
            
            # Generate aggregated data
            print(f"   📊 Generating 1-minute aggregated data...")
            aggregated_data = await collector.generate_1m_aggregated_data(symbol)
            
            if aggregated_data:
                # Add some metadata
                aggregated_data['collection_metadata'] = {
                    'collection_duration_seconds': 30,
                    'collection_timestamp': datetime.utcnow().isoformat(),
                    'data_completeness': 'full',
                    'mongodb_available': collector.mongo_available,
                    'exchange_markets_loaded': len(collector.exchange.markets) if hasattr(collector.exchange, 'markets') else 'unknown'
                }
                
                all_data[symbol] = aggregated_data
                print(f"   ✅ Data collected for {symbol}")
                
                # Show quick summary
                trade_metrics = aggregated_data.get('trade_metrics', {})
                print(f"      Trades: {trade_metrics.get('count', 0)}")
                print(f"      Volume: {trade_metrics.get('total_volume', 0):.6f}")
                print(f"      VWAP: ${trade_metrics.get('vwap', 0):.2f}")
            else:
                print(f"   ❌ Failed to collect data for {symbol}")
        
        # Create output filename with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_file = f"slave_vm_data_dump_{timestamp}.json"
        
        # Add summary to the data
        summary = {
            'collection_summary': {
                'timestamp': datetime.utcnow().isoformat(),
                'symbols_collected': len(all_data),
                'total_symbols_requested': len(config.symbols),
                'success_rate': f"{len(all_data)/len(config.symbols)*100:.1f}%",
                'collector_version': '1m_aggregated_unified',
                'data_types_per_symbol': [
                    'ohlcv', 'trade_metrics', 'liquidation_metrics', 
                    'orderbook', 'orderbook_metrics', 'funding_rate',
                    'long_short_ratios', 'ticker_24h', 'open_interest', 
                    'enhanced_metrics'
                ]
            },
            'symbols_data': all_data
        }
        
        # Write to JSON file
        print(f"\n💾 Writing data to {output_file}...")
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        # Get file size
        file_size = os.path.getsize(output_file)
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✅ Data dumped successfully!")
        print(f"   File: {output_file}")
        print(f"   Size: {file_size:,} bytes ({file_size_mb:.2f} MB)")
        
        # Print file structure summary
        print(f"\n📋 JSON FILE STRUCTURE:")
        print(f"├── collection_summary")
        print(f"│   ├── timestamp: {summary['collection_summary']['timestamp']}")
        print(f"│   ├── symbols_collected: {summary['collection_summary']['symbols_collected']}")
        print(f"│   └── success_rate: {summary['collection_summary']['success_rate']}")
        print(f"├── symbols_data")
        for symbol in all_data.keys():
            data = all_data[symbol]
            print(f"│   ├── {symbol}")
            print(f"│   │   ├── symbol: {data.get('symbol')}")
            print(f"│   │   ├── timestamp: {datetime.fromtimestamp(data.get('timestamp', 0)/1000)}")
            print(f"│   │   ├── ohlcv: {len(data.get('ohlcv', []))} values" if data.get('ohlcv') else "│   │   ├── ohlcv: null")
            print(f"│   │   ├── trade_metrics: {len(data.get('trade_metrics', {}))} fields")
            print(f"│   │   ├── liquidation_metrics: {len(data.get('liquidation_metrics', {}))} fields")
            print(f"│   │   ├── orderbook: {len(data.get('orderbook', {}).get('bids', []))} bids, {len(data.get('orderbook', {}).get('asks', []))} asks")
            print(f"│   │   ├── funding_rate: {'present' if data.get('funding_rate') else 'null'}")
            print(f"│   │   ├── long_short_ratios: {len(data.get('long_short_ratios', {}))} types")
            print(f"│   │   ├── open_interest: {'present' if data.get('open_interest') else 'null'}")
            print(f"│   │   ├── ticker_24h: {'present' if data.get('ticker_24h') else 'null'}")
            print(f"│   │   └── enhanced_metrics: {len(data.get('enhanced_metrics', {}))} fields")
        
        # Show data completeness
        print(f"\n📊 DATA COMPLETENESS ANALYSIS:")
        data_types = ['ohlcv', 'trade_metrics', 'liquidation_metrics', 'orderbook', 
                     'funding_rate', 'long_short_ratios', 'ticker_24h', 'open_interest', 'enhanced_metrics']
        
        for symbol in all_data.keys():
            data = all_data[symbol]
            completeness = []
            for dt in data_types:
                if dt in data and data[dt]:
                    if isinstance(data[dt], dict) and len(data[dt]) > 0:
                        completeness.append(f"✅ {dt}")
                    elif isinstance(data[dt], list) and len(data[dt]) > 0:
                        completeness.append(f"✅ {dt}")
                    elif data[dt] is not None:
                        completeness.append(f"✅ {dt}")
                    else:
                        completeness.append(f"❌ {dt}")
                else:
                    completeness.append(f"❌ {dt}")
            
            complete_count = sum(1 for c in completeness if c.startswith("✅"))
            print(f"   {symbol}: {complete_count}/{len(data_types)} ({complete_count/len(data_types)*100:.1f}%)")
        
        print(f"\n🎯 JSON file ready for inspection!")
        print(f"You can now examine the complete data structure in: {output_file}")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error during collection: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    asyncio.run(collect_and_dump_data())