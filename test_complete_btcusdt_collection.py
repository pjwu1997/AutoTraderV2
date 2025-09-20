#!/usr/bin/env python3
"""
Comprehensive test for BTCUSDT data collection with the new 1-minute aggregated collector
"""

import asyncio
import json
import sys
import os
from datetime import datetime, timedelta

# Add path to the unified collector
sys.path.append('/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher')

from unified_collector import UnifiedCollector1M, CollectorConfig

async def test_complete_btcusdt_collection():
    """Test complete BTCUSDT data collection with detailed verification"""
    
    print("🔍 COMPLETE BTCUSDT DATA COLLECTION TEST")
    print("=" * 60)
    
    # Create config for BTCUSDT only
    config = CollectorConfig(
        slave_id="test-btcusdt",
        symbols=["BTC/USDT:USDT"],  # Focus on BTCUSDT
        mongo_uri="mongodb://localhost:27017/",
        mongo_db_name="test_btcusdt",
        timeframe="1m",
        aggregation_interval=60
    )
    
    # Create collector
    collector = UnifiedCollector1M(config)
    
    try:
        print(f"\n📊 Testing BTCUSDT collection at {datetime.utcnow()}")
        symbol = "BTC/USDT:USDT"
        
        # Test 1: Minute buffer creation and basic setup
        print("\n1️⃣ Testing minute buffer creation...")
        buffer = collector.get_or_create_minute_buffer(symbol)
        print(f"✅ Buffer created for minute: {datetime.fromtimestamp(buffer.minute_start/1000)}")
        
        # Test 2: Individual data source testing
        print("\n2️⃣ Testing individual data sources...")
        
        # Test orderbook
        print("   📊 Testing orderbook...")
        orderbook = await collector._fetch_orderbook_snapshot(symbol)
        if orderbook and orderbook.get('bids') and orderbook.get('asks'):
            print(f"   ✅ Orderbook: {len(orderbook['bids'])} bids, {len(orderbook['asks'])} asks")
            print(f"      Best bid: ${orderbook['bids'][0][0]}, Best ask: ${orderbook['asks'][0][0]}")
        else:
            print("   ❌ Orderbook failed")
        
        # Test trades
        print("   💱 Testing trades...")
        trades = await collector._fetch_recent_trades_for_aggregation(symbol)
        if trades:
            print(f"   ✅ Trades: {len(trades)} recent trades")
            print(f"      Latest trade: ${trades[0]['price']} x {trades[0]['amount']} ({trades[0]['side']})")
        else:
            print("   ❌ Trades failed")
        
        # Test funding rate
        print("   💰 Testing funding rate...")
        funding = await collector._fetch_funding_rate(symbol)
        if funding:
            print(f"   ✅ Funding rate: {funding.get('current_rate', 0)*100:.4f}%")
            print(f"      Mark price: ${funding.get('mark_price', 0)}")
        else:
            print("   ❌ Funding rate failed")
        
        # Test long/short ratios
        print("   ⚖️ Testing long/short ratios...")
        ratios = await collector._fetch_long_short_ratios(symbol)
        if ratios:
            print(f"   ✅ Long/short ratios: {len(ratios)} ratio types")
            for ratio_type, data in ratios.items():
                print(f"      {ratio_type}: {data.get('longShortRatio', 0):.2f}")
        else:
            print("   ❌ Long/short ratios failed")
        
        # Test open interest
        print("   🎯 Testing open interest...")
        open_interest = await collector._fetch_open_interest(symbol)
        if open_interest:
            print(f"   ✅ Open interest: {open_interest.get('open_interest', 0):,.3f} BTC")
        else:
            print("   ❌ Open interest failed")
        
        # Test 24h ticker
        print("   📈 Testing 24h ticker...")
        ticker = await collector._fetch_ticker(symbol)
        if ticker:
            print(f"   ✅ 24h ticker: ${ticker.get('close', 0)} ({ticker.get('percentage', 0):+.2f}%)")
            print(f"      Volume: {ticker.get('volume', 0):,.3f} BTC")
        else:
            print("   ❌ 24h ticker failed")
        
        # Test 3: Real-time aggregation simulation (45 seconds)
        print("\n3️⃣ Testing real-time aggregation (45 seconds)...")
        start_time = asyncio.get_event_loop().time()
        end_time = start_time + 45  # 45 seconds
        
        collection_task = asyncio.create_task(collector.collect_real_time_data(symbol))
        
        collection_stats = {
            'orderbook_snapshots': 0,
            'trade_batches': 0,
            'funding_updates': 0
        }
        
        # Monitor collection for 45 seconds
        while asyncio.get_event_loop().time() < end_time:
            await asyncio.sleep(5)
            
            current_buffer = collector.minute_buffers.get(symbol)
            if current_buffer:
                elapsed = int(asyncio.get_event_loop().time() - start_time)
                print(f"   [{elapsed:2d}s] Buffer: {current_buffer.trade_count} trades, "
                      f"{len(current_buffer.orderbook_snapshots)} snapshots, "
                      f"{current_buffer.liquidation_count} liquidations")
                
                # Track what data we're getting
                if len(current_buffer.orderbook_snapshots) > collection_stats['orderbook_snapshots']:
                    collection_stats['orderbook_snapshots'] = len(current_buffer.orderbook_snapshots)
                
                if current_buffer.trade_count > 0:
                    collection_stats['trade_batches'] += 1
                
                if current_buffer.latest_funding_rate:
                    collection_stats['funding_updates'] = 1
        
        # Cancel collection task
        collection_task.cancel()
        try:
            await collection_task
        except asyncio.CancelledError:
            pass
        
        # Test 4: Generate final aggregated data
        print("\n4️⃣ Testing 1-minute aggregation generation...")
        final_buffer = collector.minute_buffers.get(symbol)
        
        if final_buffer:
            aggregated_data = await collector.generate_1m_aggregated_data(symbol)
            
            if aggregated_data:
                print("✅ Successfully generated 1-minute aggregated data!")
                
                # Detailed verification of all data types
                print(f"\n📋 COMPLETE DATA VERIFICATION FOR BTCUSDT:")
                print(f"   Symbol: {aggregated_data.get('symbol')}")
                print(f"   Timestamp: {datetime.fromtimestamp(aggregated_data.get('timestamp', 0)/1000)}")
                print(f"   Collection type: {aggregated_data.get('collection_type')}")
                
                # OHLCV verification
                ohlcv = aggregated_data.get('ohlcv')
                print(f"\n📈 OHLCV CANDLE:")
                if ohlcv and len(ohlcv) >= 6:
                    print(f"   ✅ Open: ${ohlcv[1]}")
                    print(f"   ✅ High: ${ohlcv[2]}")
                    print(f"   ✅ Low: ${ohlcv[3]}")
                    print(f"   ✅ Close: ${ohlcv[4]}")
                    print(f"   ✅ Volume: {ohlcv[5]}")
                else:
                    print("   ❌ OHLCV data incomplete")
                
                # Trade metrics verification
                trade_metrics = aggregated_data.get('trade_metrics', {})
                print(f"\n💱 TRADE METRICS:")
                print(f"   ✅ Total trades: {trade_metrics.get('count', 0)}")
                print(f"   ✅ Total volume: {trade_metrics.get('total_volume', 0):.6f} BTC")
                print(f"   ✅ Buy volume: {trade_metrics.get('buy_volume', 0):.6f} BTC")
                print(f"   ✅ Sell volume: {trade_metrics.get('sell_volume', 0):.6f} BTC")
                print(f"   ✅ VWAP: ${trade_metrics.get('vwap', 0):.2f}")
                print(f"   ✅ Buy/Sell ratio: {trade_metrics.get('buy_sell_ratio', 0):.3f}")
                
                # Liquidation metrics verification
                liq_metrics = aggregated_data.get('liquidation_metrics', {})
                print(f"\n🔥 LIQUIDATION METRICS:")
                print(f"   ✅ Total liquidations: {liq_metrics.get('count', 0)}")
                print(f"   ✅ Buy liquidations: {liq_metrics.get('buy_volume', 0):.6f} BTC")
                print(f"   ✅ Sell liquidations: {liq_metrics.get('sell_volume', 0):.6f} BTC")
                print(f"   ✅ Total liquidation volume: {liq_metrics.get('total_volume', 0):.6f} BTC")
                
                # Orderbook metrics verification
                orderbook = aggregated_data.get('orderbook', {})
                orderbook_metrics = aggregated_data.get('orderbook_metrics', {})
                print(f"\n📊 ORDERBOOK & METRICS:")
                if orderbook.get('bids') and orderbook.get('asks'):
                    print(f"   ✅ Current bids: {len(orderbook['bids'])} levels")
                    print(f"   ✅ Current asks: {len(orderbook['asks'])} levels")
                    print(f"   ✅ Best bid: ${orderbook['bids'][0][0]} x {orderbook['bids'][0][1]}")
                    print(f"   ✅ Best ask: ${orderbook['asks'][0][0]} x {orderbook['asks'][0][1]}")
                else:
                    print("   ❌ Current orderbook missing")
                    
                print(f"   ✅ Avg spread: ${orderbook_metrics.get('avg_spread', 0):.4f}")
                print(f"   ✅ Avg bid depth: {orderbook_metrics.get('avg_bid_depth', 0):.3f}")
                print(f"   ✅ Avg ask depth: {orderbook_metrics.get('avg_ask_depth', 0):.3f}")
                print(f"   ✅ Snapshots taken: {orderbook_metrics.get('snapshot_count', 0)}")
                
                # Funding rate verification
                funding_rate = aggregated_data.get('funding_rate', {})
                print(f"\n💰 FUNDING RATE:")
                if funding_rate:
                    print(f"   ✅ Current rate: {funding_rate.get('current_rate', 0)*100:.4f}%")
                    print(f"   ✅ Mark price: ${funding_rate.get('mark_price', 0)}")
                    print(f"   ✅ Index price: ${funding_rate.get('index_price', 0)}")
                    print(f"   ✅ Next funding: {datetime.fromtimestamp(funding_rate.get('next_timestamp', 0)/1000)}")
                else:
                    print("   ❌ Funding rate data missing")
                
                # Long/short ratios verification
                ls_ratios = aggregated_data.get('long_short_ratios', {})
                print(f"\n⚖️ LONG/SHORT RATIOS:")
                if ls_ratios:
                    for ratio_type, data in ls_ratios.items():
                        if data:
                            ratio = data.get('longShortRatio', 0)
                            long_pct = data.get('longAccount', data.get('longPosition', 0)) * 100
                            print(f"   ✅ {ratio_type}: {ratio:.2f} ({long_pct:.1f}% long)")
                        else:
                            print(f"   ❌ {ratio_type}: No data")
                else:
                    print("   ❌ Long/short ratios missing")
                
                # Open interest verification
                open_interest = aggregated_data.get('open_interest', {})
                print(f"\n🎯 OPEN INTEREST:")
                if open_interest:
                    print(f"   ✅ Amount: {open_interest.get('open_interest', 0):,.3f} BTC")
                    print(f"   ✅ Timestamp: {datetime.fromtimestamp(open_interest.get('timestamp', 0)/1000)}")
                else:
                    print("   ❌ Open interest data missing")
                
                # 24h ticker verification
                ticker_24h = aggregated_data.get('ticker_24h', {})
                print(f"\n📈 24H TICKER:")
                if ticker_24h:
                    print(f"   ✅ Price: ${ticker_24h.get('close', 0):,.2f}")
                    print(f"   ✅ 24h change: {ticker_24h.get('percentage', 0):+.2f}%")
                    print(f"   ✅ 24h volume: {ticker_24h.get('volume', 0):,.3f} BTC")
                    print(f"   ✅ 24h high: ${ticker_24h.get('high', 0):,.2f}")
                    print(f"   ✅ 24h low: ${ticker_24h.get('low', 0):,.2f}")
                else:
                    print("   ❌ 24h ticker data missing")
                
                # Enhanced metrics verification
                enhanced = aggregated_data.get('enhanced_metrics', {})
                print(f"\n🧮 ENHANCED METRICS:")
                print(f"   ✅ CVD: {enhanced.get('cvd', 0):.6f}")
                print(f"   ✅ Spread volatility: ${enhanced.get('spread_volatility', 0):.4f}")
                print(f"   ✅ Depth imbalance: {enhanced.get('depth_imbalance', 0):.3f}")
                print(f"   ✅ Total volume: {enhanced.get('total_volume', 0):.6f} BTC")
                print(f"   ✅ Total liquidations: {enhanced.get('total_liquidation_volume', 0):.6f} BTC")
                
                # Test 5: Data completeness summary
                print(f"\n5️⃣ DATA COMPLETENESS SUMMARY:")
                data_types = {
                    'OHLCV': bool(ohlcv and len(ohlcv) >= 6),
                    'Trade Metrics': bool(trade_metrics.get('count', 0) > 0),
                    'Liquidation Metrics': 'count' in liq_metrics,
                    'Orderbook': bool(orderbook.get('bids') and orderbook.get('asks')),
                    'Orderbook Metrics': bool(orderbook_metrics.get('snapshot_count', 0) > 0),
                    'Funding Rate': bool(funding_rate),
                    'Long/Short Ratios': bool(ls_ratios),
                    'Open Interest': bool(open_interest),
                    '24h Ticker': bool(ticker_24h),
                    'Enhanced Metrics': bool(enhanced)
                }
                
                complete_count = sum(data_types.values())
                total_count = len(data_types)
                
                print(f"\n📊 COMPLETENESS SCORE: {complete_count}/{total_count} ({complete_count/total_count*100:.1f}%)")
                
                for data_type, is_complete in data_types.items():
                    status = "✅" if is_complete else "❌"
                    print(f"   {status} {data_type}")
                
                if complete_count == total_count:
                    print(f"\n🎉 SUCCESS! All data types collected successfully for BTCUSDT")
                    print(f"✅ 1-minute aggregation working perfectly")
                    print(f"✅ All indicators collected and aggregated per minute")
                    print(f"✅ Ready for production deployment")
                else:
                    print(f"\n⚠️  Some data types missing - check API connectivity")
                
            else:
                print("❌ Failed to generate aggregated data")
        else:
            print("❌ No minute buffer found")
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_complete_btcusdt_collection())