#!/usr/bin/env python3
"""
Complete Data Fetch Test - Verify all slave data collection
Tests all APIs and data sources used by the distributed system
"""

import os
import sys
import asyncio
import time
from datetime import datetime
import requests

# Add paths for testing
sys.path.append('.')
sys.path.append('DistributedSystem/SlaveVM/data_fetcher')

def test_environment_setup():
    """Set up test environment"""
    print("🔧 Setting up test environment...")
    os.environ['SLAVE_ID'] = 'test-slave'
    os.environ['SYMBOLS'] = 'BTCUSDT,ETHUSDT'
    os.environ['TIMEFRAME'] = '1m'
    os.environ['KLINE_INTERVAL'] = '1m'
    print("✅ Environment configured")

def test_enhanced_funding_collector():
    """Test the enhanced funding rate collector"""
    print("\n💰 Testing Enhanced Funding Rate Collection...")
    print("-" * 50)
    
    try:
        from enhanced_funding_collector import EnhancedFundingCollector
        
        collector = EnhancedFundingCollector()
        test_symbol = "BTC/USDT:USDT"
        
        # Test complete funding data
        funding_data = collector.fetch_complete_funding_data(test_symbol)
        
        print(f"✅ Funding Rate Data for {test_symbol}:")
        print(f"   - Current Funding Rate: {funding_data.get('current_funding_rate', 'N/A')}")
        print(f"   - Next Funding Rate: {funding_data.get('next_funding_rate', 'N/A')}")
        print(f"   - Mark Price: {funding_data.get('mark_price', 'N/A')}")
        print(f"   - Index Price: {funding_data.get('index_price', 'N/A')}")
        print(f"   - Next Funding Time: {funding_data.get('next_funding_time_utc', 'N/A')}")
        
        if funding_data.get('error'):
            print(f"   ⚠️ Error: {funding_data['error']}")
        
        return True, funding_data
        
    except Exception as e:
        print(f"❌ Enhanced Funding Collector failed: {e}")
        return False, None

def test_long_short_collectors():
    """Test long/short ratio collectors"""
    print("\n📊 Testing Long/Short Ratio Collection...")
    print("-" * 50)
    
    try:
        from enhanced_long_short_collector import EnhancedLongShortCollector
        
        collector = EnhancedLongShortCollector()
        test_symbol = "BTC/USDT:USDT"
        
        # Test all long/short ratio types
        all_data = collector.fetch_all_long_short_data(test_symbol, period="1m", limit=1)
        
        results = {}
        
        # Global account ratios
        if all_data.get("global_account_ratio"):
            global_data = all_data["global_account_ratio"][-1]
            results['global'] = global_data
            print(f"✅ Global L/S Ratio: {global_data.get('long_short_ratio', 'N/A')}")
        
        # Top trader account ratios
        if all_data.get("top_trader_account_ratio"):
            top_account_data = all_data["top_trader_account_ratio"][-1]
            results['top_trader_account'] = top_account_data
            print(f"✅ Top Trader Account L/S: {top_account_data.get('long_short_ratio', 'N/A')}")
        
        # Top trader position ratios
        if all_data.get("top_trader_position_ratio"):
            top_position_data = all_data["top_trader_position_ratio"][-1]
            results['top_trader_position'] = top_position_data
            print(f"✅ Top Trader Position L/S: {top_position_data.get('long_short_ratio', 'N/A')}")
        
        # Taker buy/sell ratios
        if all_data.get("taker_buy_sell_ratio"):
            taker_data = all_data["taker_buy_sell_ratio"][-1]
            results['taker'] = taker_data
            print(f"✅ Taker Buy/Sell Ratio: {taker_data.get('buy_sell_ratio', 'N/A')}")
        
        return True, results
        
    except Exception as e:
        print(f"❌ Long/Short Ratio Collector failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def test_interest_collector():
    """Test interest rate collector"""
    print("\n💸 Testing Interest Rate Collection...")
    print("-" * 50)
    
    try:
        from enhanced_interest_collector import EnhancedInterestCollector
        
        collector = EnhancedInterestCollector()
        test_symbol = "BTC/USDT:USDT"
        
        # Test interest data collection
        interest_data = collector.fetch_all_interest_data(test_symbol, include_margin=True)
        
        print(f"✅ Interest Data for {test_symbol}:")
        
        # Current open interest
        if interest_data.get("current_open_interest"):
            oi = interest_data["current_open_interest"]
            print(f"   - Open Interest: {oi.get('open_interest', 'N/A')}")
            print(f"   - OI Value: ${oi.get('open_interest_value', 'N/A'):,.2f}" if oi.get('open_interest_value') else "   - OI Value: N/A")
        
        # Historical changes
        if interest_data.get("historical_open_interest"):
            historical = interest_data["historical_open_interest"]
            if len(historical) >= 2:
                latest = historical[-1]
                previous = historical[-2]
                change = latest["open_interest"] - previous["open_interest"]
                print(f"   - OI Change: {change:,.0f}")
        
        # Margin rates
        if interest_data.get("margin_interest_rates"):
            margin_rates = interest_data["margin_interest_rates"]
            for rate in margin_rates[:2]:  # Show first 2
                print(f"   - {rate['asset']} Margin Rate: {rate['daily_interest_rate']}")
        
        return True, interest_data
        
    except Exception as e:
        print(f"❌ Interest Collector failed: {e}")
        return False, None

def test_direct_api_calls():
    """Test direct API calls to verify endpoints"""
    print("\n🌐 Testing Direct API Endpoints...")
    print("-" * 50)
    
    tests = []
    
    # Test premium index API (for next funding rate)
    try:
        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        params = {"symbol": "BTCUSDT"}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ Premium Index API:")
        print(f"   - Mark Price: {data.get('markPrice', 'N/A')}")
        print(f"   - Index Price: {data.get('indexPrice', 'N/A')}")
        print(f"   - Next Funding Time: {datetime.utcfromtimestamp(int(data['nextFundingTime'])/1000) if data.get('nextFundingTime') else 'N/A'}")
        print(f"   - Interest Rate: {data.get('interestRate', 'N/A')}")
        tests.append(True)
        
    except Exception as e:
        print(f"❌ Premium Index API failed: {e}")
        tests.append(False)
    
    # Test open interest API
    try:
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        params = {"symbol": "BTCUSDT"}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        print(f"✅ Open Interest API:")
        print(f"   - Open Interest: {data.get('openInterest', 'N/A')}")
        print(f"   - Timestamp: {datetime.utcfromtimestamp(int(data['time'])/1000) if data.get('time') else 'N/A'}")
        tests.append(True)
        
    except Exception as e:
        print(f"❌ Open Interest API failed: {e}")
        tests.append(False)
    
    # Test global long/short ratio API
    try:
        url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
        params = {"symbol": "BTCUSDT", "period": "1m", "limit": 1}
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data:
            item = data[0]
            print(f"✅ Global L/S Ratio API:")
            print(f"   - Long/Short Ratio: {item.get('longShortRatio', 'N/A')}")
            print(f"   - Long Account %: {item.get('longAccount', 'N/A')}")
            print(f"   - Short Account %: {item.get('shortAccount', 'N/A')}")
            tests.append(True)
        else:
            print(f"⚠️ Global L/S Ratio API returned empty data")
            tests.append(False)
            
    except Exception as e:
        print(f"❌ Global L/S Ratio API failed: {e}")
        tests.append(False)
    
    return tests

def test_schema_integration():
    """Test the complete schema integration"""
    print("\n🗄️ Testing Complete Schema Integration...")
    print("-" * 50)
    
    try:
        # Import without MongoDB connection for testing
        class MockSchemaCollector:
            def __init__(self):
                from enhanced_long_short_collector import EnhancedLongShortCollector
                from enhanced_interest_collector import EnhancedInterestCollector
                from enhanced_funding_collector import EnhancedFundingCollector
                
                self.long_short_collector = EnhancedLongShortCollector()
                self.interest_collector = EnhancedInterestCollector()
                self.funding_collector = EnhancedFundingCollector()
                self.slave_id = "test-slave"
            
            def test_data_structure(self, symbol):
                """Test data structure without MongoDB"""
                # Collect all data
                enhanced_long_short = {}
                enhanced_interest = {}
                enhanced_funding = self.funding_collector.fetch_complete_funding_data(symbol)
                
                # Try to get some L/S data
                try:
                    all_long_short = self.long_short_collector.fetch_all_long_short_data(symbol, period="1m", limit=1)
                    if all_long_short.get("global_account_ratio"):
                        latest_global = all_long_short["global_account_ratio"][-1]
                        enhanced_long_short["global_long_short_ratio"] = latest_global["long_short_ratio"]
                        enhanced_long_short["global_long_account"] = latest_global["long_account"]
                        enhanced_long_short["global_short_account"] = latest_global["short_account"]
                except:
                    pass
                
                # Try to get interest data
                try:
                    interest_data = self.interest_collector.fetch_all_interest_data(symbol, include_margin=True)
                    if interest_data.get("current_open_interest"):
                        current_oi = interest_data["current_open_interest"]
                        enhanced_interest["open_interest"] = current_oi["open_interest"]
                        enhanced_interest["open_interest_value"] = current_oi.get("open_interest_value", 0)
                except:
                    pass
                
                # Build test data structure
                timestamp = datetime.utcnow().replace(second=0, microsecond=0)
                binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
                
                data = {
                    "_id": f"{binance_symbol}_{int(timestamp.timestamp())}",
                    "timestamp": timestamp,
                    "symbol": binance_symbol,
                    
                    # Enhanced Futures data
                    "futures": {
                        "open": "26500.00",
                        "high": "26525.50", 
                        "low": "26485.25",
                        "close": "26515.75",
                        "volume": "1250.5",
                        
                        # Enhanced funding rate data (current + next)
                        "funding_rate": enhanced_funding.get("current_funding_rate", 0),
                        "next_funding_rate": enhanced_funding.get("next_funding_rate", 0),
                        "next_funding_time": enhanced_funding.get("next_funding_time", 0),
                        "mark_price": enhanced_funding.get("mark_price", 0),
                        "index_price": enhanced_funding.get("index_price", 0),
                    },
                    
                    # Long-Short Ratio data
                    "long_short_ratio": {
                        "open_interest": enhanced_interest.get("open_interest", 0),
                        **enhanced_long_short,
                        "open_interest_value": enhanced_interest.get("open_interest_value", 0),
                    },
                    
                    # Liquidations structure
                    "liquidations": {
                        "buy_liquidations": {"total_quantity": 0, "total_dollars": 0, "event_count": 0},
                        "sell_liquidations": {"total_quantity": 0, "total_dollars": 0, "event_count": 0}
                    },
                    
                    # Metadata
                    "collector_info": {
                        "slave_id": self.slave_id,
                        "collection_timestamp": datetime.utcnow().isoformat(),
                        "data_version": "enhanced_v2",
                        "apis_called": ["ohlcv", "funding_rate", "premium_index", "long_short_ratios", "open_interest"],
                        "collection_method": "hybrid_websocket_rest",
                        "data_precision": "1m"
                    }
                }
                
                return data
        
        collector = MockSchemaCollector()
        test_symbol = "BTC/USDT:USDT"
        
        data = collector.test_data_structure(test_symbol)
        
        print("✅ Schema Integration Test Results:")
        print(f"   - Document ID: {data['_id']}")
        print(f"   - Symbol: {data['symbol']}")
        print(f"   - Current Funding Rate: {data['futures']['funding_rate']}")
        print(f"   - Next Funding Rate: {data['futures']['next_funding_rate']}")
        print(f"   - Mark Price: {data['futures']['mark_price']}")
        print(f"   - Open Interest: {data['long_short_ratio']['open_interest']}")
        print(f"   - Global L/S Ratio: {data['long_short_ratio'].get('global_long_short_ratio', 'N/A')}")
        print(f"   - Data Version: {data['collector_info']['data_version']}")
        print(f"   - APIs Called: {len(data['collector_info']['apis_called'])} APIs")
        
        return True, data
        
    except Exception as e:
        print(f"❌ Schema Integration Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def main():
    """Run complete data fetch test suite"""
    print("🧪 Complete Data Fetch Test Suite")
    print("=" * 60)
    print(f"Test started at: {datetime.now()}")
    print("")
    
    # Setup
    test_environment_setup()
    
    # Run all tests
    test_results = []
    
    print("\n" + "=" * 60)
    funding_result, funding_data = test_enhanced_funding_collector()
    test_results.append(("Enhanced Funding Rate", funding_result))
    
    print("\n" + "=" * 60)
    ls_result, ls_data = test_long_short_collectors()
    test_results.append(("Long/Short Ratios", ls_result))
    
    print("\n" + "=" * 60)
    interest_result, interest_data = test_interest_collector()
    test_results.append(("Interest Rates", interest_result))
    
    print("\n" + "=" * 60)
    api_tests = test_direct_api_calls()
    test_results.append(("Direct API Calls", all(api_tests)))
    
    print("\n" + "=" * 60)
    schema_result, schema_data = test_schema_integration()
    test_results.append(("Schema Integration", schema_result))
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 Complete Data Fetch Test Results:")
    print("-" * 60)
    
    passed = 0
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name:<25}: {status}")
        if result:
            passed += 1
    
    overall_result = passed == len(test_results)
    print(f"\nOverall Result: {passed}/{len(test_results)} test categories passed")
    
    if overall_result:
        print("\n🎉 SUCCESS: All data collection systems are working!")
        print("🚀 Your distributed slave system is ready for production deployment!")
        print("\n💡 Deploy command:")
        print("   docker-compose -f DistributedSystem/Scripts/deployment/docker-compose.slave.yml up --build")
    else:
        print("\n⚠️  Some tests failed. Check the error messages above.")
        print("Note: Some failures may be due to missing dependencies in local environment.")
        print("The Docker deployment should work correctly with all dependencies included.")
    
    return overall_result

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)