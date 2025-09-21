#!/usr/bin/env python3
"""
Test slave VM functionality locally
This simulates the full slave VM environment without Docker
"""

import os
import sys
import asyncio
import subprocess
import time
import signal
from pathlib import Path

def setup_local_slave_environment():
    """Setup local environment to simulate slave VM"""
    
    print("🔧 SETTING UP LOCAL SLAVE VM ENVIRONMENT")
    print("=" * 60)
    
    # Set environment variables for local testing
    env_vars = {
        "SLAVE_ID": "local-test-slave",
        "SYMBOLS": "BTC/USDT:USDT,ETH/USDT:USDT",
        "MASTER_URL": "http://localhost:8080",
        "MONGO_URI": "mongodb://localhost:27017/",
        "MONGO_DB_NAME": "local_test_trading_data",
        "MONGO_USERNAME": "",
        "MONGO_PASSWORD": "",
        "MONGO_AUTH_SOURCE": "",
        "TIMEFRAME": "1m",
        "FETCH_INTERVAL": "60",
        "BATCH_SIZE": "2",  # Small batch for testing
        "RATE_LIMIT_DELAY": "0.1",
        "MAX_RETRIES": "3",
        "BINANCE_API_KEY": "",
        "BINANCE_API_SECRET": "",
        "ENABLE_WEBSOCKET": "true",
        "ENABLE_REST_API": "true"
    }
    
    print("📋 Setting environment variables:")
    for key, value in env_vars.items():
        os.environ[key] = value
        print(f"   {key}={value}")
    
    return env_vars

def test_collector_imports():
    """Test if all collector modules can be imported"""
    
    print("\n📦 TESTING COLLECTOR IMPORTS")
    print("-" * 40)
    
    try:
        # Add the collector path
        collector_path = "/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher"
        sys.path.insert(0, collector_path)
        
        print("   📁 Adding collector path to sys.path...")
        print(f"      Path: {collector_path}")
        
        # Test unified collector import
        print("   📦 Importing unified_collector...")
        from unified_collector import UnifiedCollector1M, CollectorConfig
        print("   ✅ unified_collector imported successfully")
        
        # Test run script import
        print("   📦 Importing run_unified_collector...")
        import run_unified_collector
        print("   ✅ run_unified_collector imported successfully")
        
        return UnifiedCollector1M, CollectorConfig
        
    except Exception as e:
        print(f"   ❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def test_collector_initialization():
    """Test collector initialization with local config"""
    
    print("\n🔧 TESTING COLLECTOR INITIALIZATION")
    print("-" * 40)
    
    try:
        # Import collector classes
        collector_path = "/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher"
        sys.path.insert(0, collector_path)
        from unified_collector import UnifiedCollector1M, CollectorConfig
        
        # Create config from environment
        config = CollectorConfig(
            slave_id=os.getenv("SLAVE_ID"),
            symbols=os.getenv("SYMBOLS").split(","),
            mongo_uri=os.getenv("MONGO_URI"),
            mongo_db_name=os.getenv("MONGO_DB_NAME"),
            timeframe=os.getenv("TIMEFRAME"),
            aggregation_interval=int(os.getenv("FETCH_INTERVAL", "60")),
            batch_size=int(os.getenv("BATCH_SIZE", "2"))
        )
        
        print(f"   📋 Config created:")
        print(f"      Slave ID: {config.slave_id}")
        print(f"      Symbols: {config.symbols}")
        print(f"      MongoDB: {config.mongo_uri}")
        print(f"      Batch size: {config.batch_size}")
        
        # Initialize collector
        print("   🔧 Initializing collector...")
        collector = UnifiedCollector1M(config)
        print("   ✅ Collector initialized successfully")
        
        # Test MongoDB connection
        if collector.mongo_available:
            print("   ✅ MongoDB connection: Available")
        else:
            print("   ⚠️  MongoDB connection: Not available (will work without)")
        
        # Test exchange connection
        print("   📡 Testing exchange connection...")
        markets = collector.exchange.loadMarkets()
        print(f"   ✅ Exchange connected: {len(markets)} markets loaded")
        
        return collector
        
    except Exception as e:
        print(f"   ❌ Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return None

async def test_data_collection_cycle(collector, duration=30):
    """Test a short data collection cycle"""
    
    print(f"\n📊 TESTING DATA COLLECTION CYCLE ({duration}s)")
    print("-" * 40)
    
    try:
        # Test with first symbol only
        test_symbol = collector.config.symbols[0]
        print(f"   🎯 Testing with symbol: {test_symbol}")
        
        # Create minute buffer
        buffer = collector.get_or_create_minute_buffer(test_symbol)
        print(f"   📦 Buffer created for minute: {buffer.minute_start}")
        
        # Test individual data fetching
        print("   📡 Testing individual data sources...")
        
        # Test orderbook
        orderbook = await collector._fetch_orderbook_snapshot(test_symbol)
        if orderbook:
            print(f"      ✅ Orderbook: {len(orderbook['bids'])} bids, {len(orderbook['asks'])} asks")
        else:
            print(f"      ❌ Orderbook failed")
        
        # Test trades
        trades = await collector._fetch_recent_trades_for_aggregation(test_symbol)
        if trades:
            print(f"      ✅ Trades: {len(trades)} recent trades")
        else:
            print(f"      ❌ Trades failed")
        
        # Test funding rate
        funding = await collector._fetch_funding_rate(test_symbol)
        if funding:
            print(f"      ✅ Funding rate: {funding.get('current_rate', 0)*100:.4f}%")
        else:
            print(f"      ❌ Funding rate failed")
        
        # Test long/short ratios
        ratios = await collector._fetch_long_short_ratios(test_symbol)
        if ratios:
            print(f"      ✅ Long/short ratios: {len(ratios)} types")
        else:
            print(f"      ❌ Long/short ratios failed")
        
        # Test open interest
        oi = await collector._fetch_open_interest(test_symbol)
        if oi:
            print(f"      ✅ Open interest: {oi.get('open_interest', 0):,.3f}")
        else:
            print(f"      ❌ Open interest failed")
        
        # Test ticker
        ticker = await collector._fetch_ticker(test_symbol)
        if ticker:
            print(f"      ✅ 24h ticker: ${ticker.get('close', 0):,.2f}")
        else:
            print(f"      ❌ 24h ticker failed")
        
        # Test real-time collection for short duration
        print(f"   🔄 Testing real-time collection for {duration} seconds...")
        
        collection_task = asyncio.create_task(collector.collect_real_time_data(test_symbol))
        
        # Monitor for specified duration
        start_time = time.time()
        while time.time() - start_time < duration:
            await asyncio.sleep(5)
            
            current_buffer = collector.minute_buffers.get(test_symbol)
            if current_buffer:
                elapsed = int(time.time() - start_time)
                print(f"      [{elapsed:2d}s] Buffer: {current_buffer.trade_count} trades, "
                      f"{len(current_buffer.orderbook_snapshots)} snapshots")
        
        # Cancel collection task
        collection_task.cancel()
        try:
            await collection_task
        except asyncio.CancelledError:
            pass
        
        # Test aggregation
        print("   📊 Testing 1-minute aggregation...")
        final_buffer = collector.minute_buffers.get(test_symbol)
        if final_buffer:
            aggregated_data = await collector.generate_1m_aggregated_data(test_symbol)
            if aggregated_data:
                print("      ✅ Aggregation successful!")
                
                # Show key metrics
                trade_metrics = aggregated_data.get('trade_metrics', {})
                print(f"         Trades: {trade_metrics.get('count', 0)}")
                print(f"         Volume: {trade_metrics.get('total_volume', 0):.6f} BTC")
                print(f"         VWAP: ${trade_metrics.get('vwap', 0):.2f}")
                
                return True
            else:
                print("      ❌ Aggregation failed")
                return False
        else:
            print("      ❌ No buffer found")
            return False
        
    except Exception as e:
        print(f"   ❌ Collection cycle failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_run_script_execution():
    """Test the run script in a subprocess"""
    
    print("\n🚀 TESTING RUN SCRIPT EXECUTION")
    print("-" * 40)
    
    try:
        # Path to the run script
        script_path = "/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher/run_unified_collector.py"
        
        print(f"   📁 Script path: {script_path}")
        print("   🚀 Starting run script (will run for 10 seconds)...")
        
        # Start the process
        process = subprocess.Popen(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=os.environ,
            cwd="/Users/pj/Desktop/projects/AutoTraderV2/DistributedSystem/SlaveVM/data_fetcher"
        )
        
        # Let it run for 10 seconds
        time.sleep(10)
        
        # Send SIGTERM to gracefully shutdown
        process.terminate()
        
        # Wait for process to finish or force kill after 5 seconds
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
        
        print(f"   📊 Process exit code: {process.returncode}")
        
        if stdout:
            print("   📄 STDOUT (last 10 lines):")
            stdout_lines = stdout.strip().split('\n')
            for line in stdout_lines[-10:]:
                print(f"      {line}")
        
        if stderr:
            print("   ⚠️  STDERR:")
            stderr_lines = stderr.strip().split('\n')
            for line in stderr_lines[-5:]:  # Show last 5 error lines
                print(f"      {line}")
        
        # Check if process started successfully
        if process.returncode in [0, -15]:  # 0 = success, -15 = SIGTERM
            print("   ✅ Run script executed successfully")
            return True
        else:
            print(f"   ❌ Run script failed with exit code {process.returncode}")
            return False
        
    except Exception as e:
        print(f"   ❌ Run script test failed: {e}")
        return False

async def run_full_local_test():
    """Run complete local slave VM test"""
    
    print("🧪 SLAVE VM LOCAL TESTING")
    print("=" * 60)
    print("This test simulates the complete slave VM environment locally")
    print("without Docker to verify all components work correctly.")
    print("")
    
    # Setup environment
    env_vars = setup_local_slave_environment()
    
    # Test imports
    UnifiedCollector1M, CollectorConfig = test_collector_imports()
    if not UnifiedCollector1M:
        print("\n❌ Cannot proceed - import failures")
        return False
    
    # Test initialization
    collector = test_collector_initialization()
    if not collector:
        print("\n❌ Cannot proceed - initialization failures")
        return False
    
    # Test data collection
    collection_success = await test_data_collection_cycle(collector, duration=30)
    
    # Test run script
    run_script_success = test_run_script_execution()
    
    # Final summary
    print("\n📋 LOCAL SLAVE VM TEST SUMMARY")
    print("=" * 60)
    
    tests = [
        ("Environment Setup", True),
        ("Module Imports", UnifiedCollector1M is not None),
        ("Collector Initialization", collector is not None),
        ("Data Collection Cycle", collection_success),
        ("Run Script Execution", run_script_success)
    ]
    
    passed = sum(1 for _, success in tests if success)
    total = len(tests)
    
    for test_name, success in tests:
        status = "✅" if success else "❌"
        print(f"   {status} {test_name}")
    
    print(f"\n📊 RESULTS: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    
    if passed == total:
        print("\n🎉 SUCCESS! Slave VM is ready for local deployment")
        print("✅ All components working correctly")
        print("✅ Data collection functioning")
        print("✅ 1-minute aggregation operational")
        print("✅ Ready for Docker deployment")
    else:
        print(f"\n⚠️  {total-passed} tests failed - check issues above")
    
    return passed == total

if __name__ == "__main__":
    asyncio.run(run_full_local_test())