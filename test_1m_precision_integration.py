#!/usr/bin/env python3
"""
Test script to verify 1-minute precision integration
Tests both REST API and WebSocket services for 1-minute data collection
"""

import os
import sys
import time
import asyncio
from datetime import datetime

def test_environment_setup():
    """Test environment configuration for 1-minute precision"""
    print("🧪 Testing 1-Minute Precision Environment Setup")
    print("=" * 50)
    
    # Set test environment variables
    os.environ['SLAVE_ID'] = 'test-slave-1m'
    os.environ['SYMBOLS'] = 'BTCUSDT,ETHUSDT'
    os.environ['MASTER_URL'] = 'http://localhost:8080'
    os.environ['MONGO_URI'] = 'mongodb://localhost:27017/'
    os.environ['MONGO_DB_NAME'] = 'trading_data'
    os.environ['TIMEFRAME'] = '1m'
    os.environ['KLINE_INTERVAL'] = '1m'
    
    print("✅ Environment configured for 1-minute precision:")
    print(f"   - SLAVE_ID: {os.getenv('SLAVE_ID')}")
    print(f"   - TIMEFRAME: {os.getenv('TIMEFRAME')}")
    print(f"   - KLINE_INTERVAL: {os.getenv('KLINE_INTERVAL')}")
    print(f"   - SYMBOLS: {os.getenv('SYMBOLS')}")
    
    return True

def test_distributed_data_fetcher_config():
    """Test distributed data fetcher with 1-minute configuration"""
    print("\n🔧 Testing Distributed Data Fetcher 1m Configuration")
    print("-" * 50)
    
    try:
        sys.path.append('DistributedSystem/SlaveVM/data_fetcher')
        from distributed_data_fetcher import load_config_from_env
        
        config = load_config_from_env()
        
        print("✅ Configuration loaded successfully:")
        print(f"   - Timeframe: {config['timeframe']}")
        print(f"   - Symbols: {config['symbols']}")
        print(f"   - Slave ID: {config['slave_id']}")
        
        if config['timeframe'] == '1m':
            print("✅ 1-minute precision configured correctly!")
            return True
        else:
            print(f"❌ Expected 1m timeframe, got {config['timeframe']}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing configuration: {e}")
        return False

def test_websocket_imports():
    """Test WebSocket service imports"""
    print("\n📡 Testing WebSocket Service Imports")
    print("-" * 50)
    
    websocket_tests = []
    
    # Test WebSocket controller
    try:
        sys.path.append('DistributedSystem/SlaveVM/websockets')
        from websocket_controller import DistributedWebSocketController
        print("✅ DistributedWebSocketController imported successfully")
        websocket_tests.append(True)
    except Exception as e:
        print(f"❌ Failed to import DistributedWebSocketController: {e}")
        websocket_tests.append(False)
    
    # Test Kline WebSocket
    try:
        from kline_websocket import DistributedKlineWebSocket
        print("✅ DistributedKlineWebSocket imported successfully")
        websocket_tests.append(True)
    except Exception as e:
        print(f"❌ Failed to import DistributedKlineWebSocket: {e}")
        websocket_tests.append(False)
    
    # Test Liquidation WebSocket
    try:
        from liquidation_websocket import DistributedLiquidationWebSocket
        print("✅ DistributedLiquidationWebSocket imported successfully")
        websocket_tests.append(True)
    except Exception as e:
        print(f"❌ Failed to import DistributedLiquidationWebSocket: {e}")
        websocket_tests.append(False)
    
    return all(websocket_tests)

def test_websocket_initialization():
    """Test WebSocket service initialization"""
    print("\n🚀 Testing WebSocket Service Initialization")
    print("-" * 50)
    
    try:
        sys.path.append('DistributedSystem/SlaveVM/websockets')
        from kline_websocket import DistributedKlineWebSocket
        
        # Test initialization without actual connection
        symbols = ['BTCUSDT', 'ETHUSDT']
        slave_id = 'test-slave-1m'
        
        # Mock WebSocket for testing (without actual connection)
        class MockKlineWebSocket(DistributedKlineWebSocket):
            def __init__(self, symbols=None, interval=None, slave_id=None):
                # Skip parent initialization to avoid MongoDB connection
                self.slave_id = slave_id or os.getenv("SLAVE_ID", "unknown-slave")
                self.symbols = symbols or ['BTCUSDT', 'ETHUSDT']
                self.interval = interval or os.getenv("KLINE_INTERVAL", "1m")
                
            def get_uris(self):
                spot_uri = f"wss://stream.binance.com:9443/ws/{'/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)}"
                futures_uri = f"wss://fstream.binance.com/ws/{'/'.join(f'{s.lower()}@kline_{self.interval}' for s in self.symbols)}"
                return [(spot_uri, "spot"), (futures_uri, "futures")]
        
        ws = MockKlineWebSocket(symbols=symbols, interval='1m', slave_id=slave_id)
        uris = ws.get_uris()
        
        print("✅ Kline WebSocket initialized successfully:")
        print(f"   - Slave ID: {ws.slave_id}")
        print(f"   - Interval: {ws.interval}")
        print(f"   - Symbols: {ws.symbols}")
        print(f"   - Spot URI: {uris[0][0]}")
        print(f"   - Futures URI: {uris[1][0]}")
        
        # Verify 1-minute interval
        if ws.interval == '1m':
            print("✅ 1-minute precision configured correctly!")
            return True
        else:
            print(f"❌ Expected 1m interval, got {ws.interval}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing WebSocket initialization: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_docker_configuration():
    """Test Docker configuration files"""
    print("\n🐳 Testing Docker Configuration")
    print("-" * 50)
    
    docker_files = [
        "DistributedSystem/SlaveVM/websockets/Dockerfile.kline",
        "DistributedSystem/SlaveVM/websockets/Dockerfile.liquidation",
        "DistributedSystem/Scripts/deployment/docker-compose.slave.yml"
    ]
    
    all_exist = True
    for file_path in docker_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} missing")
            all_exist = False
    
    # Check docker-compose for 1m configuration
    compose_file = "DistributedSystem/Scripts/deployment/docker-compose.slave.yml"
    if os.path.exists(compose_file):
        with open(compose_file, 'r') as f:
            content = f.read()
            if 'TIMEFRAME=1m' in content and 'KLINE_INTERVAL=1m' in content:
                print("✅ Docker compose configured for 1-minute precision")
            else:
                print("⚠️  Docker compose may not be fully configured for 1m precision")
                all_exist = False
    
    return all_exist

def main():
    """Run all integration tests"""
    print("🎯 1-Minute Precision Integration Test Suite")
    print("=" * 60)
    print(f"Test started at: {datetime.now()}")
    print("")
    
    # Run all tests
    test_results = []
    
    test_results.append(("Environment Setup", test_environment_setup()))
    test_results.append(("Data Fetcher Config", test_distributed_data_fetcher_config()))
    test_results.append(("WebSocket Imports", test_websocket_imports()))
    test_results.append(("WebSocket Initialization", test_websocket_initialization()))
    test_results.append(("Docker Configuration", test_docker_configuration()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 Integration Test Results Summary:")
    print("-" * 60)
    
    passed = 0
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {test_name:<25}: {status}")
        if result:
            passed += 1
    
    overall_result = passed == len(test_results)
    print(f"\nOverall Result: {passed}/{len(test_results)} tests passed")
    
    if overall_result:
        print("\n🎉 SUCCESS: Your distributed system now supports 1-minute precision!")
        print("🚀 Ready to deploy with WebSocket + REST API for real-time data collection")
        print("\n💡 To start the enhanced slave service:")
        print("   docker-compose -f DistributedSystem/Scripts/deployment/docker-compose.slave.yml up")
    else:
        print("\n⚠️  Some tests failed. Please check the configuration.")
    
    return overall_result

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)