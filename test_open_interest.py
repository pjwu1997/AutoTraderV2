#!/usr/bin/env python3
"""Test Open Interest data availability"""

import ccxt
import requests
import json

def test_open_interest():
    """Test if we can get open interest data"""
    
    print("🔍 Testing Open Interest Data Sources")
    print("=" * 40)
    
    # Test CCXT method
    print("\n1️⃣ Testing CCXT fetchOpenInterest...")
    try:
        exchange = ccxt.binance({
            'sandbox': False,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        # Check if exchange supports open interest
        if hasattr(exchange, 'fetchOpenInterest'):
            try:
                oi = exchange.fetchOpenInterest('BTC/USDT:USDT')
                print(f"✅ CCXT Open Interest: {oi}")
            except Exception as e:
                print(f"❌ CCXT fetchOpenInterest failed: {e}")
        else:
            print("❌ CCXT doesn't support fetchOpenInterest for Binance")
            
    except Exception as e:
        print(f"❌ CCXT setup failed: {e}")
    
    # Test Binance REST API directly
    print("\n2️⃣ Testing Binance REST API...")
    try:
        # Binance Futures Open Interest endpoint
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        response = requests.get(url, params={"symbol": "BTCUSDT"}, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Binance API Open Interest: {json.dumps(data, indent=2)}")
        else:
            print(f"❌ Binance API failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Binance API error: {e}")
    
    # Test Open Interest Statistics
    print("\n3️⃣ Testing Open Interest Statistics...")
    try:
        url = "https://fapi.binance.com/futures/data/openInterestHist"
        response = requests.get(url, params={
            "symbol": "BTCUSDT",
            "period": "1m",
            "limit": 1
        }, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Open Interest History: {json.dumps(data, indent=2)}")
        else:
            print(f"❌ OI History failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ OI History error: {e}")
    
    # Test all available symbols OI
    print("\n4️⃣ Testing Bulk Open Interest...")
    try:
        url = "https://fapi.binance.com/fapi/v1/openInterest"
        # Get without symbol to see if it returns all
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print("✅ Bulk OI endpoint works")
        else:
            print(f"❌ Bulk OI failed: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Bulk OI error: {e}")
    
    print("\n📊 Summary:")
    print("Open Interest can be collected from:")
    print("• Binance /fapi/v1/openInterest - Current OI")
    print("• Binance /futures/data/openInterestHist - Historical OI")
    print("• Should be added to unified collector")

if __name__ == "__main__":
    test_open_interest()