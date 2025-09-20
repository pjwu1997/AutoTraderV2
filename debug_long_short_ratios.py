#!/usr/bin/env python3
"""Debug long/short ratios API calls"""

import requests
import json

def debug_long_short_ratios():
    """Debug why long/short ratios are failing"""
    
    print("🔍 DEBUGGING LONG/SHORT RATIOS API")
    print("=" * 50)
    
    symbol = "BTCUSDT"
    base_url = "https://fapi.binance.com"
    
    endpoints = [
        ("Global Account Ratio", f"{base_url}/futures/data/globalLongShortAccountRatio"),
        ("Top Trader Ratio", f"{base_url}/futures/data/topLongShortAccountRatio"),
        ("Top Position Ratio", f"{base_url}/futures/data/topLongShortPositionRatio")
    ]
    
    for name, url in endpoints:
        print(f"\n📊 Testing {name}...")
        print(f"URL: {url}")
        
        try:
            response = requests.get(url, params={
                "symbol": symbol,
                "period": "1m",
                "limit": 1
            }, timeout=10)
            
            print(f"Status: {response.status_code}")
            print(f"Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Success! Data: {json.dumps(data, indent=2)}")
            else:
                print(f"❌ Failed with status {response.status_code}")
                print(f"Response: {response.text}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
    
    # Test without period parameter
    print(f"\n🔧 Testing without period parameter...")
    try:
        url = f"{base_url}/futures/data/globalLongShortAccountRatio"
        response = requests.get(url, params={
            "symbol": symbol,
            "limit": 1
        }, timeout=10)
        
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success without period! Data: {json.dumps(data, indent=2)}")
        else:
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    debug_long_short_ratios()