#!/usr/bin/env python3
"""Test different periods for long/short ratios"""

import requests
import json

def test_periods():
    """Test different time periods for long/short ratios"""
    
    print("🔍 TESTING LONG/SHORT RATIO PERIODS")
    print("=" * 50)
    
    symbol = "BTCUSDT"
    url = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
    
    periods = ["5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"]
    
    for period in periods:
        print(f"\n📊 Testing period: {period}")
        
        try:
            response = requests.get(url, params={
                "symbol": symbol,
                "period": period,
                "limit": 1
            }, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    print(f"✅ Success! Found {len(data)} records")
                    print(f"   Latest data: {json.dumps(data[0], indent=2)}")
                else:
                    print(f"❌ Empty response")
            else:
                print(f"❌ Failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")

if __name__ == "__main__":
    test_periods()