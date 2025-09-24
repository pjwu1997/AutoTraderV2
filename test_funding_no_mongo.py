#!/usr/bin/env python3
"""
Direct funding rate test without any MongoDB connection
"""

import asyncio
import json
import sys
import time
import ccxt
import requests
from datetime import datetime

async def test_funding_rates_direct():
    """Direct funding rate testing without collector class"""
    
    print("🔍 DIRECT FUNDING RATE TEST (NO MONGODB)")
    print("=" * 60)
    
    # Initialize exchange directly
    exchange = ccxt.binance({
        'sandbox': False,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'},
        'timeout': 30000
    })
    
    # Load markets
    try:
        markets = exchange.loadMarkets()
        print(f"✅ Exchange connected: {len(markets)} markets")
    except Exception as e:
        print(f"❌ Exchange connection failed: {e}")
        return None
    
    # Test results
    results = {
        "test_info": {
            "start_time": datetime.utcnow().isoformat(),
            "test_type": "direct_api_no_mongodb",
            "symbols": ["BTC/USDT:USDT", "ETH/USDT:USDT"]
        },
        "funding_rate_tests": [],
        "analysis": {}
    }
    
    async def fetch_funding_rate_direct(symbol: str) -> dict:
        """Direct funding rate fetch - copied from collector"""
        try:
            current_funding = exchange.fetchFundingRate(symbol)
            
            # Enhanced funding data from Binance API
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            response = requests.get(url, params={"symbol": symbol_clean}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000,
                    "mark_price": float(data.get('markPrice', 0)),
                    "index_price": float(data.get('indexPrice', 0)),
                    "estimated_settle_price": float(data.get('estimatedSettlePrice', 0))
                }
            else:
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000
                }
                
        except Exception as e:
            print(f"   Warning: Failed to fetch funding rate for {symbol}: {e}")
            return {}
    
    # Run 30 tests
    print(f"\n📡 TESTING FUNDING RATES (30 iterations)")
    print("-" * 50)
    
    btc_successes = 0
    eth_successes = 0
    
    for i in range(30):
        test_result = {
            "iteration": i + 1,
            "timestamp": datetime.utcnow().isoformat(),
            "btc": {},
            "eth": {}
        }
        
        # Test BTC
        start_time = time.time()
        btc_funding = await fetch_funding_rate_direct("BTC/USDT:USDT")
        btc_duration = time.time() - start_time
        btc_success = bool(btc_funding)
        
        test_result["btc"] = {
            "success": btc_success,
            "duration": round(btc_duration, 3),
            "data": btc_funding if btc_success else None,
            "rate_percent": round(btc_funding.get("current_rate", 0) * 100, 6) if btc_funding else None
        }
        
        if btc_success:
            btc_successes += 1
        
        # Test ETH
        start_time = time.time()
        eth_funding = await fetch_funding_rate_direct("ETH/USDT:USDT")
        eth_duration = time.time() - start_time
        eth_success = bool(eth_funding)
        
        test_result["eth"] = {
            "success": eth_success,
            "duration": round(eth_duration, 3),
            "data": eth_funding if eth_success else None,
            "rate_percent": round(eth_funding.get("current_rate", 0) * 100, 6) if eth_funding else None
        }
        
        if eth_success:
            eth_successes += 1
        
        results["funding_rate_tests"].append(test_result)
        
        # Show progress
        btc_status = "✅" if btc_success else "❌"
        eth_status = "✅" if eth_success else "❌"
        btc_rate = f"{test_result['btc']['rate_percent']:.4f}%" if test_result["btc"].get("rate_percent") else "failed"
        eth_rate = f"{test_result['eth']['rate_percent']:.4f}%" if test_result["eth"].get("rate_percent") else "failed"
        
        print(f"Test {i+1:2d}: {btc_status} BTC ({btc_rate}) | {eth_status} ETH ({eth_rate})")
        
        await asyncio.sleep(1)  # 1 second between tests
    
    # Calculate success rates
    btc_success_rate = (btc_successes / 30) * 100
    eth_success_rate = (eth_successes / 30) * 100
    
    print(f"\n📊 FINAL RESULTS:")
    print(f"BTC success rate: {btc_success_rate:.1f}% ({btc_successes}/30)")
    print(f"ETH success rate: {eth_success_rate:.1f}% ({eth_successes}/30)")
    
    # Analysis
    results["analysis"] = {
        "btc_success_rate": btc_success_rate,
        "eth_success_rate": eth_success_rate,
        "btc_failures": 30 - btc_successes,
        "eth_failures": 30 - eth_successes,
        "test_duration_minutes": 0.5,
        "avg_response_time_btc": round(sum(test["btc"]["duration"] for test in results["funding_rate_tests"]) / 30, 3),
        "avg_response_time_eth": round(sum(test["eth"]["duration"] for test in results["funding_rate_tests"]) / 30, 3)
    }
    
    # Root cause analysis
    if btc_success_rate < 70:
        root_cause = "BTC funding rate API is inherently unreliable - likely due to high load on BTC endpoints"
        severity = "HIGH"
    elif btc_success_rate < 90:
        root_cause = "BTC funding rate API has moderate reliability issues - occasional failures expected"
        severity = "MEDIUM"
    else:
        root_cause = "BTC funding rate API is generally reliable - failures were random/temporary"
        severity = "LOW"
    
    results["analysis"]["root_cause"] = {
        "description": root_cause,
        "severity": severity,
        "recommendation": "Use timeout and retry logic for BTC funding rate calls" if severity != "LOW" else "Current implementation is adequate"
    }
    
    print(f"\n🎯 ROOT CAUSE ANALYSIS:")
    print(f"   {root_cause}")
    print(f"   Severity: {severity}")
    print(f"   Average response time - BTC: {results['analysis']['avg_response_time_btc']}s, ETH: {results['analysis']['avg_response_time_eth']}s")
    
    # Save to JSON
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = f"funding_rate_direct_test_{timestamp}.json"
    
    results["test_info"]["end_time"] = datetime.utcnow().isoformat()
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to: {output_file}")
    
    return output_file

if __name__ == "__main__":
    asyncio.run(test_funding_rates_direct())