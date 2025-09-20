#!/usr/bin/env python3
"""Test which symbol formats work with CCXT"""

import ccxt
import asyncio

async def test_symbol_formats():
    """Test different symbol formats"""
    
    exchange = ccxt.binance({
        'sandbox': False,
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })
    
    # Test formats
    formats_to_test = [
        "BTC:USDT",
        "BTCUSDT", 
        "BTC/USDT",
        "BTC/USDT:USDT"
    ]
    
    print("Testing symbol formats with Binance futures...")
    
    for symbol_format in formats_to_test:
        try:
            ticker = exchange.fetchTicker(symbol_format)
            print(f"✅ {symbol_format} -> Price: ${ticker['last']}")
            
            # Test funding rate too
            try:
                funding = exchange.fetchFundingRate(symbol_format)
                print(f"   Funding rate: {funding['fundingRate']}")
            except Exception as e:
                print(f"   Funding rate failed: {e}")
                
        except Exception as e:
            print(f"❌ {symbol_format} -> Error: {e}")
    
    # Load markets to see available symbols
    print(f"\nLoading markets...")
    markets = exchange.loadMarkets()
    
    # Find BTC symbols
    btc_symbols = [symbol for symbol in markets.keys() if 'BTC' in symbol and 'USDT' in symbol][:5]
    print(f"\nAvailable BTC symbols (first 5): {btc_symbols}")

if __name__ == "__main__":
    asyncio.run(test_symbol_formats())