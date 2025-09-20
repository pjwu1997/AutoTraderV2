#!/usr/bin/env python3
"""
Generate example schema output from the unified collector
"""

import json
import time
from datetime import datetime

def generate_example_schema():
    """Generate example output schema from unified collector"""
    
    # Main REST API collection schema
    rest_api_schema = {
        "symbol": "BTC/USDT:USDT",
        "timestamp": int(time.time() * 1000),  # Current timestamp in milliseconds
        "slave_id": "slave-1",
        "collection_type": "unified_market_data",
        
        # OHLCV Data (last 100 candles)
        "ohlcv": [
            [
                1726617600000,  # timestamp
                115000.0,       # open
                115500.0,       # high  
                114800.0,       # low
                115200.0,       # close
                125.5           # volume
            ],
            # ... up to 100 candles
        ],
        
        # Orderbook Data
        "orderbook": {
            "bids": [
                [115180.0, 0.5],    # [price, quantity]
                [115175.0, 1.2],
                [115170.0, 0.8],
                # ... up to 20 levels
            ],
            "asks": [
                [115185.0, 0.3],    # [price, quantity] 
                [115190.0, 0.9],
                [115195.0, 1.1],
                # ... up to 20 levels
            ],
            "timestamp": 1726617650000
        },
        
        # Recent Trades
        "trades": [
            {
                "price": 115182.5,
                "amount": 0.15,
                "side": "buy",
                "timestamp": 1726617640000
            },
            {
                "price": 115180.0,
                "amount": 0.08,
                "side": "sell", 
                "timestamp": 1726617641000
            },
            # ... up to 50 recent trades
        ],
        
        # Funding Rate Data
        "funding_rate": {
            "current_rate": 0.00006299,           # Current funding rate
            "current_timestamp": 1726617600000,    # Current funding timestamp
            "next_timestamp": 1726646400000,       # Next funding timestamp (+8 hours)
            "mark_price": 115182.5,               # Mark price
            "index_price": 115181.8,              # Index price
            "estimated_settle_price": 115182.1    # Estimated settlement price
        },
        
        # Long/Short Ratios
        "long_short_ratios": {
            "global_account_ratio": {
                "longShortRatio": 2.45,           # Global long/short ratio
                "longAccount": 0.71,              # Long account percentage
                "shortAccount": 0.29,             # Short account percentage  
                "timestamp": 1726617600000
            },
            "top_trader_ratio": {
                "longShortRatio": 1.85,           # Top trader long/short ratio
                "longAccount": 0.65,              # Top trader long percentage
                "shortAccount": 0.35,             # Top trader short percentage
                "timestamp": 1726617600000
            },
            "top_position_ratio": {
                "longShortRatio": 1.92,           # Top position long/short ratio  
                "longPosition": 0.66,             # Long position percentage
                "shortPosition": 0.34,            # Short position percentage
                "timestamp": 1726617600000
            }
        },
        
        # 24hr Ticker Statistics
        "ticker_24h": {
            "open": 114500.0,                    # 24h open price
            "high": 116200.0,                    # 24h high price
            "low": 114200.0,                     # 24h low price
            "close": 115182.5,                   # Current close price
            "volume": 1250.75,                   # 24h base volume
            "quote_volume": 144_125_000.0,       # 24h quote volume (USDT)
            "change": 682.5,                     # 24h price change
            "percentage": 0.596,                 # 24h percentage change
            "vwap": 115091.2                     # Volume weighted average price
        },
        
        # Open Interest Data
        "open_interest": {
            "open_interest": 88586.541,          # Current open interest amount
            "timestamp": 1726617600000           # OI data timestamp
        },
        
        # Enhanced Calculated Metrics
        "enhanced_metrics": {
            "cvd": 12.5,                         # Cumulative Volume Delta
            "buy_sell_ratio": 1.35,              # Buy vs sell volume ratio
            "spread": 5.0,                       # Best bid-ask spread
            "spread_percentage": 0.0043,         # Spread as percentage
            "volatility": 0.024                  # Price volatility (20-period)
        }
    }
    
    # WebSocket kline data schema
    websocket_kline_schema = {
        "symbol": "BTC/USDT:USDT",
        "timestamp": 1726617660000,
        "open": 115180.0,
        "high": 115185.0,
        "low": 115175.0,
        "close": 115182.5,
        "volume": 1.25,
        "quote_volume": 144_000.0,
        "trades": 45,
        "is_closed": True,                       # Whether kline is finalized
        "slave_id": "slave-1", 
        "data_source": "websocket"
    }
    
    # WebSocket liquidation data schema  
    websocket_liquidation_schema = {
        "symbol": "BTC/USDT:USDT",
        "timestamp": 1726617665000,
        "side": "SELL",                          # Liquidation side (BUY/SELL)
        "order_type": "MARKET",                  # Order type
        "time_in_force": "IOC",                  # Time in force
        "quantity": 0.15,                        # Liquidated quantity
        "price": 115150.0,                       # Liquidation price
        "average_price": 115148.5,               # Average execution price
        "execution_type": "TRADE",               # Execution type
        "order_status": "FILLED",                # Order status
        "slave_id": "slave-1",
        "data_source": "websocket"
    }
    
    return {
        "rest_api_market_data": rest_api_schema,
        "websocket_kline_data": websocket_kline_schema, 
        "websocket_liquidation_data": websocket_liquidation_schema
    }

def print_schema_summary():
    """Print a summary of all data collected"""
    
    print("📊 UNIFIED COLLECTOR OUTPUT SCHEMA")
    print("=" * 50)
    
    print("\n🔄 REST API DATA (every 60 seconds)")
    print("-" * 30)
    print("• OHLCV: 100 candles × [timestamp, open, high, low, close, volume]")
    print("• Orderbook: 20 bid/ask levels × [price, quantity]") 
    print("• Trades: 50 recent trades × [price, amount, side, timestamp]")
    print("• Funding: current_rate, next_timestamp, mark_price, index_price")
    print("• Long/Short Ratios: global, top_trader, top_position")
    print("• 24h Ticker: OHLC, volume, change%, VWAP")
    print("• Open Interest: current amount, timestamp")
    print("• Enhanced Metrics: CVD, spread, volatility, buy/sell ratio")
    
    print("\n⚡ WEBSOCKET DATA (real-time)")
    print("-" * 30)
    print("• Kline Streams: Real-time 1m candles")
    print("• Liquidations: Live liquidation events")
    
    print("\n💾 STORAGE COLLECTIONS")
    print("-" * 30)
    print("• {symbol}_market_data: Complete REST API data")
    print("• kline_data: Real-time kline updates")
    print("• liquidations: Real-time liquidation events")
    
    print("\n📋 METADATA")
    print("-" * 30)
    print("• symbol: Trading pair (e.g., BTC/USDT:USDT)")
    print("• timestamp: Collection time (milliseconds)")
    print("• slave_id: Which slave collected the data") 
    print("• data_source: 'rest_api' or 'websocket'")
    print("• collection_type: 'unified_market_data'")

if __name__ == "__main__":
    # Generate example schemas
    schemas = generate_example_schema()
    
    print("🔍 EXAMPLE OUTPUT SCHEMAS")
    print("=" * 50)
    
    print("\n1️⃣ REST API MARKET DATA")
    print("-" * 25)
    print(json.dumps(schemas["rest_api_market_data"], indent=2)[:1000] + "...")
    
    print("\n2️⃣ WEBSOCKET KLINE DATA")
    print("-" * 25)
    print(json.dumps(schemas["websocket_kline_data"], indent=2))
    
    print("\n3️⃣ WEBSOCKET LIQUIDATION DATA") 
    print("-" * 25)
    print(json.dumps(schemas["websocket_liquidation_data"], indent=2))
    
    print("\n" + "=" * 50)
    print_schema_summary()