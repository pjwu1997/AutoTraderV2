#!/usr/bin/env python3
"""
Schema example for 1-minute aggregated unified collector output
"""

import json
import time
from datetime import datetime

def generate_1m_aggregated_schema():
    """Generate example 1-minute aggregated schema"""
    
    current_time = int(time.time() * 1000)
    minute_start = (current_time // 60000) * 60000  # Round down to minute boundary
    
    schema = {
        "symbol": "BTC/USDT:USDT",
        "timestamp": minute_start,                    # Minute start timestamp
        "minute_end": minute_start + 60000,           # Minute end timestamp (+1 minute)
        "slave_id": "slave-1",
        "collection_type": "unified_market_data_1m",
        
        # 1-minute OHLCV candle (from exchange)
        "ohlcv": [
            minute_start,         # timestamp
            117600.0,            # open
            117650.0,            # high
            117580.0,            # low
            117635.0,            # close
            45.267               # volume
        ],
        
        # Current orderbook state (last snapshot of the minute)
        "orderbook": {
            "bids": [
                [117634.5, 2.5],     # [price, quantity]
                [117634.0, 1.8],
                [117633.5, 0.9]
                # ... up to 10 levels
            ],
            "asks": [
                [117635.0, 1.2],     # [price, quantity]
                [117635.5, 2.1],
                [117636.0, 1.5]
                # ... up to 10 levels
            ],
            "timestamp": minute_start + 59000  # Near end of minute
        },
        
        # 1-minute aggregated orderbook metrics
        "orderbook_metrics": {
            "avg_spread": 0.15,              # Average spread during the minute
            "min_spread": 0.10,              # Minimum spread
            "max_spread": 0.25,              # Maximum spread
            "avg_bid_depth": 15.7,           # Average bid depth (top 10 levels)
            "avg_ask_depth": 12.3,           # Average ask depth (top 10 levels)
            "snapshot_count": 12             # Number of orderbook snapshots taken
        },
        
        # 1-minute aggregated trade metrics
        "trade_metrics": {
            "count": 127,                    # Total trades in the minute
            "total_volume": 8.547,           # Total volume traded
            "buy_volume": 4.821,             # Total buy volume
            "sell_volume": 3.726,            # Total sell volume
            "vwap": 117618.7,                # Volume-weighted average price
            "buy_sell_ratio": 1.294          # Buy volume / sell volume ratio
        },
        
        # 1-minute aggregated liquidation metrics
        "liquidation_metrics": {
            "count": 3,                      # Total liquidations in the minute
            "buy_volume": 0.150,             # Total buy liquidation volume
            "sell_volume": 2.345,            # Total sell liquidation volume
            "total_volume": 2.495            # Total liquidation volume
        },
        
        # Latest funding rate data (most recent within the minute)
        "funding_rate": {
            "current_rate": 0.00004125,      # Current funding rate
            "current_timestamp": minute_start + 45000,  # When it was fetched
            "next_timestamp": minute_start + 8*60*60*1000,  # Next funding time
            "mark_price": 117635.2,          # Mark price
            "index_price": 117634.8,         # Index price
            "estimated_settle_price": 117635.0
        },
        
        # Latest long/short ratios (most recent 1m period data)
        "long_short_ratios": {
            "global_account_ratio": {
                "longShortRatio": 2.34,      # 1-minute period ratio
                "longAccount": 0.70,         # Long account percentage
                "shortAccount": 0.30,        # Short account percentage
                "timestamp": minute_start
            },
            "top_trader_ratio": {
                "longShortRatio": 1.87,      # 1-minute period ratio
                "longAccount": 0.65,         # Top trader long percentage
                "shortAccount": 0.35,        # Top trader short percentage
                "timestamp": minute_start
            },
            "top_position_ratio": {
                "longShortRatio": 1.95,      # 1-minute period ratio
                "longPosition": 0.66,        # Long position percentage
                "shortPosition": 0.34,       # Short position percentage
                "timestamp": minute_start
            }
        },
        
        # Latest 24h ticker (most recent within the minute)
        "ticker_24h": {
            "open": 117200.0,               # 24h open
            "high": 118100.0,               # 24h high
            "low": 116900.0,                # 24h low
            "close": 117635.0,              # Current close
            "volume": 12847.52,             # 24h volume
            "quote_volume": 1_512_500_000.0, # 24h quote volume
            "change": 435.0,                # 24h change
            "percentage": 0.372,            # 24h percentage
            "vwap": 117523.8                # 24h VWAP
        },
        
        # Latest open interest (most recent within the minute)
        "open_interest": {
            "open_interest": 91234.567,     # Current open interest
            "timestamp": minute_start + 50000  # When it was fetched
        },
        
        # Enhanced metrics calculated from 1-minute aggregated data
        "enhanced_metrics": {
            "cvd": 1.095,                   # Cumulative Volume Delta (buy_vol - sell_vol)
            "buy_sell_ratio": 1.294,        # Buy/sell volume ratio
            "vwap": 117618.7,               # Volume-weighted average price
            "trade_count": 127,             # Number of trades
            "total_volume": 8.547,          # Total volume
            "liquidation_count": 3,         # Number of liquidations
            "total_liquidation_volume": 2.495,  # Total liquidation volume
            "avg_spread": 0.15,             # Average spread
            "spread_volatility": 0.15,      # Max spread - min spread
            "avg_bid_depth": 15.7,          # Average bid depth
            "avg_ask_depth": 12.3,          # Average ask depth
            "depth_imbalance": 0.121        # (bid_depth - ask_depth) / (bid_depth + ask_depth)
        }
    }
    
    return schema

def print_1m_schema_summary():
    """Print a summary of 1-minute aggregated schema"""
    
    print("📊 1-MINUTE AGGREGATED COLLECTOR SCHEMA")
    print("=" * 60)
    
    print("\n🎯 AGGREGATION APPROACH:")
    print("• Collection: Every 5 seconds during the minute")
    print("• Aggregation: At the end of each minute")
    print("• Storage: One document per symbol per minute")
    
    print("\n📋 AGGREGATED DATA TYPES:")
    print("-" * 40)
    print("✅ TRADES → Aggregated per minute:")
    print("   • Total volume, buy/sell volumes")
    print("   • Trade count, VWAP")
    print("   • Buy/sell ratio, CVD")
    
    print("\n✅ LIQUIDATIONS → Aggregated per minute:")
    print("   • Total liquidation amounts")
    print("   • Buy/sell liquidation volumes")
    print("   • Liquidation count")
    
    print("\n✅ ORDERBOOK → Aggregated metrics:")
    print("   • Average spreads and depths")
    print("   • Spread volatility (min/max)")
    print("   • Depth imbalance metrics")
    print("   • Current orderbook state")
    
    print("\n✅ OHLCV → Native 1-minute candles:")
    print("   • Direct from exchange (already 1m)")
    
    print("\n✅ RATES → Latest within minute:")
    print("   • Funding rates (latest)")
    print("   • Long/short ratios (1m period)")
    print("   • Open interest (latest)")
    print("   • 24h ticker (latest)")
    
    print("\n💾 STORAGE PATTERN:")
    print("• Collection: {symbol}_1m_aggregated")
    print("• Frequency: 1 document per minute per symbol")
    print("• Size estimate: ~5KB per document")
    print("• Daily storage: ~7.2MB per symbol")

def show_comparison():
    """Show comparison between old and new approaches"""
    
    print("\n📊 BEFORE vs AFTER COMPARISON")
    print("=" * 60)
    
    print("\n❌ BEFORE (Mixed timescales):")
    print("• OHLCV: 1-minute candles")
    print("• Trades: Individual trade records")
    print("• Orderbook: Real-time snapshots")
    print("• Liquidations: Individual events")
    print("• Collection: Every 60 seconds")
    print("• Result: Mixed granularity data")
    
    print("\n✅ AFTER (All 1-minute aggregated):")
    print("• OHLCV: 1-minute candles")
    print("• Trades: 1-minute aggregated volumes")
    print("• Orderbook: 1-minute aggregated metrics")
    print("• Liquidations: 1-minute total amounts")
    print("• Collection: Every 5s, aggregated per minute")
    print("• Result: Consistent 1-minute granularity")
    
    print("\n🎯 BENEFITS:")
    print("• Consistent time-series data")
    print("• Better for technical analysis")
    print("• Reduced data noise")
    print("• Standardized aggregation periods")
    print("• Enhanced minute-level metrics")

if __name__ == "__main__":
    # Generate and display schema
    schema = generate_1m_aggregated_schema()
    
    print("🔍 1-MINUTE AGGREGATED SCHEMA EXAMPLE")
    print("=" * 60)
    
    # Show condensed version
    condensed = json.dumps(schema, indent=2)[:2000] + "\n... (truncated)"
    print(condensed)
    
    print_1m_schema_summary()
    show_comparison()