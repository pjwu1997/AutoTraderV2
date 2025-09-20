# 📊 Unified Collector Data Schema

## Overview
The unified collector produces comprehensive market data in JSON format, stored in MongoDB collections. Each document contains complete market state for a symbol at a specific timestamp.

---

## 🔄 Primary Collection Schema: `{symbol}_market_data`

### Complete Document Structure:
```json
{
  "symbol": "BTC/USDT:USDT",                    // Trading pair identifier
  "timestamp": 1758212141677,                   // Collection timestamp (milliseconds)
  "slave_id": "slave-1",                        // Which slave collected this data
  "collection_type": "unified_market_data",     // Data type identifier
  
  // 📈 OHLCV Data (100 most recent 1-minute candles)
  "ohlcv": [
    [
      1758206160000,    // Timestamp (milliseconds)
      117594.2,         // Open price
      117615.9,         // High price
      117594.2,         // Low price
      117605.6,         // Close price
      42.17             // Volume
    ]
    // ... 99 more candles
  ],
  
  // 📊 Order Book (20 levels each side)
  "orderbook": {
    "bids": [
      [117594.1, 0.5],      // [price, quantity]
      [117594.0, 1.2],
      [117593.9, 0.8]
      // ... up to 20 levels
    ],
    "asks": [
      [117594.2, 0.3],      // [price, quantity]
      [117594.3, 0.9],
      [117594.4, 1.1]
      // ... up to 20 levels
    ],
    "timestamp": 1758212141000
  },
  
  // 💱 Recent Trades (50 most recent)
  "trades": [
    {
      "price": 117594.2,
      "amount": 0.15,
      "side": "buy",        // "buy" or "sell"
      "timestamp": 1758212140000
    }
    // ... up to 50 trades
  ],
  
  // 💰 Funding Rate Information
  "funding_rate": {
    "current_rate": 0.00003914,           // Current funding rate
    "current_timestamp": 1758212100000,   // Current funding timestamp
    "next_timestamp": 1758240900000,      // Next funding time (+8 hours)
    "mark_price": 117594.5,              // Mark price
    "index_price": 117594.3,             // Index price
    "estimated_settle_price": 117594.4   // Estimated settlement price
  },
  
  // ⚖️ Long/Short Ratios
  "long_short_ratios": {
    "global_account_ratio": {
      "longShortRatio": 2.45,           // Global long/short ratio
      "longAccount": 0.71,              // Long account percentage
      "shortAccount": 0.29,             // Short account percentage
      "timestamp": 1758212100000
    },
    "top_trader_ratio": {
      "longShortRatio": 1.85,           // Top trader long/short ratio
      "longAccount": 0.65,              // Top trader long percentage
      "shortAccount": 0.35,             // Top trader short percentage
      "timestamp": 1758212100000
    },
    "top_position_ratio": {
      "longShortRatio": 1.92,           // Top position long/short ratio
      "longPosition": 0.66,             // Long position percentage
      "shortPosition": 0.34,            // Short position percentage
      "timestamp": 1758212100000
    }
  },
  
  // 📊 24-Hour Ticker Statistics
  "ticker_24h": {
    "open": 117200.0,                   // 24h open price
    "high": 118500.0,                   // 24h high price
    "low": 116800.0,                    // 24h low price
    "close": 117594.2,                  // Current close price
    "volume": 15432.75,                 // 24h base volume
    "quote_volume": 1_814_250_000.0,    // 24h quote volume (USDT)
    "change": 394.2,                    // 24h price change
    "percentage": 0.336,                // 24h percentage change
    "vwap": 117450.8                    // Volume weighted average price
  },
  
  // 🎯 Open Interest Data
  "open_interest": {
    "open_interest": 90768.769,         // Current open interest amount
    "timestamp": 1758212135317          // OI data timestamp
  },
  
  // 🧮 Enhanced Calculated Metrics
  "enhanced_metrics": {
    "cvd": 7.886,                       // Cumulative Volume Delta
    "buy_sell_ratio": 1.35,             // Buy vs sell volume ratio
    "spread": 0.10,                     // Best bid-ask spread
    "spread_percentage": 0.0001,        // Spread as percentage
    "volatility": 0.000296              // Price volatility (20-period)
  }
}
```

---

## ⚡ WebSocket Collections

### `kline_data` Collection (Real-time 1m candles):
```json
{
  "symbol": "BTC/USDT:USDT",
  "timestamp": 1758212160000,
  "open": 117594.2,
  "high": 117595.0,
  "low": 117593.5,
  "close": 117594.8,
  "volume": 12.5,
  "quote_volume": 1_470_000.0,
  "trades": 45,
  "is_closed": true,                    // Whether kline is finalized
  "slave_id": "slave-1",
  "data_source": "websocket"
}
```

### `liquidations` Collection (Real-time liquidation events):
```json
{
  "symbol": "BTC/USDT:USDT",
  "timestamp": 1758212165000,
  "side": "SELL",                       // Liquidation side (BUY/SELL)
  "order_type": "MARKET",               // Order type
  "time_in_force": "IOC",               // Time in force
  "quantity": 2.15,                     // Liquidated quantity
  "price": 117580.0,                    // Liquidation price
  "average_price": 117578.5,            // Average execution price
  "execution_type": "TRADE",            // Execution type
  "order_status": "FILLED",             // Order status
  "slave_id": "slave-1",
  "data_source": "websocket"
}
```

---

## 🗂️ Collection Strategy

### Per-Symbol Collections:
- **`BTCUSDT_market_data`**: Complete market data for BTC/USDT
- **`ETHUSDT_market_data`**: Complete market data for ETH/USDT
- **`ADAUSDT_market_data`**: Complete market data for ADA/USDT
- *(etc. for all 526+ symbols)*

### Shared Collections:
- **`kline_data`**: Real-time kline updates from all symbols
- **`liquidations`**: Real-time liquidation events from all symbols

---

## 📋 Data Freshness & Update Frequency

| Data Type | Update Frequency | Source |
|-----------|------------------|---------|
| OHLCV (100 candles) | Every 60 seconds | REST API |
| Orderbook (20 levels) | Every 60 seconds | REST API |
| Recent Trades (50) | Every 60 seconds | REST API |
| Funding Rates | Every 60 seconds | REST API |
| Long/Short Ratios | Every 60 seconds | REST API |
| 24h Ticker | Every 60 seconds | REST API |
| Open Interest | Every 60 seconds | REST API |
| Enhanced Metrics | Every 60 seconds | Calculated |
| Kline Streams | Real-time | WebSocket |
| Liquidations | Real-time | WebSocket |

---

## 🔍 Data Usage Examples

### Query latest market data:
```javascript
db.BTCUSDT_market_data.findOne({}, {sort: {timestamp: -1}})
```

### Get recent klines:
```javascript
db.kline_data.find({symbol: "BTC/USDT:USDT"}).sort({timestamp: -1}).limit(100)
```

### Find liquidations in last hour:
```javascript
db.liquidations.find({
  timestamp: {$gte: Date.now() - 3600000}
}).sort({timestamp: -1})
```

### Aggregate open interest across symbols:
```javascript
db.BTCUSDT_market_data.aggregate([
  {$sort: {timestamp: -1}},
  {$limit: 1},
  {$project: {"open_interest.open_interest": 1, symbol: 1}}
])
```

---

## 💾 Storage Estimates

**Per symbol per day:**
- REST API data: ~1,440 documents (every 60s)
- Document size: ~50KB each
- Daily storage: ~70MB per symbol
- **Total for 526 symbols: ~37GB per day**

**WebSocket data per day:**
- Kline updates: ~1,440 per symbol
- Liquidations: Variable (10-1000 per day)
- Additional storage: ~5GB per day

**Total estimated storage: ~42GB per day**