# 📊 DB Schema 增強指南

## 🎯 概覽

基於你現有的 MongoDB schema，我已經擴展了資料收集功能，保持**完全向後兼容**的同時加入了新的市場資料。

## 📋 現有 Schema vs 增強版對照

### 🔍 原始 Schema 結構
```json
{
  "_id": "68c05940e82c18c46415b838",
  "timestamp": "2025-09-09T16:43:00.000Z",
  "symbol": "DOTUSDT",
  
  "futures": {
    "open": "4.040",
    "high": "4.047", 
    "low": "4.039",
    "close": "4.045",
    "volume": "34524.2",
    "quote_volume": "139597.5463",
    "trade_num": 339,
    "taker_buy_base": "21317.2",
    "taker_buy_quote": "86194.5263",
    "cvd": -13202.229913473422,
    "calculated_volume": 34524.2,
    "funding_rate": 0.0001
  },
  
  "long_short_ratio": {
    "open_interest": 26247963.4,
    "premium_index": 0
  },
  
  "spot": {
    "open": "4.04100000",
    "high": "4.04800000",
    "low": "4.04000000", 
    "close": "4.04600000",
    "volume": "8221.88000000",
    "quote_volume": "33258.40867000",
    "trade_num": 120,
    "taker_buy_base": "6763.73000000",
    "taker_buy_quote": "27362.47967000",
    "cvd": -1457.224172021749,
    "calculated_volume": 8221.88,
    "market_cap": 6144585871
  },
  
  "spot_margin_fee": {
    "dailyInterestRate": 0.00000637
  },
  
  "liquidations": {
    "buy_liquidations": {
      "total_quantity": 0,
      "total_dollars": 0,
      "event_count": 0
    },
    "sell_liquidations": {
      "total_quantity": 0,
      "total_dollars": 0,
      "event_count": 0
    }
  }
}
```

### ✨ 增強版 Schema 結構

#### 1. **保持不變的部分**
- `_id`, `timestamp`, `symbol` - 完全相同
- `futures` - 所有現有欄位保持不變
- `spot` - 所有現有欄位保持不變  
- `liquidations` - 結構完全相同 (功能待實作)

#### 2. **擴展的 `long_short_ratio` 物件**
```json
"long_short_ratio": {
  // === 現有欄位 (保持不變) ===
  "open_interest": 26247963.4,
  "premium_index": 0,
  
  // === 新增: 全域帳戶多空比 ===
  "global_long_short_ratio": 1.1164,
  "global_long_account": 0.527,
  "global_short_account": 0.473,
  
  // === 新增: 頂級交易者帳戶多空比 ===
  "top_trader_long_short_ratio": 1.3041,
  "top_trader_long_account": 0.566,
  "top_trader_short_account": 0.434,
  
  // === 新增: 頂級交易者倉位多空比 ===
  "top_trader_position_ratio": 1.2156,
  "top_trader_long_position": 0.5487,
  "top_trader_short_position": 0.4513,
  
  // === 新增: Taker 買賣比例 ===
  "taker_buy_sell_ratio": 1.3849,
  "taker_buy_volume": 154.375,
  "taker_sell_volume": 111.469,
  
  // === 新增: 未平倉合約量變化分析 ===
  "open_interest_value": 1234567890.50,
  "open_interest_change": 245732.1,
  "open_interest_change_percent": 2.83,
  "open_interest_trend": "increasing"
}
```

#### 3. **擴展的 `spot_margin_fee` 物件**
```json
"spot_margin_fee": {
  // === 現有欄位 (保持不變) ===
  "dailyInterestRate": 0.00000637,
  
  // === 新增: 多種資產的保證金利率 ===
  "margin_daily_rate_btc": 0.00001234,
  "margin_daily_rate_eth": 0.00001567,
  "margin_daily_rate_usdt": 0.00000637,
  
  // === 新增: 下一小時利率 ===
  "next_hourly_rate_btc": 0.00000156,
  "next_hourly_rate_eth": 0.00000198,
  "next_hourly_rate_usdt": 0.00000089,
  "next_hourly_time_btc": 1726080000000,
  "next_hourly_time_eth": 1726080000000,
  "next_hourly_time_usdt": 1726080000000
}
```

#### 4. **新增: `collector_info` 物件**
```json
"collector_info": {
  "slave_id": "slave-1",
  "collection_timestamp": "2025-09-10T18:30:15.123Z",
  "data_version": "enhanced_v2",
  "apis_called": ["ohlcv", "funding_rate", "long_short_ratios", "open_interest"]
}
```

## 🔄 資料流對照

### 原始資料收集 → 增強版資料收集

| 資料類型 | 原始來源 | 增強版來源 | 新增欄位數量 |
|---------|---------|-----------|-------------|
| OHLCV | ✅ ccxt | ✅ ccxt | 0 (保持不變) |
| CVD | ✅ 計算 | ✅ 計算 | 0 (保持不變) |
| Funding Rate | ✅ ccxt | ✅ ccxt | 0 (保持不變) |
| Open Interest | ✅ 基本值 | ✅ 詳細分析 | +4 欄位 |
| Long-Short Ratio | ❌ 僅佔位 | ✅ 完整收集 | +9 欄位 |
| Interest Rate | ✅ 基本值 | ✅ 多資產 | +6 欄位 |
| Liquidations | ❌ 空結構 | ⏳ 待實作 | 0 (待擴展) |

## 📊 新增資料的商業價值

### 1. **市場情緒分析**
```javascript
// 查詢看多情緒強烈的 symbols
db.market_data.find({
  "long_short_ratio.global_long_short_ratio": {$gt: 2.0}
}).sort({"timestamp": -1})

// 分析大戶 vs 散戶情緒差異
db.market_data.aggregate([
  {$project: {
    symbol: 1,
    timestamp: 1,
    retail_sentiment: "$long_short_ratio.global_long_short_ratio",
    whale_sentiment: "$long_short_ratio.top_trader_long_short_ratio",
    sentiment_divergence: {
      $subtract: ["$long_short_ratio.top_trader_long_short_ratio", 
                  "$long_short_ratio.global_long_short_ratio"]
    }
  }}
])
```

### 2. **流動性分析**
```javascript
// 查詢未平倉合約量快速增長的 symbols
db.market_data.find({
  "long_short_ratio.open_interest_change_percent": {$gt: 10}
}).sort({"timestamp": -1})

// 分析市場規模變化
db.market_data.aggregate([
  {$group: {
    _id: "$symbol",
    latest_oi: {$last: "$long_short_ratio.open_interest"},
    avg_change: {$avg: "$long_short_ratio.open_interest_change_percent"}
  }}
])
```

### 3. **資金成本監控**
```javascript
// 監控各資產借貸成本
db.market_data.find({
  "spot_margin_fee.next_hourly_rate_btc": {$gt: 0.001}
})

// 套利機會識別
db.market_data.aggregate([
  {$project: {
    symbol: 1,
    funding_rate: "$futures.funding_rate",
    margin_rate: "$spot_margin_fee.dailyInterestRate",
    rate_spread: {
      $subtract: ["$futures.funding_rate", "$spot_margin_fee.dailyInterestRate"]
    }
  }}
])
```

## 🔧 遷移和兼容性

### ✅ 完全向後兼容
- 所有現有查詢語句繼續有效
- 現有應用程式無需修改
- 資料結構只是**擴展**，不是替換

### 🆕 新功能啟用
```javascript
// 檢查是否為增強版資料
db.market_data.find({
  "collector_info.data_version": "enhanced_v2"
})

// 新功能查詢範例
db.market_data.find({
  "long_short_ratio.global_long_short_ratio": {$exists: true}
})
```

### 📈 效能優化建議

#### 新增索引 (針對新欄位)
```javascript
// 多空比查詢索引
db.market_data.createIndex({
  "symbol": 1,
  "long_short_ratio.global_long_short_ratio": -1,
  "timestamp": -1
})

// 未平倉合約量變化索引
db.market_data.createIndex({
  "long_short_ratio.open_interest_change_percent": -1,
  "timestamp": -1
})

// 收集器查詢索引
db.market_data.createIndex({
  "collector_info.slave_id": 1,
  "timestamp": -1
})
```

## 🎯 實際使用範例

### 1. **交易信號生成**
```javascript
// 識別多空分歧機會
db.market_data.find({
  $expr: {
    $and: [
      {$gt: ["$long_short_ratio.global_long_short_ratio", 2.0]}, // 散戶極度看多
      {$lt: ["$long_short_ratio.top_trader_long_short_ratio", 0.8]} // 大戶看空
    ]
  }
})
```

### 2. **風險管理**
```javascript
// 監控高槓桿風險
db.market_data.find({
  $and: [
    {"long_short_ratio.open_interest_change_percent": {$gt: 20}}, // OI快速增長
    {"long_short_ratio.taker_buy_sell_ratio": {$gt: 3.0}} // 主動買入過多
  ]
})
```

### 3. **市場概覽儀表板**
```javascript
// 生成市場情緒儀表板資料
db.market_data.aggregate([
  {$match: {"timestamp": {$gte: new Date(Date.now() - 3600000)}}}, // 最近1小時
  {$group: {
    _id: "$symbol",
    latest_price: {$last: "$futures.close"},
    sentiment_score: {$last: "$long_short_ratio.global_long_short_ratio"},
    oi_trend: {$last: "$long_short_ratio.open_interest_trend"},
    whale_sentiment: {$last: "$long_short_ratio.top_trader_long_short_ratio"}
  }},
  {$sort: {"sentiment_score": -1}},
  {$limit: 20}
])
```

## 🚀 總結

增強版 schema 提供了：

✅ **100% 向後兼容性** - 現有程式碼無需修改
✅ **豐富的市場數據** - 9 種新的多空比指標  
✅ **深度流動性分析** - 未平倉合約量變化趨勢
✅ **多資產利率監控** - 跨資產套利機會
✅ **分散式追蹤** - 收集器來源和版本管理

這個增強版 schema 將你的資料庫從**基礎價格資料**升級為**全方位市場情報平台**！