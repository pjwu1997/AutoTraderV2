# 🚨 Deployment Guide Updates Needed

## Critical Mismatches Found Between Implementation and Documentation

### ❌ **1. Data Collection Precision**

**Current Guide Says:**
```
- **OHLCV**: 5分鐘K線資料
- **CVD**: 累積成交量差異  
- **Funding Rate**: 資金費率
```

**Should Update To:**
```
- **OHLCV**: 1分鐘K線資料 (WebSocket即時 + REST API備份)
- **CVD**: Spot + Futures CVD (1分鐘精度)
- **Funding Rate**: 當前費率 + 下一期費率 + Mark Price + Index Price
- **Long/Short Ratios**: 4種不同類型的多空比 (全域、頂級交易者等)
- **Liquidations**: 即時清算事件 (WebSocket 1分鐘聚合)
- **Open Interest**: 當前 + 變化 + 趨勢分析
```

---

### ❌ **2. Missing WebSocket Services**

**Current Guide Shows:**
```bash
# 只提到這些服務
data-fetcher
health-checker
```

**Should Add:**
```bash
# 完整服務列表 (4個容器)
data-fetcher          # REST API 資料收集 (1m精度)
kline-websocket       # 即時K線資料 (Spot + Futures)
liquidation-websocket # 即時清算資料
health-checker        # 健康監控
```

---

### ❌ **3. Missing Environment Variables**

**Should Add These Variables:**
```bash
# === 新增環境變數 ===
# 精度設定
export TIMEFRAME=1m                    # ← 從 5m 改為 1m
export KLINE_INTERVAL=1m              # WebSocket K線間隔

# WebSocket URLs
export KLINE_SPOT_WS_URL="wss://stream.binance.com:9443/ws/{streams}"
export KLINE_FUTURES_WS_URL="wss://fstream.binance.com/ws/{streams}" 
export LIQUIDATION_WS_URL="wss://fstream.binance.com/ws/!forceOrder@arr"

# 清算設定
export LIQUIDATION_CLEANUP_MINUTES=5
export LIQUIDATION_RECONNECT_INTERVAL=86100
```

---

### ❌ **4. Missing Docker Compose Updates**

**Current docker-compose.slave.yml references:**
```yaml
# 只啟用了 data-fetcher 和 health-checker
```

**Should Enable:**
```yaml
services:
  data-fetcher:        # ✅ 已存在
    # ... REST API 服務
    
  kline-websocket:     # ❌ 需要取消註解並啟用
    # ... WebSocket K線服務
    
  liquidation-websocket: # ❌ 需要取消註解並啟用  
    # ... WebSocket 清算服務
    
  health-checker:      # ✅ 已存在
    # ... 健康監控
```

---

### ❌ **5. Missing Data Schema Information**

**Should Add Section:**
```markdown
## 📊 完整資料結構

每分鐘插入 MongoDB 的完整文檔結構：

```json
{
  "_id": "BTCUSDT_1694640000",
  "timestamp": "2023-09-13T20:00:00Z",
  "symbol": "BTCUSDT",
  
  "futures": {
    // OHLCV + CVD (WebSocket + REST)
    "open": "26500.00", "high": "26525.50", "close": "26515.75",
    "volume": "1250.5", "cvd": 125.75,
    
    // 完整資金費率資料
    "funding_rate": 0.0001,           // 當前費率
    "next_funding_rate": 0.0002,      // 下一期費率  
    "next_funding_time": 1694649600000, // 下一期時間
    "mark_price": 26515.75,           // 標記價格
    "index_price": 26512.50           // 指數價格
  },
  
  "long_short_ratio": {
    // 4種多空比 + 開放利息
    "global_long_short_ratio": 1.25,
    "top_trader_long_short_ratio": 1.15,
    "taker_buy_sell_ratio": 1.12,
    "open_interest": 125000000,
    "open_interest_change_percent": 1.01
  },
  
  "liquidations": {
    // 即時清算資料 (1分鐘聚合)
    "buy_liquidations": {"total_quantity": 125.5, "total_dollars": 3326375.0},
    "sell_liquidations": {"total_quantity": 89.25, "total_dollars": 2365781.25}
  },
  
  "collector_info": {
    "slave_id": "slave-001",
    "data_precision": "1m",
    "collection_method": "hybrid_websocket_rest"
  }
}
```
```

---

### ❌ **6. Missing Service Health Checks**

**Should Add:**
```bash
# === 服務健康檢查 ===

# 檢查所有4個容器狀態
docker-compose -f docker-compose.slave.yml ps

# 檢查 WebSocket 連接
docker-compose -f docker-compose.slave.yml logs kline-websocket
docker-compose -f docker-compose.slave.yml logs liquidation-websocket

# 檢查即時資料流
curl http://slave-ip:8081/health  # 健康檢查
tail -f logs/kline_websocket.log  # WebSocket 日誌
```

---

### ❌ **7. Missing Performance Information**

**Should Update:**
```markdown
## 🚀 效能規格

### 資料收集頻率
- **REST API**: 每1分鐘收集一次完整資料
- **WebSocket**: 即時串流資料 (毫秒級延遲)
- **聚合處理**: 每1分鐘聚合並儲存

### 資料完整性
- **526個交易對**: 完整覆蓋
- **6種資料類型**: OHLCV, CVD, 資金費率, 多空比, 清算, 開放利息
- **雙重來源**: WebSocket即時 + REST API備份
```

---

## 🔧 **Quick Fix Actions Needed:**

1. **Update AZURE_DEPLOYMENT_GUIDE.md** - Add missing WebSocket services
2. **Update DistributedSystem/README.md** - Change 5m to 1m precision
3. **Add environment variables** - WebSocket URLs and 1m precision settings
4. **Document 4-container architecture** - Not just 2 containers
5. **Add WebSocket troubleshooting** - Connection and stream issues
6. **Update cost analysis** - Account for additional WebSocket containers

## 🎯 **Impact:**
Without these updates, users will:
- ❌ Deploy incomplete system (missing WebSocket services)
- ❌ Get 5-minute data instead of 1-minute precision
- ❌ Miss critical funding rate information
- ❌ Have no real-time liquidation tracking

**Recommendation: Update deployment guide ASAP to match actual implementation!** 🚨