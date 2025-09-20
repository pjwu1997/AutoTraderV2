# 分散式交易資料收集系統架構文件

## 🏗️ 完整系統架構 (1分鐘精度 + WebSocket即時資料)

### 整體架構圖
```
                    ┌─────────────────────┐
                    │     Master VM       │
                    │  (協調 & 監控中心)   │ ← Dashboard: :8080
                    │  - Symbol 分配      │   MongoDB: :27017  
                    │  - 健康監控         │   API: :8080/api
                    │  - 資料彙整         │
                    └──────────┬──────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
   ┌────▼────┐           ┌────▼────┐           ┌────▼────┐
   │Slave-1  │           │Slave-2  │    ...    │Slave-5  │
   │ 105 個  │           │ 105 個  │           │ 105 個  │  
   │ Symbol  │           │ Symbol  │           │ Symbol  │
   │獨立 IP   │           │獨立 IP   │           │獨立 IP   │
   └────┬────┘           └────┬────┘           └────┬────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                   ┌──────────▼──────────┐
                   │   Shared MongoDB    │ ← 所有資料匯聚
                   │ (trading_data DB)   │   完整市場資料
                   └─────────────────────┘
```

### 每個 Slave VM 的4容器架構

```
┌─────────────────────────────────────────┐
│              Slave VM (獨立IP)            │
├─────────────────────────────────────────┤
│  ┌─────────────────────────────────────┐ │
│  │        data-fetcher (:8081)         │ │ ← REST API 1分鐘資料收集
│  │  • OHLCV (1m精度)                  │ │   • CVD計算 (Spot+Futures)
│  │  • Enhanced Funding Rate           │ │   • Long/Short Ratios (4類型)
│  │  • Open Interest                   │ │   • Premium Index
│  └─────────────────────────────────────┘ │
│  ┌─────────────────────────────────────┐ │
│  │       kline-websocket (:8082)       │ │ ← WebSocket 即時K線
│  │  • Spot K線 (1m精度)               │ │   • Futures K線 (1m精度)  
│  │  • 即時價格更新                     │ │   • 毫秒級延遲
│  └─────────────────────────────────────┘ │
│  ┌─────────────────────────────────────┐ │
│  │   liquidation-websocket (:8083)    │ │ ← WebSocket 即時清算
│  │  • 即時清算事件                     │ │   • 1分鐘聚合處理
│  │  • Buy/Sell清算統計                │ │   • 自動重連機制
│  └─────────────────────────────────────┘ │
│  ┌─────────────────────────────────────┐ │
│  │       health-checker (:8084)       │ │ ← 健康監控服務
│  │  • 服務狀態監控                     │ │   • Master心跳通信
│  │  • 資源使用監控                     │ │   • 故障自動恢復
│  └─────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

## 📊 完整資料收集流程

### 1. 資料收集精度與頻率
```
時間精度: 1分鐘 (相比原本5分鐘提升5倍精度)
收集方式: 雙重保障 (WebSocket即時 + REST API備份)
處理延遲: WebSocket < 100ms, REST API < 5s
資料完整性: 526個交易對 × 6種資料類型 = 完整市場覆蓋
```

### 2. 資料類型與來源

| 資料類型 | 來源 | 精度 | 更新頻率 | API端點 |
|---------|------|------|---------|---------|
| **OHLCV** | WebSocket + REST | 1分鐘 | 即時 + 1分鐘備份 | `/api/v3/klines` |
| **CVD (Spot)** | REST API | 1分鐘 | 每分鐘 | `/api/v3/ticker/24hr` |
| **CVD (Futures)** | REST API | 1分鐘 | 每分鐘 | `/fapi/v1/ticker/24hr` |
| **Funding Rate (當前)** | CCXT + REST | 即時 | 每8小時更新 | `/fapi/v1/fundingRate` |
| **Funding Rate (下一期)** | REST API | 即時 | 即時預測 | `/fapi/v1/premiumIndex` |
| **Long/Short Ratios** | REST API | 1分鐘 | 每分鐘 | `/futures/data/globalLongShortAccountRatio` |
| **Open Interest** | REST API | 即時 | 每5分鐘 | `/fapi/v1/openInterest` |
| **Liquidations** | WebSocket | 即時 | 即時聚合 | `!forceOrder@arr` |
| **Mark/Index Price** | REST API | 即時 | 即時 | `/fapi/v1/premiumIndex` |

### 3. WebSocket 連接架構

```
每個 Slave VM 維護的 WebSocket 連接:

┌─── Spot K線 WebSocket ───┐
│ wss://stream.binance.com:9443/ws/
│ {symbol1}@kline_1m/{symbol2}@kline_1m/...
│ 連接數: 1個多流連接 (每Slave)
└─────────────────────────┘

┌─── Futures K線 WebSocket ───┐  
│ wss://fstream.binance.com/ws/
│ {symbol1}@kline_1m/{symbol2}@kline_1m/...
│ 連接數: 1個多流連接 (每Slave)
└─────────────────────────────┘

┌─── 清算事件 WebSocket ───┐
│ wss://fstream.binance.com/ws/
│ !forceOrder@arr (全域清算流)
│ 連接數: 1個 (每Slave 都監聽)
└────────────────────────┘

總WebSocket連接: 3個/Slave × 5個Slave = 15個連接
```

## 🗄️ MongoDB 資料結構

### 完整文檔 Schema (1分鐘精度)
```json
{
  "_id": "BTCUSDT_1694640000",
  "timestamp": "2023-09-13T20:00:00Z", 
  "symbol": "BTCUSDT",
  
  "spot": {
    "open": "26500.00",
    "high": "26525.50", 
    "low": "26485.25",
    "close": "26515.75",
    "volume": "856.25",
    "cvd": 45.75
  },
  
  "futures": {
    "open": "26500.00",
    "high": "26525.50",
    "low": "26485.25", 
    "close": "26515.75",
    "volume": "1250.5",
    "cvd": 125.75,
    
    "funding_rate": 0.0001,
    "next_funding_rate": 0.0002,
    "next_funding_time": 1694649600000,
    "mark_price": 26515.75,
    "index_price": 26512.50
  },
  
  "long_short_ratio": {
    "global_long_short_ratio": 1.25,
    "top_trader_long_short_ratio": 1.15, 
    "top_trader_long_account": 0.535,
    "top_trader_short_account": 0.465,
    "taker_buy_sell_ratio": 1.12,
    "open_interest": 125000000,
    "open_interest_change_percent": 1.01
  },
  
  "liquidations": {
    "buy_liquidations": {
      "total_quantity": 125.5,
      "total_dollars": 3326375.0,
      "event_count": 23
    },
    "sell_liquidations": {
      "total_quantity": 89.25, 
      "total_dollars": 2365781.25,
      "event_count": 18
    }
  },
  
  "collector_info": {
    "slave_id": "slave-001",
    "collection_timestamp": "2023-09-13T20:00:05Z",
    "data_version": "enhanced_v2",
    "apis_called": ["ohlcv", "funding_rate", "premium_index", "long_short_ratios", "open_interest", "liquidations"],
    "collection_method": "hybrid_websocket_rest",
    "data_precision": "1m",
    "websocket_latency_ms": 45
  }
}
```

### 資料庫統計 (預期每日資料量)
```
單一 Symbol 每日文檔數: 1440個 (24小時 × 60分鐘)
全系統 Symbol 數量: 526個
每日總文檔數: 757,440個
每個文檔大小: ~2KB
每日資料量: ~1.5GB
每月資料量: ~45GB
```

## ⚡ 效能規格與監控

### 系統效能指標
```
資料收集延遲:
- WebSocket: < 100ms (即時資料)
- REST API: < 5秒 (1分鐘備份)
- 資料庫寫入: < 500ms

API 請求限制:
- Binance限制: 1200請求/分鐘/IP
- 系統設計: 每Slave 約800請求/分鐘 (66%使用率)
- 安全邊際: 33% (應對突發流量)

WebSocket 連接穩定性:
- 自動重連: 5秒延遲, 最多重試10次
- 心跳檢測: 30秒間隔
- 24小時重連: 防止連接劣化
```

### 監控項目
```bash
# 系統健康檢查
curl http://master-vm:8080/api/status
curl http://slave-1:8081/health
curl http://slave-1:8082/health  # K線WebSocket
curl http://slave-1:8083/health  # 清算WebSocket

# 資料完整性檢查
db.market_data.countDocuments({
  "timestamp": {
    "$gte": new Date("2023-09-13T20:00:00Z"),
    "$lt": new Date("2023-09-13T21:00:00Z")
  }
})
# 預期結果: 526個文檔 (每分鐘)

# WebSocket連接狀態
docker-compose -f docker-compose.slave.yml logs kline-websocket | grep "Connected"
docker-compose -f docker-compose.slave.yml logs liquidation-websocket | grep "Connected"
```

## 🚀 部署架構優勢

### 1. 資料精度提升
- **5倍精度提升**: 從5分鐘降至1分鐘
- **即時性增強**: WebSocket毫秒級更新
- **完整性保障**: 雙重來源防止資料遺失

### 2. 系統穩定性
- **故障隔離**: 單一Slave故障不影響其他
- **自動恢復**: WebSocket斷線自動重連
- **負載分散**: 5個獨立IP避免API限制

### 3. 可擴展性
- **水平擴展**: 輕鬆增加Slave數量
- **垂直擴展**: 可升級單一VM規格
- **地理分散**: 可部署至不同區域

### 4. 成本效率
- **獨立IP**: 避免昂貴的NAT Gateway
- **資源最佳化**: 每服務獨立容器化
- **運維簡化**: 統一Docker部署

## 📋 運維檢查清單

### 每日檢查項目
```bash
□ 檢查Master狀態: curl http://master-vm:8080/api/status
□ 檢查所有Slave健康: for i in {1..5}; do curl http://slave-$i:8081/health; done
□ 檢查WebSocket連接: docker logs | grep "WebSocket connected"
□ 檢查資料完整性: 驗證昨日資料量 = 526 × 1440
□ 檢查MongoDB儲存空間: db.stats()
□ 檢查API使用率: 確保 < 1000請求/分鐘/IP
```

### 每週檢查項目
```bash  
□ 更新Docker images: docker-compose pull && docker-compose up -d
□ 清理舊日誌檔案: find logs/ -name "*.log" -mtime +7 -delete
□ MongoDB效能分析: db.market_data.getIndexes()
□ 網路安全檢查: 驗證防火牆規則
□ 備份驗證: 確認MongoDB備份完整性
```

這個架構文件提供了完整的1分鐘精度WebSocket系統架構說明，涵蓋了從資料收集到儲存的完整流程，以及運維監控的詳細指導。