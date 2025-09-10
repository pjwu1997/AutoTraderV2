# Master/Slave 溝通協議

## 🔗 溝通機制總覽

### 網路架構
```
┌─────────────────────────────────────┐
│             Master VM               │
│  ┌─────────┐  ┌──────────────────┐  │
│  │MongoDB  │  │Master Coordinator│  │
│  │:27017   │  │:8080 (API)      │  │
│  └─────────┘  └──────────────────┘  │
│  ┌─────────┐  ┌──────────────────┐  │  
│  │Redis    │  │Dashboard         │  │
│  │:6379    │  │:8080/dashboard   │  │
│  └─────────┘  └──────────────────┘  │
└─────────────────┬───────────────────┘
                  │ HTTP/TCP
    ┌─────────────┼─────────────┐
    │             │             │
┌───▼───┐    ┌───▼───┐    ┌───▼───┐
│Slave-1│    │Slave-2│    │Slave-N│
│:8081  │    │:8081  │    │:8081  │  
└───────┘    └───────┘    └───────┘
```

## 📡 溝通協議細節

### 1. Slave 註冊流程

**步驟 1: Slave 啟動註冊**
```
POST http://master-vm:8080/api/register
Content-Type: application/json

{
  "slave_id": "slave-1",
  "ip_address": "10.0.1.101", 
  "capabilities": {
    "max_symbols": 100,
    "cpu_cores": 2,
    "memory_gb": 4
  },
  "services": ["data_fetcher", "websocket", "health_checker"]
}
```

**步驟 2: Master 回應分配**
```json
{
  "status": "registered",
  "slave_id": "slave-1",
  "assignment": {
    "symbols": ["BTCUSDT", "ETHUSDT", ...],  // 分配的 symbols
    "symbol_count": 85,
    "tasks": {
      "data_collection": true,
      "websocket_kline": true, 
      "websocket_liquidation": true
    }
  },
  "config": {
    "fetch_interval": 60,
    "heartbeat_interval": 30,
    "mongo_uri": "mongodb://master-vm:27017/",
    "mongo_db": "trading_data",
    "batch_size": 10,
    "rate_limit_delay": 0.1
  }
}
```

### 2. 心跳機制 (每30秒)

**Slave → Master 心跳**
```
POST http://master-vm:8080/api/heartbeat/slave-1
Content-Type: application/json

{
  "timestamp": "2024-01-01T12:00:00Z",
  "status": "online",
  "health": {
    "cpu_usage": 45.2,
    "memory_usage": 67.8,
    "disk_usage": 23.1,
    "network_latency_ms": 12
  },
  "performance": {
    "symbols_processed_last_cycle": 85,
    "avg_processing_time_ms": 1250,
    "error_count": 2,
    "success_rate": 97.6
  },
  "data_stats": {
    "records_inserted_last_hour": 5100,
    "last_successful_fetch": "2024-01-01T11:59:30Z",
    "mongo_connection_status": "healthy"
  }
}
```

**Master → Slave 回應**
```json
{
  "status": "acknowledged",
  "timestamp": "2024-01-01T12:00:01Z",
  "instructions": {
    "continue": true,
    "adjust_rate_limit": false,
    "new_symbol_assignment": null
  },
  "warnings": [],
  "next_heartbeat_in": 30
}
```

### 3. 工作負載調整機制

**當 Slave 負載過高時:**
```json
{
  "status": "overloaded", 
  "instructions": {
    "reduce_symbols": true,
    "new_assignment": ["BTCUSDT", "ETHUSDT", ...],  // 減少的 symbols
    "removed_symbols": ["ADAUSDT", "DOTUSDT", ...], // 被移除的 symbols
    "rate_limit_increase": 0.2  // 增加請求間隔
  }
}
```

**當其他 Slave 下線時:**
```json
{
  "status": "rebalancing",
  "instructions": {
    "additional_symbols": ["LINKUSDT", "LTCUSDT", ...],  // 新增的 symbols
    "reason": "slave-3 offline, redistributing workload"
  }
}
```

## 🗄️ 資料流向

### 1. 資料收集流程

```
┌─────────────┐    1. 收集    ┌─────────────┐
│   Slave-1   │ ────────────→ │   Binance   │
│ (symbols    │               │     API     │
│  1-85)      │ ←──────────── │             │
└──────┬──────┘    2. 回應    └─────────────┘
       │
       │ 3. 儲存資料
       ▼
┌─────────────┐               ┌─────────────┐
│ Master VM   │               │   MongoDB   │
│ MongoDB     │ ←─────────────│Collection:  │
│ :27017      │   4. 寫入     │market_data  │
└─────────────┘               └─────────────┘
```

### 2. MongoDB 連接配置

**所有 Slave 連接到 Master 的 MongoDB:**
```yaml
# Slave 環境變數
MONGO_URI=mongodb://master-vm-ip:27017/
MONGO_DB_NAME=trading_data
MONGO_AUTH_SOURCE=admin
MONGO_USERNAME=trader_user
MONGO_PASSWORD=secure_password
```

**Master MongoDB 配置允許外部連接:**
```yaml
# Master docker-compose.yml
mongodb:
  ports:
    - "0.0.0.0:27017:27017"  # 允許所有 IP 連接
  environment:
    - MONGO_INITDB_ROOT_USERNAME=admin
    - MONGO_INITDB_ROOT_PASSWORD=admin_password
```

## 🔄 故障處理協議

### 1. Slave 失聯處理

**Master 偵測邏輯:**
```python
# 當 Slave 超過 2 分鐘未發送心跳
if last_heartbeat > 120_seconds_ago:
    slave.status = "offline"
    redistribute_symbols(slave.assigned_symbols)
    notify_other_slaves()
```

**重新分配演算法:**
```python
def redistribute_symbols(failed_symbols):
    online_slaves = get_online_slaves()
    symbols_per_slave = len(failed_symbols) // len(online_slaves)
    
    for i, slave in enumerate(online_slaves):
        start_idx = i * symbols_per_slave
        end_idx = start_idx + symbols_per_slave
        additional_symbols = failed_symbols[start_idx:end_idx]
        
        send_rebalance_instruction(slave.id, additional_symbols)
```

### 2. Master 故障處理

**Slave 偵測 Master 失聯:**
```python
def handle_master_offline():
    # 1. 繼續當前資料收集（本地快取配置）
    continue_current_assignment()
    
    # 2. 每分鐘嘗試重新連接
    schedule.every(1).minute.do(attempt_master_reconnection)
    
    # 3. 本地日誌記錄狀態
    log_local_status()
```

## 📊 監控和除錯

### 1. 即時狀態查詢

**檢查所有 Slave 狀態:**
```bash
curl http://master-vm:8080/api/slaves | jq '.'
```

**檢查特定 Slave:**
```bash
curl http://master-vm:8080/api/slaves/slave-1 | jq '.'
```

**檢查系統總覽:**
```bash
curl http://master-vm:8080/api/status | jq '.'
```

### 2. 資料統計查詢

**MongoDB 資料統計:**
```javascript
// 各 Slave 收集的資料量
db.market_data.aggregate([
  {"$group": {"_id": "$collector_id", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}}
])

// 最近一小時的資料分佈
db.market_data.aggregate([
  {"$match": {"timestamp": {"$gte": new Date(Date.now() - 3600000)}}},
  {"$group": {"_id": {"symbol": "$symbol", "collector": "$collector_id"}, "count": {"$sum": 1}}}
])
```

## 🔧 效能調優

### 1. 批次處理優化

**Slave 批次收集:**
```python
def batch_collect_symbols(symbols, batch_size=10):
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        
        # 並行處理批次
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(collect_symbol_data, symbol) for symbol in batch]
            results = [future.result() for future in futures]
        
        # 批次寫入 MongoDB
        batch_insert_to_mongo(results)
        
        # 速率限制
        time.sleep(rate_limit_delay)
```

### 2. 連接池優化

**MongoDB 連接池:**
```python
# 每個 Slave 使用連接池
mongo_client = MongoClient(
    uri,
    maxPoolSize=20,
    minPoolSize=5,
    maxIdleTimeMS=30000,
    serverSelectionTimeoutMS=5000
)
```

## 🚨 安全考量

### 1. 網路安全

**防火牆規則:**
```bash
# Master VM
sudo ufw allow from slave-vm-ip to any port 27017  # MongoDB
sudo ufw allow from slave-vm-ip to any port 8080   # API
sudo ufw allow from slave-vm-ip to any port 6379   # Redis

# Slave VM  
sudo ufw allow from master-vm-ip to any port 8081  # Health check
```

### 2. 認證機制

**MongoDB 認證:**
```yaml
# 建立專用資料庫使用者
db.createUser({
  user: "trader_collector",
  pwd: "secure_password",
  roles: [
    { role: "readWrite", db: "trading_data" }
  ]
})
```

**API Token 認證:**
```python
# Master API 加入簡單認證
headers = {
    "Authorization": "Bearer slave_token_here",
    "Content-Type": "application/json"
}
```