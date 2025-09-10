# 🚀 AutoTrader 全量收集系統指南

## 🎯 系統概覽

此系統將收集**所有** Binance Perpetual Contracts (~400+ symbols)，分散到 5 台 VM，每台具有獨立 IP。MongoDB 架設在 Master VM 上，所有 Slave 連接到同一個資料庫。

## 🏗️ 架構設計

```
                    🌐 Internet
                         │
                ┌────────▼─────────┐
                │   Master VM      │
                │ ┌──────────────┐ │
                │ │Master API    │ │ ← 協調中心
                │ │:8080         │ │
                │ ├──────────────┤ │
                │ │MongoDB       │ │ ← 資料庫
                │ │:27017        │ │   (所有資料)
                │ ├──────────────┤ │
                │ │Redis         │ │ ← 任務佇列
                │ │:6379         │ │
                │ └──────────────┘ │
                └────────┬─────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼────┐      ┌───▼────┐      ┌───▼────┐
   │Slave-1  │      │Slave-2 │ ...  │Slave-5 │
   │80 symbols│     │80 symbols│     │80 symbols│
   │獨立IP-1  │     │獨立IP-2 │     │獨立IP-5 │
   └─────────┘      └────────┘      └────────┘
        │                │                │
        └────────────────┼────────────────┘
                         │
                   MongoDB 資料寫入
```

## 📡 Master/Slave 溝通機制

### 1. 初始化流程

```sequence
Slave-1->Master: POST /api/register (slave_id, capabilities)
Master->Slave-1: Response (assigned_symbols, config)
Slave-1->Master MongoDB: Connect (mongo_uri)
Slave-1->Master: POST /api/heartbeat (health_status)
```

### 2. 持續運作流程

```
每分鐘循環:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Slave-1   │    │   Slave-2   │    │   Slave-5   │
│收集80 symbols│   │收集80 symbols│   │收集80 symbols│
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       ▼                  ▼                  ▼
┌─────────────────────────────────────────────────────┐
│            Master MongoDB (統一資料庫)               │
│ - market_data collection                           │
│ - kline_data collection                            │
│ - liquidation_data collection                      │
└─────────────────────────────────────────────────────┘

每30秒心跳:
Slave-1 ──heartbeat──→ Master API
Slave-2 ──heartbeat──→ Master API  
Slave-5 ──heartbeat──→ Master API
```

### 3. 資料收集詳細流程

每個 Slave 每分鐘執行：

```
1. 批次處理分配的 symbols (15個一批)
   ├─ OHLCV 資料收集
   ├─ CVD 計算
   ├─ Spot CVD 收集  
   ├─ Long-Short Ratio
   └─ Funding Rate

2. WebSocket 即時資料收集
   ├─ K線資料 (即時)
   └─ 清算資料 (即時)

3. 資料寫入 Master MongoDB
   ├─ 批次寫入優化
   ├─ 連接池管理
   └─ 錯誤重試機制

4. 狀態回報給 Master
   ├─ 處理數量統計
   ├─ 錯誤計數
   ├─ 系統資源使用率
   └─ 網路延遲監控
```

## 🚀 部署流程

### 步驟 1: 準備環境

**硬體需求:**
```
Master VM:  2 vCPU, 4GB RAM, 50GB SSD  
Slave VM:   1 vCPU, 2GB RAM, 20GB SSD (x5)
網路:       每台 VM 獨立公網 IP
```

**軟體需求:**
```bash
# 每台 VM 都需要
sudo apt update
sudo apt install -y docker.io docker-compose-plugin curl jq
sudo usermod -aG docker $USER
```

### 步驟 2: 部署 Master VM

```bash
# 下載專案到 Master VM
git clone <your-repo>
cd AutoTraderV2/DistributedSystem/Scripts/deployment

# 編輯配置 (設定密碼、API 金鑰等)
vim ../../Config/master/master_full_collection.env

# 執行全量收集部署
./deploy_full_collection.sh 10.0.1.100 5
#                            │       │
#                            │       └─ Slave 數量
#                            └─ Master VM IP
```

**部署後檢查:**
```bash
# 檢查服務狀態
docker-compose -f docker-compose.master.yml ps

# 檢查 Master API
curl http://localhost:8080/api/status | jq .

# 檢查 MongoDB
docker exec shared-mongo mongosh --eval "db.runCommand('ping')"

# 查看 Dashboard
open http://master-vm-ip:8080/dashboard.html
```

### 步驟 3: 部署 Slave VMs

```bash
# 在每台 Slave VM 執行

# 從 Master 複製配置檔案
scp master-vm:/path/to/DistributedSystem/Config/slaves/slave-1.env ./
scp master-vm:/path/to/DistributedSystem/Scripts/deployment/docker-compose.slave.yml ./  
scp master-vm:/path/to/DistributedSystem/Scripts/deployment/deploy_slave.sh ./

# 部署 Slave 服務
./deploy_slave.sh slave-1  # 根據 VM 調整編號

# 檢查狀態
curl http://localhost:8081/health | jq .
```

**批次部署 (可選):**
```bash
# 在 Master VM 上使用自動生成的腳本
cd ../../Config/slaves
./deploy_all_slaves.sh
```

## 📊 負載分配與監控

### 預期負載分配

```
總 Symbols: ~400 個
分配策略: 平均分配到 5 台 VM

Slave-1: ~80 symbols (高交易量)
Slave-2: ~80 symbols  
Slave-3: ~80 symbols
Slave-4: ~80 symbols
Slave-5: ~80 symbols (相對低交易量)

每 Slave 每分鐘: ~400 API 請求
Binance 限制: 1200 請求/分鐘/IP
安全邊際: 66% 使用率 ✅
```

### 即時監控

**Master Dashboard:**
```
http://master-vm-ip:8080/dashboard.html

顯示資訊:
- 所有 Slave 狀態 (線上/離線)
- 每 Slave 處理的 symbols 數量
- CPU/記憶體使用率
- 錯誤率統計
- 資料收集速率
```

**API 監控:**
```bash
# 系統總覽
curl http://master-vm:8080/api/status | jq .

# 所有 Slave 狀態
curl http://master-vm:8080/api/slaves | jq .

# 特定 Slave 健康檢查
curl http://slave-vm-1:8081/health | jq .
```

**MongoDB 資料監控:**
```javascript
// 連接到 Master MongoDB
mongosh mongodb://master-vm:27017/trading_data

// 查看資料量統計
db.market_data.countDocuments()

// 各 Slave 收集統計
db.market_data.aggregate([
  {"$group": {"_id": "$collector_id", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}}
])

// 最近一小時資料
db.market_data.countDocuments({
  "timestamp": {"$gte": new Date(Date.now() - 3600000)}
})

// 各 Symbol 資料分佈
db.market_data.aggregate([
  {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}},
  {"$limit": 20}
])
```

## 🔧 故障處理

### 常見問題與解決方案

**1. Slave 無法連接到 Master MongoDB**
```bash
# 檢查網路連通性
telnet master-vm-ip 27017

# 檢查 MongoDB 日誌
docker logs shared-mongo

# 檢查防火牆
sudo ufw status
sudo ufw allow from slave-vm-ip to any port 27017
```

**2. API 請求過多，觸發限制**
```bash
# 調整收集頻率 (在 slave 配置中)
FETCH_INTERVAL=90  # 改為90秒
RATE_LIMIT_DELAY=0.2  # 增加請求間隔

# 或增加更多 Slave VM 分散負載
```

**3. Slave 離線或故障**
```bash
# Master 會自動偵測並重新分配
# 檢查 Master 日誌
docker-compose -f docker-compose.master.yml logs master-coordinator

# 手動重啟 Slave
docker-compose -f docker-compose.slave.yml restart
```

**4. MongoDB 連接過多**
```bash
# 調整連接池設定 (在 Slave 程式碼中)
maxPoolSize=10  # 降低連接池大小
minPoolSize=2
```

## 📈 效能調優

### 1. 資料庫優化

```javascript
// 建立索引加速查詢
db.market_data.createIndex({"symbol": 1, "timestamp": -1})
db.market_data.createIndex({"timestamp": -1})
db.market_data.createIndex({"collector_id": 1})

// 設定 TTL 自動清理舊資料 (30天)
db.market_data.createIndex(
  {"timestamp": 1}, 
  {"expireAfterSeconds": 2592000}
)
```

### 2. 系統資源優化

```yaml
# 調整 Docker 資源限制
deploy:
  resources:
    limits:
      memory: 1.5G  # 增加記憶體
      cpus: '1.0'   # 增加 CPU
```

### 3. 網路優化

```bash
# 調整 TCP 參數
echo 'net.ipv4.tcp_tw_reuse = 1' >> /etc/sysctl.conf
echo 'net.core.somaxconn = 1024' >> /etc/sysctl.conf
sysctl -p
```

## 💰 成本分析

**Azure 雲端成本 (台幣/月):**
```
Master VM (B2s):     600
Slave VM (B1s) x5:  1,250  
總計:               1,850
```

**對比傳統方案:**
```
單VM + 5個NAT Gateway: ~4,500/月
節省成本: 59% ⭐
```

**擴展成本:**
```
增加到10台Slave: +1,250/月
處理能力: 雙倍
成本效益: 極佳
```

## 🔄 維護建議

### 每日檢查
```bash
# 自動化健康檢查腳本
#!/bin/bash
echo "=== AutoTrader 每日健康檢查 ==="
curl -s http://master-vm:8080/api/status | jq '.online_slaves, .total_symbols'
echo "MongoDB 連接測試..."
docker exec shared-mongo mongosh --eval "db.runCommand('ping')" > /dev/null && echo "✅ OK" || echo "❌ Failed"
```

### 每週維護
```bash
# 清理 Docker 映像和容器
docker system prune -f

# 檢查磁碟空間
df -h

# 檢查 MongoDB 資料大小
docker exec shared-mongo mongosh --eval "db.stats()"
```

### 監控告警設定
```bash
# 設定 CPU/記憶體告警 (可整合 Prometheus + Grafana)
# 設定 API 錯誤率告警
# 設定 MongoDB 連接失敗告警
```

## 🎯 總結

這個全量收集系統具備以下優勢：

✅ **完整性**: 收集所有 Binance Perpetual Contracts  
✅ **擴展性**: 可輕鬆增加更多 Slave VM  
✅ **穩定性**: 自動故障檢測和重新分配  
✅ **經濟性**: 比 NAT Gateway 方案節省 59% 成本  
✅ **監控性**: 完整的即時監控和告警系統  

透過這個架構，你可以：
- 收集 400+ symbols 的完整市場資料
- 每分鐘獲得最新的價格、成交量、資金費率等資訊
- 透過 WebSocket 獲得即時的 K線和清算資料
- 在單一 MongoDB 中查詢所有歷史資料
- 透過 Dashboard 監控整個系統狀態

系統設計考量了 Binance API 限制、網路穩定性、資料一致性等因素，提供了可靠的企業級解決方案。