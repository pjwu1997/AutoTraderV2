# 分散式資料收集系統

## 🏗️ 系統架構

此系統將原本的單機資料收集分散到多台 VM，每台具有獨立 IP 以避免 Binance API 限制。

```
┌─────────────────┐
│   Master VM     │ ← 監控協調中心
│ (監控 & 協調)    │   - 分配 symbols
└────────┬────────┘   - 監控健康狀況
         │            - 提供 Dashboard
    ┌────┴────────────────────────┐
    │                             │
┌───▼───┐ ┌───────┐ ┌───────┐ ┌──▼────┐ ┌───────┐
│Slave-1│ │Slave-2│ │Slave-3│ │Slave-4│ │Slave-5│
│50票據  │ │50票據  │ │50票據  │ │50票據  │ │50票據  │
│獨立IP  │ │獨立IP  │ │獨立IP  │ │獨立IP  │ │獨立IP  │
└───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘
    │         │         │         │         │
    └─────────┴─────────┴─────────┴─────────┘
                        │
               ┌────────▼────────┐
               │  Shared MongoDB │ ← 所有資料彙整
               │(資料彙整中心)    │
               └─────────────────┘
```

## 📁 目錄結構

```
DistributedSystem/
├── MasterVM/                    # Master VM 程式碼
│   ├── src/master_coordinator.py
│   ├── Dockerfile
│   └── requirements.txt
├── SlaveVM/                     # Slave VM 程式碼
│   ├── data_fetcher/            # REST API 資料收集
│   │   ├── distributed_data_fetcher.py
│   │   ├── enhanced_funding_collector.py
│   │   ├── schema_compatible_collector.py
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── websockets/              # WebSocket 即時資料
│   │   ├── websocket_controller.py
│   │   ├── kline_websocket.py
│   │   ├── liquidation_websocket.py
│   │   └── Dockerfile
│   └── health_checker/
│       ├── slave_health.py
│       └── Dockerfile
├── Common/                      # 共用模組
│   ├── models/data_models.py
│   └── utils/symbol_distributor.py
├── Config/                      # 配置檔案
│   ├── master/master.env
│   └── slaves/                  # 自動生成
├── Scripts/                     # 部署和監控腳本
│   ├── deployment/
│   │   ├── docker-compose.master.yml
│   │   ├── docker-compose.slave.yml
│   │   ├── deploy_master.sh
│   │   └── deploy_slave.sh
│   └── monitoring/
│       └── system_status.py
└── README.md
```

## 🚀 部署流程

### 1. 準備 VM 環境

**硬體需求:**
- **1 台 Master VM**: 2 vCPU, 4GB RAM (監控用)
- **5 台 Slave VM**: 1 vCPU, 2GB RAM (資料收集用)
- **每台 VM 必須有獨立的公網 IP**

**軟體需求:**
```bash
# 每台 VM 都需要安裝
sudo apt update
sudo apt install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
```

### 2. 部署 Master VM

```bash
# 在 Master VM 上執行
cd DistributedSystem/Scripts/deployment

# 編輯主配置
vim ../../Config/master/master.env

# 部署 Master 服務
./deploy_master.sh
```

部署完成後：
- Dashboard: http://master-vm:8080/dashboard.html
- API: http://master-vm:8080/api/status
- MongoDB: master-vm:27017

### 3. 部署 Slave VMs

```bash
# 在每台 Slave VM 上執行
cd DistributedSystem/Scripts/deployment

# 從 Master 複製配置檔案
scp master-vm:~/DistributedSystem/Config/slaves/slave-1.env ../../Config/slaves/

# 部署 Slave 服務 (根據 VM 調整 slave-id)
./deploy_slave.sh slave-1  # 第一台
./deploy_slave.sh slave-2  # 第二台
./deploy_slave.sh slave-3  # 第三台
./deploy_slave.sh slave-4  # 第四台
./deploy_slave.sh slave-5  # 第五台
```

## 📊 功能特色

### 完整資料收集 (1分鐘精度)
每台 Slave VM 會收集分配 symbols 的所有資料：
- **OHLCV**: 1分鐘K線資料 (WebSocket即時 + REST API備份)
- **CVD**: Spot + Futures CVD (1分鐘精度)
- **Funding Rate**: 當前費率 + 下一期費率 + Mark Price + Index Price
- **Long/Short Ratios**: 4種不同類型的多空比 (全域、頂級交易者等)
- **Liquidations**: 即時清算事件 (WebSocket 1分鐘聚合)
- **Open Interest**: 當前 + 變化 + 趨勢分析

### 監控和健康檢查
- **Master Dashboard**: 即時監控所有 Slave 狀態
- **自動心跳**: 每30秒檢查 Slave 健康狀況
- **故障轉移**: 自動重新分配失敗 Slave 的工作
- **資源監控**: CPU、記憶體、磁碟使用率

### 彈性擴展
- **動態分配**: 自動計算最佳 Symbol 分配
- **負載平衡**: 根據 VM 效能調整負載
- **水平擴展**: 可輕鬆增加更多 Slave VM

## 🔧 管理指令

### 系統狀態檢查
```bash
# 檢查整體系統狀態
python Scripts/monitoring/system_status.py http://master-vm:8080

# 檢查 Master 狀態
curl http://master-vm:8080/api/status | jq

# 檢查特定 Slave 狀態  
curl http://slave-1:8081/health | jq
```

### 服務管理
```bash
# Master VM
docker-compose -f docker-compose.master.yml logs
docker-compose -f docker-compose.master.yml restart
docker-compose -f docker-compose.master.yml down

# Slave VM (4個服務容器)
docker-compose -f docker-compose.slave.yml logs
docker-compose -f docker-compose.slave.yml logs data-fetcher
docker-compose -f docker-compose.slave.yml logs kline-websocket
docker-compose -f docker-compose.slave.yml logs liquidation-websocket
docker-compose -f docker-compose.slave.yml logs health-checker
docker-compose -f docker-compose.slave.yml restart data-fetcher
docker-compose -f docker-compose.slave.yml down
```

### MongoDB 查詢
```bash
# 連接到共享 MongoDB
docker exec -it shared-mongo mongosh

# 查看資料統計
db.market_data.countDocuments()
db.market_data.find().limit(5)

# 查看各 Symbol 資料量
db.market_data.aggregate([
  {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}}
])
```

## 💰 成本分析 (Azure)

| 項目 | 規格 | 數量 | 月費 (NT$) |
|------|------|------|-----------|
| Master VM | B2s (2C4G) | 1 | 600 |
| Slave VM | B1s (1C1G) | 5 | 1,250 |
| MongoDB VM | B2s (2C4G) | 1 | 600 |
| **總計** | | | **2,450** |

**對比方案:**
- 單VM + 5個NAT Gateway: ~4,000-5,000/月
- 節省成本: **40-50%**

## 🚨 故障排除

### Master 無法啟動
```bash
# 檢查配置
cat Config/master/master.env

# 檢查 MongoDB 連接
docker exec shared-mongo mongosh --eval "db.runCommand('ping')"

# 檢查端口占用
netstat -tulpn | grep 8080
```

### Slave 無法連接到 Master
```bash
# 測試網路連通性
curl http://master-vm:8080/api/status

# 檢查防火牆
sudo ufw status
```

### 資料收集停止
```bash
# 檢查所有服務狀態
docker-compose -f docker-compose.slave.yml ps

# 檢查各服務日誌
docker-compose -f docker-compose.slave.yml logs data-fetcher
docker-compose -f docker-compose.slave.yml logs kline-websocket  
docker-compose -f docker-compose.slave.yml logs liquidation-websocket

# 檢查 WebSocket 連接
curl http://slave-ip:8081/health  # 健康檢查
tail -f logs/kline_websocket.log  # WebSocket 日誌

# 檢查 API 配額
# Binance API 每分鐘限制 1200 請求/IP
```

## 🔄 維護建議

1. **定期備份**: MongoDB 每日自動備份
2. **監控告警**: 設定 CPU/記憶體使用率告警
3. **日誌輪轉**: 避免日誌檔案過大
4. **安全更新**: 定期更新 Docker images
5. **效能調優**: 根據實際負載調整 Symbol 分配

## 📈 擴展方案

當需要處理更多 Symbols 時：

1. **增加 Slave VM**: 修改 `NUM_SLAVES` 參數
2. **升級 VM 規格**: 提升單台處理能力  
3. **區域分散**: 在不同區域部署減少延遲
4. **資料庫分片**: MongoDB 分片提升寫入效能