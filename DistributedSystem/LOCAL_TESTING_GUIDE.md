# 🧪 AutoTrader 本地測試指南

## 🎯 本地測試概覽

在本地模擬分散式系統，使用 Docker 容器代替多台 VM，讓你可以在部署到 Azure 之前完整測試所有功能。

## 🏗️ 本地架構設計

```
本地 Docker 環境
┌─────────────────────────────────────┐
│  Docker Network: autotrader-local   │
│                                     │
│  ┌─────────────────┐               │
│  │   Master         │               │
│  │ - API :8080     │               │
│  │ - MongoDB :27017│               │
│  └─────────┬───────┘               │
│            │                       │
│    ┌───────┼───────────────────┐   │
│    │       │                   │   │
│ ┌──▼──┐ ┌──▼──┐ ┌──▼──┐ ┌────▼─┐ │
│ │Slave│ │Slave│ │Slave│ │...   │ │
│ │  1  │ │  2  │ │  3  │ │      │ │
│ │:8081│ │:8082│ │:8083│ │      │ │
│ └─────┘ └─────┘ └─────┘ └──────┘ │
└─────────────────────────────────────┘
```

---

## 📝 設置步驟

### **1. 準備本地環境**

#### 檢查 Docker 安裝
```bash
# 檢查 Docker 版本
docker --version
docker compose version

# 如果沒有安裝 Docker，請先安裝
# macOS: brew install docker
# Ubuntu: sudo apt install docker.io docker-compose-plugin
```

#### 確認系統資源
```bash
# 確認系統有足夠資源
# 建議: 至少 8GB RAM, 4 CPU cores
free -h  # Linux
# 或 Activity Monitor (macOS)
```

### **2. 創建本地配置檔案**

#### 創建測試用環境變數
```bash
# 在專案根目錄創建本地測試配置
cd /Users/pj/Desktop/projects/AutoTraderV2
```

#### 本地 Master 配置
創建 `DistributedSystem/Config/local/master-local.env`:
```bash
# Master 本地測試配置
MASTER_PORT=8080
MASTER_VM_IP=master
MASTER_HOSTNAME=master

# MongoDB 配置 (容器內部)
MONGO_URI=mongodb://mongodb:27017/
MONGO_DB_NAME=trading_data_test
MONGO_ROOT_USER=admin
MONGO_ROOT_PASSWORD=test123456
MONGO_AUTH_SOURCE=admin

# 測試用 Binance API (可選)
BINANCE_API_KEY=your_test_api_key
BINANCE_API_SECRET=your_test_secret

# 縮小測試範圍
NUM_SLAVES=3
TOTAL_SYMBOLS_LIMIT=50  # 只測試 50 個 symbols
TEST_MODE=true

# 日誌設定
LOG_LEVEL=DEBUG
HEARTBEAT_INTERVAL=10  # 更頻繁的心跳檢查
HEARTBEAT_TIMEOUT=30
```

### **3. 創建本地 Docker Compose**

#### 創建本地測試用 docker-compose
創建 `DistributedSystem/docker-compose.local.yml`:
```yaml
# 本地測試 Docker Compose 配置
version: '3.8'

services:
  # MongoDB - 資料庫
  mongodb:
    image: mongo:7.0
    container_name: autotrader-mongo-local
    ports:
      - "27017:27017"
    environment:
      - MONGO_INITDB_ROOT_USERNAME=admin
      - MONGO_INITDB_ROOT_PASSWORD=test123456
      - MONGO_INITDB_DATABASE=trading_data_test
    volumes:
      - mongodb_local_data:/data/db
      - ./Scripts/deployment/mongodb.conf:/etc/mongod.conf:ro
    networks:
      - autotrader-local
    restart: unless-stopped

  # Master 協調器
  master:
    build:
      context: ./MasterVM
      dockerfile: Dockerfile
    container_name: autotrader-master-local
    ports:
      - "8080:8080"
    environment:
      - MASTER_PORT=8080
      - MONGO_URI=mongodb://mongodb:27017/
      - MONGO_DB_NAME=trading_data_test
      - MONGO_ROOT_USER=admin
      - MONGO_ROOT_PASSWORD=test123456
      - NUM_SLAVES=3
      - TEST_MODE=true
      - LOG_LEVEL=DEBUG
    volumes:
      - ./Config/local:/app/config:ro
      - ./MasterVM/templates:/app/templates:ro
    networks:
      - autotrader-local
    depends_on:
      - mongodb
    restart: unless-stopped

  # Slave-1 資料收集器
  slave-1:
    build:
      context: ./SlaveVM/data_fetcher
      dockerfile: Dockerfile
    container_name: autotrader-slave-1-local
    ports:
      - "8081:8081"
    environment:
      - SLAVE_ID=slave-1
      - MASTER_URL=http://master:8080
      - MONGO_URI=mongodb://mongodb:27017/
      - MONGO_DB_NAME=trading_data_test
      - HEALTH_PORT=8081
      - TEST_MODE=true
      - LOG_LEVEL=DEBUG
      # 測試用少量 symbols
      - SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,ADAUSDT,XRPUSDT,DOTUSDT,LINKUSDT,UNIUSDT,LTCUSDT,BCHUSDT,AVAXUSDT,ATOMUSDT,ALGOUSDT,VETUSDT,MATICUSDT,FILUSDT,AAVEUSDT
    networks:
      - autotrader-local
    depends_on:
      - master
    restart: unless-stopped

  # Slave-2 資料收集器
  slave-2:
    build:
      context: ./SlaveVM/data_fetcher
      dockerfile: Dockerfile
    container_name: autotrader-slave-2-local
    ports:
      - "8082:8081"  # 映射到不同端口避免衝突
    environment:
      - SLAVE_ID=slave-2
      - MASTER_URL=http://master:8080
      - MONGO_URI=mongodb://mongodb:27017/
      - MONGO_DB_NAME=trading_data_test
      - HEALTH_PORT=8081
      - TEST_MODE=true
      - LOG_LEVEL=DEBUG
      # 測試用少量 symbols
      - SYMBOLS=BNBUSDT,TRXUSDT,EOSUSDT,XLMUSDT,XMRUSDT,DASHUSDT,ETCUSDT,IOTAUSDT,NEOUSDT,ONTUSDT,QTUMUSDT,ICXUSDT,LSKUSDT,NANOUSDT,ZILUSDT,BATUSDT,ENJUSDT
    networks:
      - autotrader-local
    depends_on:
      - master
    restart: unless-stopped

  # Slave-3 資料收集器
  slave-3:
    build:
      context: ./SlaveVM/data_fetcher
      dockerfile: Dockerfile
    container_name: autotrader-slave-3-local
    ports:
      - "8083:8081"  # 映射到不同端口避免衝突
    environment:
      - SLAVE_ID=slave-3
      - MASTER_URL=http://master:8080
      - MONGO_URI=mongodb://mongodb:27017/
      - MONGO_DB_NAME=trading_data_test
      - HEALTH_PORT=8081
      - TEST_MODE=true
      - LOG_LEVEL=DEBUG
      # 測試用少量 symbols
      - SYMBOLS=CHZUSDT,HBARUSDT,STXUSDT,CRVUSDT,COMPUSDT,YFIUSDT,SNXUSDT,UMAUSDT,BALUSDT,CVCUSDT,STORJUSDT,KNCUSDT,LRCUSDT,BANDUSDT,RLCUSDT,NMRUSDT
    networks:
      - autotrader-local
    depends_on:
      - master
    restart: unless-stopped

# 網路配置
networks:
  autotrader-local:
    driver: bridge

# 資料卷配置
volumes:
  mongodb_local_data:
```

### **4. 準備測試數據**

#### 創建測試 symbols 分配腳本
創建 `DistributedSystem/Scripts/testing/generate_test_config.py`:
```python
#!/usr/bin/env python3
"""
生成本地測試用的 symbol 配置
"""
import os
import json

def generate_test_symbols():
    """生成測試用的 symbols 分配"""
    
    # 測試用 symbols (選擇主要的加密貨幣)
    test_symbols = {
        'slave-1': [
            'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 
            'DOTUSDT', 'LINKUSDT', 'UNIUSDT', 'LTCUSDT', 'BCHUSDT',
            'AVAXUSDT', 'ATOMUSDT', 'ALGOUSDT', 'VETUSDT', 'MATICUSDT',
            'FILUSDT', 'AAVEUSDT'
        ],
        'slave-2': [
            'BNBUSDT', 'TRXUSDT', 'EOSUSDT', 'XLMUSDT', 'XMRUSDT',
            'DASHUSDT', 'ETCUSDT', 'IOTAUSDT', 'NEOUSDT', 'ONTUSDT',
            'QTUMUSDT', 'ICXUSDT', 'LSKUSDT', 'NANOUSDT', 'ZILUSDT',
            'BATUSDT', 'ENJUSDT'
        ],
        'slave-3': [
            'CHZUSDT', 'HBARUSDT', 'STXUSDT', 'CRVUSDT', 'COMPUSDT',
            'YFIUSDT', 'SNXUSDT', 'UMAUSDT', 'BALUSDT', 'CVCUSDT',
            'STORJUSDT', 'KNCUSDT', 'LRCUSDT', 'BANDUSDT', 'RLCUSDT',
            'NMRUSDT'
        ]
    }
    
    # 創建配置目錄
    config_dir = '../Config/local'
    os.makedirs(config_dir, exist_ok=True)
    
    # 生成各 slave 的環境變數檔案
    for slave_id, symbols in test_symbols.items():
        env_content = f"""# 本地測試配置 for {slave_id}
SLAVE_ID={slave_id}
SYMBOLS={','.join(symbols)}
SYMBOL_COUNT={len(symbols)}

# Master 連接 (Docker 容器名稱)
MASTER_URL=http://master:8080
MONGO_URI=mongodb://mongodb:27017/
MONGO_DB_NAME=trading_data_test

# 測試設定
TEST_MODE=true
LOG_LEVEL=DEBUG
TIMEFRAME=5m
FETCH_INTERVAL=120  # 2分鐘收集一次 (測試用)
BATCH_SIZE=5        # 小批次測試
RATE_LIMIT_DELAY=1.0  # 較長的延遲避免API限制
MAX_RETRIES=2

# Health Check
HEALTH_PORT=8081
HEARTBEAT_INTERVAL=10
"""
        
        env_file = f"{config_dir}/{slave_id}-local.env"
        with open(env_file, 'w') as f:
            f.write(env_content)
        print(f"✅ 生成 {env_file}")
    
    # 生成測試總覽
    summary = {
        "test_config": "LOCAL_TESTING",
        "total_slaves": len(test_symbols),
        "total_symbols": sum(len(symbols) for symbols in test_symbols.values()),
        "symbols_per_slave": {
            slave_id: {
                "count": len(symbols),
                "symbols": symbols
            }
            for slave_id, symbols in test_symbols.items()
        }
    }
    
    summary_file = f"{config_dir}/test_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"✅ 生成 {summary_file}")
    
    print(f"\n📊 測試配置摘要:")
    print(f"   - 總 Slaves: {len(test_symbols)}")
    print(f"   - 總 Symbols: {sum(len(symbols) for symbols in test_symbols.values())}")
    print(f"   - 每 Slave 約: {sum(len(symbols) for symbols in test_symbols.values()) // len(test_symbols)} symbols")

if __name__ == "__main__":
    print("🧪 生成本地測試配置...")
    generate_test_symbols()
    print("✅ 完成！")
```

---

## 🚀 啟動本地測試

### **步驟 1: 準備配置**

```bash
# 1. 進入 DistributedSystem 目錄
cd DistributedSystem

# 2. 創建本地配置目錄
mkdir -p Config/local Scripts/testing

# 3. 生成測試配置 (先手動創建上面的 Python 腳本)
python Scripts/testing/generate_test_config.py
```

### **步驟 2: 啟動服務**

```bash
# 構建並啟動所有服務
docker compose -f docker-compose.local.yml up --build -d

# 查看服務狀態
docker compose -f docker-compose.local.yml ps
```

### **步驟 3: 驗證服務**

```bash
# 1. 檢查 Master API
curl http://localhost:8080/api/status | jq

# 2. 檢查各 Slave 健康狀況
curl http://localhost:8081/health | jq  # Slave-1
curl http://localhost:8082/health | jq  # Slave-2  
curl http://localhost:8083/health | jq  # Slave-3

# 3. 檢查 MongoDB 連接
docker exec -it autotrader-mongo-local mongosh \
  --username admin --password test123456 \
  --eval "db.adminCommand('listCollections')"

# 4. 監控日誌
docker compose -f docker-compose.local.yml logs -f master
docker compose -f docker-compose.local.yml logs -f slave-1
```

---

## 📊 測試驗證方法

### **1. 功能測試**

#### Master 協調功能
```bash
# 檢查 Master 狀態
curl http://localhost:8080/api/status

# 期待回應:
{
  "status": "online",
  "slaves": 3,
  "online_slaves": 3,
  "total_symbols": 51
}
```

#### Slave 註冊和心跳
```bash
# 檢查 Slave 註冊狀況
curl http://localhost:8080/api/slaves | jq

# 期待看到 3 個 slaves 都在線上
```

#### 資料收集驗證
```bash
# 進入 MongoDB 檢查資料
docker exec -it autotrader-mongo-local mongosh \
  --username admin --password test123456

# 在 mongosh 中執行:
use trading_data_test
show collections
db.market_data.countDocuments()
db.market_data.findOne()

# 檢查各 symbol 的資料量
db.market_data.aggregate([
  {"$group": {"_id": "$symbol", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}}
])
```

### **2. 壓力測試**

#### 調整收集頻率
```bash
# 修改環境變數測試高頻收集
docker compose -f docker-compose.local.yml exec slave-1 \
  env FETCH_INTERVAL=30 python distributed_data_fetcher.py
```

#### 監控資源使用
```bash
# 檢查容器資源使用
docker stats autotrader-master-local autotrader-slave-1-local autotrader-slave-2-local autotrader-slave-3-local

# 檢查網路流量
docker compose -f docker-compose.local.yml exec master netstat -i
```

### **3. 故障測試**

#### 模擬 Slave 故障
```bash
# 停止一個 Slave
docker compose -f docker-compose.local.yml stop slave-2

# 檢查 Master 如何處理
curl http://localhost:8080/api/slaves | jq

# 重啟 Slave
docker compose -f docker-compose.local.yml start slave-2
```

#### 模擬網路故障
```bash
# 斷開 Slave 與 Master 的連接 (高級)
docker network disconnect autotrader-local autotrader-slave-3-local

# 檢查系統反應
curl http://localhost:8080/api/status

# 重新連接
docker network connect autotrader-local autotrader-slave-3-local
```

---

## 🔧 故障排除

### **常見問題**

#### 1. 容器無法啟動
```bash
# 檢查 Docker 日誌
docker compose -f docker-compose.local.yml logs master
docker compose -f docker-compose.local.yml logs slave-1

# 檢查端口衝突
netstat -tlnp | grep -E "(8080|8081|8082|8083|27017)"

# 清理舊容器
docker compose -f docker-compose.local.yml down -v
docker system prune -f
```

#### 2. MongoDB 連接失敗
```bash
# 檢查 MongoDB 容器
docker exec -it autotrader-mongo-local mongosh \
  --eval "db.adminCommand('ping')"

# 檢查網路連通性
docker compose -f docker-compose.local.yml exec slave-1 \
  ping mongodb
```

#### 3. API 請求失敗
```bash
# 檢查 Binance API 配置
echo "檢查 API 金鑰是否正確設定"

# 測試網路連接
docker compose -f docker-compose.local.yml exec slave-1 \
  curl -s "https://api.binance.com/api/v3/ping"
```

#### 4. 資料未寫入
```bash
# 檢查資料庫權限
docker exec -it autotrader-mongo-local mongosh \
  --username admin --password test123456 \
  --eval "db.runCommand({connectionStatus: 1})"

# 檢查 Slave 日誌中的錯誤
docker compose -f docker-compose.local.yml logs slave-1 | grep -i error
```

---

## 📈 效能優化建議

### **本地測試優化**

1. **減少 Symbol 數量**: 測試時只用 10-20 個主要 symbols
2. **延長收集間隔**: 設定 `FETCH_INTERVAL=300` (5分鐘)
3. **調整批次大小**: 設定 `BATCH_SIZE=3` 避免 API 限制
4. **啟用快取**: 如果可能，使用本地快取減少 API 請求

### **系統資源分配**

```bash
# 限制容器資源使用 (在 docker-compose.local.yml 中)
deploy:
  resources:
    limits:
      memory: 512M
      cpus: '0.5'
    reservations:
      memory: 256M
      cpus: '0.25'
```

---

## 🧹 清理環境

### **停止和清理**

```bash
# 停止所有服務
docker compose -f docker-compose.local.yml down

# 清理資料卷 (會刪除所有資料)
docker compose -f docker-compose.local.yml down -v

# 清理映像檔 (釋放空間)
docker image prune -f

# 完整清理 (小心使用)
docker system prune -af --volumes
```

---

## 📋 本地測試檢查清單

### **部署前檢查**

- [ ] 所有容器成功啟動
- [ ] Master API 回應正常 (http://localhost:8080/api/status)
- [ ] 3個 Slave 都已註冊並在線
- [ ] MongoDB 可以連接且有資料寫入
- [ ] 各 Slave 的健康檢查正常
- [ ] 日誌中無嚴重錯誤
- [ ] 資料收集功能運作正常

### **效能檢查**

- [ ] CPU 使用率 < 70%
- [ ] 記憶體使用率 < 80%
- [ ] API 請求成功率 > 95%
- [ ] 平均回應時間 < 5秒
- [ ] 無記憶體洩漏現象

### **穩定性檢查**

- [ ] 運行 30 分鐘無故障
- [ ] Slave 故障後能自動恢復
- [ ] 網路中斷後能重新連接
- [ ] 資料庫重啟後系統能恢復

---

這個本地測試環境讓你可以在部署到 Azure 之前充分驗證所有功能，確保系統的穩定性和可靠性！