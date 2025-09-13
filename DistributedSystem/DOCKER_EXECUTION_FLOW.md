# 🐳 Docker 容器執行流程詳解

## 📋 Docker 容器啟動順序和程式呼叫流程

### 🎯 **整體 Docker 啟動流程概覽**

```
docker-compose up -d 執行順序:

1. 讀取 docker-compose.yml
2. 建立 Docker 網路 (autotrader-vnet)
3. 創建 Docker 卷 (mongodb_data)
4. 按依賴順序啟動容器:

┌─────────────────┐
│   MongoDB       │ ← 1. 首先啟動 (其他服務依賴它)
│   Container     │
└─────────┬───────┘
          │ depends_on
          ▼
┌─────────────────┐
│   Master        │ ← 2. MongoDB 啟動後啟動
│   Container     │
└─────────┬───────┘
          │ depends_on  
          ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Slave-1       │    │   Slave-2       │    │   Slave-3       │ ← 3. Master 啟動後並行啟動
│   Container     │    │   Container     │    │   Container     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## 🚀 **各容器的程式進入點詳解**

### **1. Master 容器啟動流程**

#### **Dockerfile 分析**: `DistributedSystem/MasterVM/Dockerfile`
```dockerfile
FROM python:3.11-slim

# 系統依賴安裝
RUN apt-get update && apt-get install -y curl netcat-openbsd

WORKDIR /app

# Python 依賴安裝
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式碼
COPY src/ src/
COPY config/ config/
COPY templates/ templates/
COPY ../Common/ Common/

EXPOSE 8080

# 🔥 關鍵: 容器啟動時執行的指令
CMD ["python", "src/master_coordinator.py"]
```

#### **Master 容器程式執行流程**:
```python
# 🎯 程式進入點: src/master_coordinator.py

if __name__ == "__main__":
    """
    Docker 容器啟動時會執行到這裡
    """
    print("🚀 Starting Master Coordinator...")
    
    # 1. 建立 Master 實例
    coordinator = MasterCoordinator()
    
    # 2. 啟動異步事件循環
    try:
        asyncio.run(coordinator.start_server())
    except KeyboardInterrupt:
        print("Master Coordinator stopped")
    except Exception as e:
        print(f"Master Coordinator error: {e}")
        sys.exit(1)
```

#### **Docker 環境變數注入**:
```yaml
# docker-compose.yml 中的環境變數
master:
  environment:
    - MASTER_PORT=8080                          # ← 注入到容器
    - MONGO_URI=mongodb://mongodb:27017/        # ← 注入到容器
    - MONGO_DB_NAME=trading_data_test           # ← 注入到容器
    - NUM_SLAVES=3                              # ← 注入到容器
    - TEST_MODE=true                            # ← 注入到容器
    - LOG_LEVEL=DEBUG                           # ← 注入到容器

# 在 Python 程式中讀取:
master_port = int(os.getenv('MASTER_PORT', 8080))
mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
```

---

### **2. Slave 容器啟動流程**

#### **Dockerfile 分析**: `DistributedSystem/SlaveVM/data_fetcher/Dockerfile`
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# 複製依賴和程式碼
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製原始 DataFetcher (依賴)
COPY ../../../DataFetcher/ DataFetcher/

# 複製分散式資料收集器
COPY distributed_data_fetcher.py .
COPY enhanced_data_fetcher.py .

# 複製共用模組
COPY ../../Common/ Common/

# 設定 Python 路徑
ENV PYTHONPATH="/app:/app/DataFetcher"
ENV PYTHONUNBUFFERED=1

# 🔥 關鍵: 容器啟動時執行的指令
CMD ["python", "distributed_data_fetcher.py"]
```

#### **Slave 容器程式執行流程**:
```python
# 🎯 程式進入點: distributed_data_fetcher.py

if __name__ == "__main__":
    """
    Docker 容器啟動時會執行到這裡
    """
    print("🤖 Starting Distributed Data Fetcher...")
    
    # 1. 從環境變數讀取配置
    slave_id = os.getenv('SLAVE_ID', 'slave-unknown')
    master_url = os.getenv('MASTER_URL', 'http://master:8080')
    symbols_str = os.getenv('SYMBOLS', '')
    symbols = [s.strip() for s in symbols_str.split(',') if s.strip()]
    
    print(f"Slave ID: {slave_id}")
    print(f"Master URL: {master_url}")  
    print(f"Assigned Symbols: {len(symbols)}")
    
    # 2. 建立分散式資料收集器
    fetcher = DistributedDataFetcher(
        slave_id=slave_id,
        symbols=symbols,
        master_url=master_url,
        mongo_uri=os.getenv('MONGO_URI'),
        mongo_db_name=os.getenv('MONGO_DB_NAME')
    )
    
    # 3. 啟動收集器主循環
    try:
        fetcher.run_distributed_fetcher()  # ← 主要執行邏輯
    except KeyboardInterrupt:
        print(f"Slave {slave_id} stopped")
    except Exception as e:
        print(f"Slave {slave_id} error: {e}")
        sys.exit(1)
```

#### **Slave 環境變數配置**:
```yaml
# docker-compose.yml 中每個 Slave 的配置
slave-1:
  environment:
    - SLAVE_ID=slave-1                          # ← 唯一識別
    - MASTER_URL=http://master:8080             # ← Master 連接位址
    - MONGO_URI=mongodb://mongodb:27017/        # ← MongoDB 連接
    - MONGO_DB_NAME=trading_data_test           # ← 資料庫名稱
    - HEALTH_PORT=8081                          # ← 健康檢查端口
    - SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,...       # ← 分配的 symbols
    - TEST_MODE=true
    - LOG_LEVEL=DEBUG
```

---

## 🔄 **詳細容器間通訊流程**

### **容器網路通訊架構**

```
Docker Network: autotrader-vnet (bridge)

┌──────────────────────────────────────────────────────────┐
│                  Docker 內部網路                          │
│                                                          │
│  ┌─────────────────┐    ┌─────────────────┐             │
│  │   mongodb       │    │     master      │             │
│  │   (容器名稱)     │    │   (容器名稱)     │             │
│  │                 │    │                 │             │
│  │ 內部IP: 自動分配 │◄───┤ 內部IP: 自動分配 │             │
│  │ 端口: 27017     │    │ 端口: 8080      │             │
│  └─────────────────┘    └─────────┬───────┘             │
│           ▲                       │                     │
│           │                       ▼                     │
│  ┌─────────┴───────┐    ┌─────────────────┐             │
│  │    slave-1      │    │    slave-2      │             │
│  │   (容器名稱)     │    │   (容器名稱)     │             │
│  │                 │    │                 │             │
│  │ 內部IP: 自動分配 │    │ 內部IP: 自動分配 │             │
│  │ 端口: 8081      │    │ 端口: 8081      │             │
│  └─────────────────┘    └─────────────────┘             │
└──────────────────────────────────────────────────────────┘
```

### **容器間 HTTP 通訊實例**

#### **Slave 註冊到 Master**:
```python
# Slave 容器中的程式碼
def register_with_master(self):
    registration_data = {
        "slave_id": self.slave_id,
        "ip_address": self.get_container_ip(),
        "assigned_symbols": self.assigned_symbols,
        "status": "online"
    }
    
    # 🌐 容器間 HTTP 通訊
    # master:8080 會由 Docker 自動解析為 master 容器的內部 IP
    response = requests.post(
        f"{self.master_url}/api/register",  # http://master:8080/api/register
        json=registration_data,
        timeout=10
    )
```

#### **Slave 連接 MongoDB**:
```python
# Slave 容器中的程式碼
def connect_to_database(self):
    # mongodb:27017 會由 Docker 自動解析為 mongodb 容器的內部 IP
    mongo_uri = "mongodb://mongodb:27017/"
    self.mongo_client = MongoClient(mongo_uri)
    self.db = self.mongo_client[self.mongo_db_name]
```

---

## ⚙️ **容器啟動時序和依賴管理**

### **Docker Compose 依賴順序**

```yaml
# docker-compose.yml 依賴配置

services:
  mongodb:
    # 沒有 depends_on，最先啟動
    
  master:
    depends_on:
      - mongodb  # ← 等待 mongodb 啟動完成
      
  slave-1:
    depends_on:
      - master   # ← 等待 master 啟動完成
      
  slave-2:
    depends_on:
      - master   # ← 等待 master 啟動完成
      
  slave-3:
    depends_on:
      - master   # ← 等待 master 啟動完成
```

### **實際啟動時序**:

```
時間軸 (docker-compose up -d):

T=0s:   開始啟動 mongodb 容器
T=2s:   mongodb 容器啟動完成，開始啟動 master 容器
T=5s:   master 容器啟動完成，並行啟動所有 slave 容器
T=8s:   所有 slave 容器啟動完成

程式執行時序:

T=0s:   mongodb 進程啟動，監聽 27017 端口
T=2s:   master_coordinator.py 開始執行
        ├─ 讀取環境變數
        ├─ 初始化 MasterCoordinator
        ├─ 連接 MongoDB (mongodb:27017)
        ├─ 啟動 HTTP 服務器 (0.0.0.0:8080)
        └─ 開始監控 slaves 背景任務

T=5s:   distributed_data_fetcher.py 開始執行 (3個 slave 並行)
        ├─ 讀取環境變數 (SLAVE_ID, SYMBOLS, MASTER_URL)
        ├─ 初始化 DistributedDataFetcher
        ├─ 向 Master 註冊 (POST master:8080/api/register)
        ├─ 連接 MongoDB (mongodb:27017)  
        ├─ 啟動心跳執行緒 (每30秒發送到 master:8080)
        └─ 開始資料收集主循環
```

---

## 🔧 **容器健康檢查機制**

### **Master 容器健康檢查**:
```dockerfile
# MasterVM/Dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/api/status || exit 1
```

**執行邏輯**:
```bash
# Docker 每30秒執行一次健康檢查
curl -f http://localhost:8080/api/status

# 如果回傳成功 (HTTP 200)，容器標記為 healthy
# 如果失敗3次，容器標記為 unhealthy
```

### **Slave 容器健康檢查**:
```dockerfile  
# SlaveVM/data_fetcher/Dockerfile
HEALTHCHECK --interval=60s --timeout=30s --start-period=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8081/health', timeout=5)" || exit 1
```

---

## 📊 **容器監控和日誌**

### **查看容器狀態**:
```bash
# 查看所有容器狀態
docker-compose ps

# 查看容器健康狀態
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# 查看容器資源使用
docker stats autotrader-master autotrader-slave-1
```

### **查看容器日誌**:
```bash
# 查看 Master 日誌
docker-compose logs -f master

# 查看 Slave 日誌
docker-compose logs -f slave-1

# 查看所有容器日誌
docker-compose logs -f
```

### **進入容器調試**:
```bash
# 進入 Master 容器
docker exec -it autotrader-master bash

# 進入 Slave 容器
docker exec -it autotrader-slave-1 bash

# 在容器內檢查進程
docker exec -it autotrader-master ps aux

# 在容器內測試網路連通性
docker exec -it autotrader-slave-1 ping master
docker exec -it autotrader-slave-1 ping mongodb
```

---

## 🐛 **常見問題和除錯**

### **1. 容器無法啟動**
```bash
# 檢查容器啟動錯誤
docker-compose logs master

# 檢查端口衝突
netstat -tlnp | grep -E "(8080|8081|27017)"

# 重新構建容器
docker-compose build --no-cache
```

### **2. 容器間無法通訊**
```bash
# 檢查 Docker 網路
docker network ls
docker network inspect autotrader-vnet

# 測試容器間連通性
docker exec -it autotrader-slave-1 ping master
docker exec -it autotrader-slave-1 telnet master 8080
```

### **3. 程式執行錯誤**
```bash
# 檢查環境變數是否正確注入
docker exec -it autotrader-slave-1 env | grep -E "(SLAVE_ID|SYMBOLS|MASTER_URL)"

# 檢查 Python 模組路徑
docker exec -it autotrader-slave-1 python -c "import sys; print('\n'.join(sys.path))"

# 手動執行程式測試
docker exec -it autotrader-slave-1 python distributed_data_fetcher.py
```

---

## 🎯 **Docker 執行流程總結**

### **簡化版流程**:
1. **docker-compose up -d** → 讀取 YAML 配置
2. **Docker 創建網路和卷** → 準備基礎設施  
3. **按依賴順序啟動容器** → mongodb → master → slaves
4. **容器內程式開始執行** → Python 腳本啟動
5. **程式從環境變數讀取配置** → 動態配置
6. **開始業務邏輯** → 註冊、心跳、資料收集

### **關鍵理解點**:
- 🐳 **容器 = 隔離的執行環境**，每個容器運行一個主要程式
- 🌐 **容器名稱 = 網路主機名**，可以直接用於內部通訊
- 📦 **環境變數 = 配置注入**，動態配置程式行為
- 🔗 **depends_on = 啟動順序**，確保依賴服務先啟動
- ❤️ **健康檢查 = 容器狀態監控**，自動檢測程式是否正常運行

這樣你就完全理解了 Docker 容器中的程式是如何被啟動和執行的！