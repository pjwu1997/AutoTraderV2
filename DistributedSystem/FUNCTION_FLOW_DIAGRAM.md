# 🔄 AutoTrader 函數執行流程詳細圖解

## 🎯 **整體系統啟動順序**

```
🚀 系統啟動順序:
1. Master 啟動 → 2. Slaves 啟動 → 3. 資料收集開始

┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Master VM     │    │   Slave VM-1    │    │   Slave VM-N    │
│                 │    │                 │    │                 │
│ 1. 初始化       │    │ 1. 初始化       │    │ 1. 初始化       │
│ 2. 啟動 API     │◄───┤ 2. 註冊到Master │    │ 2. 註冊到Master │
│ 3. 監控 Slaves  │    │ 3. 開始收集     │    │ 3. 開始收集     │
│ 4. 處理心跳     │◄───┤ 4. 發送心跳     │    │ 4. 發送心跳     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  ▼
                    ┌─────────────────────────┐
                    │      MongoDB            │
                    │   (統一資料存儲)        │
                    └─────────────────────────┘
```

---

## 🎮 **Master 協調器詳細流程**

### **檔案**: `MasterVM/src/master_coordinator.py`

```python
# 🔥 Master 啟動完整流程

def main():
    coordinator = MasterCoordinator()  # ← 進入點
    asyncio.run(coordinator.start_server())

class MasterCoordinator:
    def __init__(self):
        """
        📍 初始化流程:
        1. 載入配置 → 2. 初始化變數 → 3. 準備監控
        """
        self.slaves: Dict[str, SlaveInfo] = {}
        self.config = self.load_config()  # ← 載入配置檔案
        
    async def start_server(self):
        """
        🚀 服務器啟動流程:
        """
        # 1. 初始化
        await self.initialize()
        
        # 2. 創建 HTTP 路由
        app = web.Application()
        app.router.add_post('/api/register', self.handle_register)
        app.router.add_post('/api/heartbeat/{slave_id}', self.handle_heartbeat_endpoint)
        app.router.add_get('/api/status', self.handle_status)
        app.router.add_get('/api/slaves', self.handle_slaves)
        
        # 3. 啟動服務器
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.config['master_port'])
        await site.start()
```

### **核心函數執行順序**:

```
MasterCoordinator 執行流程:

1. __init__()
   ├─ load_config() ──→ 讀取配置檔案 (.env 或 JSON)
   ├─ 初始化 self.slaves = {}
   └─ 設定預設值

2. start_server()
   ├─ initialize()
   │  ├─ 建立 HTTP session
   │  └─ 啟動 monitor_slaves() 背景任務 ⚡
   ├─ 設定 API 路由
   └─ 啟動 HTTP 服務器

3. monitor_slaves() [背景循環]
   └─ 每 30 秒檢查:
      ├─ 檢查 Slave 心跳超時
      ├─ 更新 Slave 狀態
      └─ 清理離線 Slaves

4. 處理 Slave 請求:
   ├─ handle_register() ──→ Slave 註冊
   ├─ handle_heartbeat_endpoint() ──→ 心跳處理
   └─ handle_status() ──→ 狀態查詢
```

---

## 🤖 **Slave 資料收集器詳細流程**

### **檔案**: `SlaveVM/data_fetcher/distributed_data_fetcher.py`

```python
# 🔥 Slave 啟動完整流程

def main():
    # 1. 從環境變數讀取配置
    slave_id = os.getenv('SLAVE_ID', 'slave-unknown')
    symbols = os.getenv('SYMBOLS', '').split(',')
    master_url = os.getenv('MASTER_URL', 'http://master:8080')
    
    # 2. 初始化分散式收集器
    fetcher = DistributedDataFetcher(slave_id, symbols, master_url)
    
    # 3. 啟動收集器
    fetcher.run_distributed_fetcher()  # ← 主要進入點

class DistributedDataFetcher:
    def run_distributed_fetcher(self):
        """
        🚀 Slave 主要執行流程
        """
        # 1. 註冊到 Master
        if not self.register_with_master():
            logger.error("Failed to register with master, exiting")
            return
        
        # 2. 啟動心跳任務
        heartbeat_thread = threading.Thread(target=self.heartbeat_loop)
        heartbeat_thread.daemon = True
        heartbeat_thread.start()
        
        # 3. 開始資料收集循環
        self.start_collection_loop()  # ← 主要收集邏輯
```

### **Slave 核心函數執行順序**:

```
DistributedDataFetcher 執行流程:

1. __init__()
   ├─ 設定 slave_id, symbols, master_url
   ├─ 初始化統計變數 (error_count, symbols_processed)
   └─ 建立 EnhancedDataFetcher 實例

2. run_distributed_fetcher()
   ├─ register_with_master() ──→ 向 Master 註冊
   │  └─ POST /api/register (slave_id, symbols, 系統資訊)
   ├─ 啟動 heartbeat_loop() [背景執行緒] ⚡
   │  └─ 每 30 秒發送心跳到 Master
   └─ start_collection_loop() ──→ 主要收集循環

3. start_collection_loop() [主循環]
   └─ 每分鐘執行:
      ├─ collect_all_data_for_symbols() ──→ 批次收集
      ├─ 更新統計數據
      ├─ 錯誤處理和重試
      └─ 休眠等待下次執行

4. collect_all_data_for_symbols()
   └─ 對每個 symbol:
      ├─ data_fetcher.fetch_enhanced_market_data() ⚡
      ├─ 處理收集到的資料
      └─ 存入 MongoDB
```

---

## 📊 **增強資料收集器核心邏輯**

### **檔案**: `SlaveVM/data_fetcher/enhanced_data_fetcher.py`

```python
class EnhancedDataFetcher:
    async def fetch_enhanced_market_data(self, symbol: str):
        """
        🎯 核心資料收集函數 - 這是整個系統的心臟!
        """
        enhanced_data = {
            'symbol': symbol,
            'timestamp': datetime.utcnow(),
            'slave_id': self.slave_id
        }
        
        # 並行收集多種資料
        tasks = [
            self.fetch_ohlcv_data(symbol),      # K線資料
            self.fetch_cvd_data(symbol),        # CVD 計算
            self.fetch_spot_cvd(symbol),        # 現貨 CVD
            self.fetch_long_short_ratio(symbol), # 多空比例
            self.fetch_funding_rate(symbol),    # 資金費率
            self.fetch_liquidation_data(symbol) # 清算資料
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 整合所有資料
        for i, result in enumerate(results):
            if not isinstance(result, Exception):
                enhanced_data.update(result)
        
        return enhanced_data
```

### **資料收集詳細流程**:

```
fetch_enhanced_market_data() 執行流程:

1. 初始化資料結構
   └─ enhanced_data = {'symbol': ..., 'timestamp': ..., 'slave_id': ...}

2. 並行 API 呼叫 (asyncio.gather):
   ├─ fetch_ohlcv_data() ──→ Binance K線 API
   │  └─ GET /api/v3/klines
   ├─ fetch_cvd_data() ──→ 交易資料 + CVD計算
   │  ├─ GET /api/v3/aggTrades
   │  └─ calculate_cvd() ⚡ [核心演算法]
   ├─ fetch_spot_cvd() ──→ 現貨市場 CVD
   │  └─ GET /api/v3/ticker/24hr
   ├─ fetch_long_short_ratio() ──→ 多空比例
   │  └─ GET /futures/data/globalLongShortAccountRatio
   ├─ fetch_funding_rate() ──→ 資金費率
   │  └─ GET /fapi/v1/fundingRate
   └─ fetch_liquidation_data() ──→ 清算資料
      └─ GET /fapi/v1/forceOrders

3. 資料整合與驗證
   ├─ 檢查 API 回應是否有錯誤
   ├─ 合併所有資料到 enhanced_data
   └─ 資料清理和格式化

4. 存儲到 MongoDB
   └─ store_enhanced_data(enhanced_data)
```

---

## 🧮 **CVD 計算演算法詳解**

### **檔案**: `SlaveVM/data_fetcher/enhanced_data_fetcher.py`

```python
def calculate_cvd(self, trades_data):
    """
    💡 CVD (Cumulative Volume Delta) 計算核心邏輯
    
    CVD 公式:
    - 買入量 (Buyers) = 正值 (+)
    - 賣出量 (Sellers) = 負值 (-)
    - CVD = Σ(買入量 - 賣出量)
    """
    
    if not trades_data:
        return 0.0
    
    cvd = 0
    for trade in trades_data:
        volume = float(trade.get('qty', 0))
        price = float(trade.get('price', 0))
        is_buyer_maker = trade.get('isBuyerMaker', False)
        
        # 🔍 關鍵邏輯:
        # isBuyerMaker=True  → 賣出 (taker 買入, maker 賣出)
        # isBuyerMaker=False → 買入 (taker 賣出, maker 買入)
        if is_buyer_maker:
            cvd -= volume  # 賣方主導
        else:
            cvd += volume  # 買方主導
    
    return cvd
```

### **CVD 計算流程圖**:

```
CVD 計算完整流程:

1. fetch_cvd_data(symbol)
   ├─ API 呼叫: GET /api/v3/aggTrades
   │  └─ 參數: symbol, limit=1000, startTime, endTime
   ├─ 取得交易資料 trades_data[]
   └─ calculate_cvd(trades_data) ⚡

2. calculate_cvd() [核心演算法]
   ├─ 初始化 cvd = 0
   ├─ 對每筆交易:
   │  ├─ 取得 qty (交易量)
   │  ├─ 取得 isBuyerMaker (買賣方向)
   │  └─ 計算:
   │     ├─ if isBuyerMaker: cvd -= qty  (賣出)
   │     └─ else: cvd += qty             (買入)
   └─ 回傳累積 CVD 值

3. CVD 資料存儲
   └─ enhanced_data['cvd'] = cvd_value
      └─ MongoDB: market_data collection
```

---

## 🎯 **Symbol 分配演算法**

### **檔案**: `Common/utils/full_symbol_distributor.py`

```python
def generate_full_distribution(self):
    """
    🧠 智能 Symbol 分配演算法
    
    目標: 按交易量平衡分配 symbols 到多台 Slaves
    """
    
    # 1. 獲取所有 symbols
    all_symbols = self.symbol_manager.fetch_all_perpetual_pairs()
    
    # 2. 豐富交易量資料
    self.symbol_manager.enrich_with_volume_data()
    
    # 3. 按交易量排序 (高到低)
    sorted_symbols = sorted(
        all_symbols,
        key=lambda x: self.symbol_manager.symbol_info[x].volume_24h,
        reverse=True  # 從高交易量到低交易量
    )
    
    # 4. 智能分配
    distribution = self.symbol_manager.distribute_symbols_across_ips(
        sorted_symbols, self.num_slaves
    )
    
    return distribution
```

### **分配演算法流程圖**:

```
Symbol 分配完整流程:

1. 資料收集階段
   ├─ fetch_all_perpetual_pairs() ──→ 取得所有永續合約 symbols
   │  └─ Binance API: /fapi/v1/exchangeInfo
   ├─ enrich_with_volume_data() ──→ 豐富交易量資料
   │  └─ Binance API: /fapi/v1/ticker/24hr
   └─ 得到 symbols + volume_24h 資料

2. 排序階段
   └─ sorted() by volume_24h (降序)
      ├─ symbols[0] = 最高交易量 (如 BTCUSDT)
      ├─ symbols[1] = 次高交易量 (如 ETHUSDT)
      └─ symbols[n] = 最低交易量

3. 分配階段 - distribute_symbols_across_ips()
   ├─ symbols_per_ip = len(symbols) // num_slaves
   ├─ remainder = len(symbols) % num_slaves
   └─ 輪流分配:
      ├─ Slave-1: 取 symbols[0:symbols_per_ip+1] (多分配1個)
      ├─ Slave-2: 取 symbols[symbols_per_ip+1:2*symbols_per_ip+1]
      └─ ...依此類推

4. 結果生成
   └─ 每個 Slave 的配置檔案:
      ├─ slave-1.env (高交易量 symbols)
      ├─ slave-2.env (中交易量 symbols)
      └─ slave-N.env (低交易量 symbols)
```

---

## ❤️ **心跳機制詳細流程**

### **心跳發送端** (Slave)

```python
def heartbeat_loop(self):
    """
    💓 Slave 心跳循環
    """
    while True:
        try:
            self.send_heartbeat()
            time.sleep(30)  # 每30秒發送一次
        except Exception as e:
            logger.error(f"Heartbeat loop error: {e}")
            time.sleep(5)  # 錯誤時短暫休息

def send_heartbeat(self):
    """
    📤 發送心跳資料到 Master
    """
    import psutil
    
    health_data = {
        "status": "online",
        "timestamp": datetime.utcnow().isoformat(),
        "cpu_usage": psutil.cpu_percent(interval=1),
        "memory_usage": psutil.virtual_memory().percent,
        "symbols_processed": self.symbols_processed,
        "error_count": self.error_count,
        "uptime_seconds": (datetime.utcnow() - self.start_time).total_seconds()
    }
    
    response = requests.post(
        f"{self.master_url}/api/heartbeat/{self.slave_id}",
        json=health_data,
        timeout=5
    )
```

### **心跳接收端** (Master)

```python
async def handle_heartbeat(self, slave_id: str, health_data: dict):
    """
    📥 Master 處理 Slave 心跳
    """
    
    if slave_id not in self.slaves:
        logger.warning(f"Received heartbeat from unknown slave: {slave_id}")
        return {"status": "unknown_slave"}
    
    # 更新 Slave 狀態
    slave = self.slaves[slave_id]
    slave.last_heartbeat = datetime.utcnow()
    slave.status = "online"
    slave.cpu_usage = health_data.get("cpu_usage", 0)
    slave.memory_usage = health_data.get("memory_usage", 0)
    slave.symbols_processed = health_data.get("symbols_processed", 0)
    slave.error_count = health_data.get("error_count", 0)
    
    return {"status": "heartbeat_received"}
```

### **心跳監控** (Master)

```python
async def monitor_slaves(self):
    """
    👁️ Master 監控 Slaves 狀態
    """
    while True:
        try:
            current_time = datetime.utcnow()
            timeout_threshold = timedelta(seconds=self.config["heartbeat_timeout"])
            
            for slave_id, slave in self.slaves.items():
                time_since_heartbeat = current_time - slave.last_heartbeat
                
                if time_since_heartbeat > timeout_threshold:
                    if slave.status == "online":
                        logger.warning(f"Slave {slave_id} timed out")
                        slave.status = "offline"
            
            await asyncio.sleep(30)  # 每30秒檢查一次
            
        except Exception as e:
            logger.error(f"Monitor slaves error: {e}")
            await asyncio.sleep(10)
```

### **心跳機制流程圖**:

```
心跳機制完整流程:

Slave 端:                    Master 端:
┌─────────────────┐         ┌─────────────────┐
│ heartbeat_loop  │         │ monitor_slaves  │
│ (背景執行緒)     │         │ (背景任務)       │
└─────────┬───────┘         └─────────┬───────┘
          │                           │
          ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│每30秒執行:       │         │每30秒檢查:       │
│1. 收集系統資訊   │         │1. 檢查心跳超時   │
│2. send_heartbeat│──────→  │2. 更新Slave狀態  │
│3. POST到Master  │         │3. 標記離線Slaves │
└─────────────────┘         └─────────────────┘
          │                           │
          ▼                           ▼
┌─────────────────┐         ┌─────────────────┐
│ 系統資源監控:    │         │ 心跳處理邏輯:    │
│- CPU 使用率     │         │- 驗證 slave_id  │
│- 記憶體使用率   │         │- 更新狀態資訊   │
│- 處理的symbols  │         │- 記錄最後心跳   │
│- 錯誤計數       │         │- 回傳確認訊息   │
└─────────────────┘         └─────────────────┘
```

這個詳細的函數流程圖解讓你能深入理解每個關鍵組件的運作方式和相互關係！