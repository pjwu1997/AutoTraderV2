# WebSocket 故障排除指南

## 🔧 WebSocket 服務故障排除

### 常見WebSocket問題與解決方案

#### 1. WebSocket 連接失敗

**症狀:**
```bash
docker-compose logs kline-websocket | grep "Error"
# 看到: WebSocketException: Connection failed
```

**排除步驟:**
```bash
# 1. 檢查網路連通性
curl -I https://stream.binance.com:9443/ws/btcusdt@kline_1m
curl -I https://fstream.binance.com/ws/btcusdt@kline_1m

# 2. 檢查DNS解析
nslookup stream.binance.com
nslookup fstream.binance.com

# 3. 檢查防火牆設定
sudo ufw status
# 確保允許HTTPS出站連接 (443, 9443端口)

# 4. 測試WebSocket連接
python3 -c "
import websocket
try:
    ws = websocket.create_connection('wss://stream.binance.com:9443/ws/btcusdt@kline_1m')
    print('✅ Spot WebSocket連接成功')
    ws.close()
except Exception as e:
    print(f'❌ Spot WebSocket連接失敗: {e}')
"
```

**解決方案:**
- 檢查VM的出站網路規則
- 確認DNS設定正確
- 檢查是否有網路代理或防火牆阻擋

---

#### 2. WebSocket 頻繁斷線重連

**症狀:**
```bash
docker-compose logs kline-websocket | grep "Reconnecting"
# 看到: WebSocket disconnected, reconnecting... (attempt 5/10)
```

**排除步驟:**
```bash
# 1. 檢查WebSocket服務狀態
docker-compose ps | grep websocket

# 2. 檢查網路穩定性
ping -c 10 stream.binance.com
ping -c 10 fstream.binance.com

# 3. 檢查系統資源
htop  # 或 top
df -h  # 檢查磁碟空間
free -h  # 檢查記憶體使用

# 4. 檢查WebSocket配置
cat ../../Config/slaves/slave-1.env | grep WEBSOCKET
```

**解決方案:**
```bash
# 調整WebSocket重連參數
export WEBSOCKET_RECONNECT_DELAY="10"  # 增加到10秒
export WEBSOCKET_MAX_RECONNECT_ATTEMPTS="20"  # 增加重試次數
export WEBSOCKET_PING_INTERVAL="60"  # 降低ping頻率

# 重啟WebSocket服務
docker-compose restart kline-websocket liquidation-websocket
```

---

#### 3. 清算WebSocket無資料

**症狀:**
```bash
docker-compose logs liquidation-websocket | tail -20
# 沒有看到新的清算事件日誌
```

**排除步驟:**
```bash
# 1. 檢查清算WebSocket連接
curl http://localhost:8083/health
# 預期回應: {"status": "healthy", "websocket_connected": true}

# 2. 手動測試清算WebSocket
python3 -c "
import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    print(f'收到清算事件: {data}')

def on_error(ws, error):
    print(f'WebSocket錯誤: {error}')

ws = websocket.WebSocketApp('wss://fstream.binance.com/ws/!forceOrder@arr',
                           on_message=on_message, on_error=on_error)
print('測試清算WebSocket連接...')
ws.run_forever()
"

# 3. 檢查清算資料是否正常寫入MongoDB
docker exec -it shared-mongo mongosh
db.market_data.find({
  'liquidations.buy_liquidations.event_count': {'$gt': 0}
}).limit(5)
```

**解決方案:**
- 清算事件可能在低波動期間較少，屬正常現象
- 檢查其他交易對是否有清算資料
- 確認WebSocket URL正確: `wss://fstream.binance.com/ws/!forceOrder@arr`

---

#### 4. K線WebSocket資料不同步

**症狀:**
```bash
# WebSocket K線時間與REST API時間不一致
```

**排除步驟:**
```bash
# 1. 檢查系統時間同步
timedatectl status
# 確認 NTP synchronized: yes

# 2. 同步系統時間
sudo ntpdate -s time.nist.gov
# 或
sudo timedatectl set-ntp true

# 3. 檢查WebSocket資料時間戳
docker-compose logs kline-websocket | grep "Kline data"
# 比對時間戳與當前時間

# 4. 檢查時區設定
echo $TZ
# 應該顯示 UTC 或正確的時區
```

**解決方案:**
```bash
# 設定正確時區
export TZ=UTC
docker-compose down && docker-compose up -d

# 在docker-compose.yml中設定時區
services:
  kline-websocket:
    environment:
      - TZ=UTC
```

---

#### 5. WebSocket記憶體洩漏

**症狀:**
```bash
# WebSocket容器記憶體使用持續增長
docker stats | grep websocket
```

**排除步驟:**
```bash
# 1. 檢查容器記憶體使用
docker stats --no-stream | grep websocket

# 2. 檢查WebSocket緩衝區設定
docker-compose logs kline-websocket | grep "Buffer"

# 3. 檢查是否有資料堆積
docker exec -it kline-websocket ps aux
```

**解決方案:**
```bash
# 1. 設定記憶體限制
# 在docker-compose.yml中加入:
services:
  kline-websocket:
    deploy:
      resources:
        limits:
          memory: 256M
        reservations:
          memory: 128M

# 2. 定期重啟WebSocket服務
# 加入crontab定時重啟 (每24小時)
0 0 * * * docker-compose -f /path/to/docker-compose.slave.yml restart kline-websocket liquidation-websocket

# 3. 調整WebSocket緩衝區設定
export WEBSOCKET_BUFFER_SIZE="8192"
export WEBSOCKET_MAX_MESSAGE_SIZE="65536"
```

## 📊 WebSocket 監控與診斷

### 即時監控指令
```bash
# 1. 檢查所有WebSocket服務狀態
docker-compose ps | grep websocket

# 2. 即時查看WebSocket日誌
docker-compose logs -f kline-websocket
docker-compose logs -f liquidation-websocket

# 3. 檢查WebSocket連接數
netstat -an | grep :8082
netstat -an | grep :8083

# 4. 檢查WebSocket API回應
curl http://localhost:8082/health | jq
curl http://localhost:8083/health | jq
```

### WebSocket效能測試
```bash
# 測試WebSocket延遲
python3 -c "
import time
import websocket
import json

start_time = time.time()

def on_message(ws, message):
    global start_time
    latency = (time.time() - start_time) * 1000
    print(f'WebSocket延遲: {latency:.2f}ms')
    ws.close()

def on_open(ws):
    global start_time
    start_time = time.time()
    
ws = websocket.WebSocketApp('wss://stream.binance.com:9443/ws/btcusdt@kline_1m',
                           on_message=on_message, on_open=on_open)
ws.run_forever()
"

# 測試WebSocket吞吐量
docker-compose logs kline-websocket | grep "messages/sec"
```

### 自動診斷腳本
```bash
#!/bin/bash
# websocket_diagnostic.sh

echo "🔍 WebSocket 系統診斷開始..."

# 檢查WebSocket服務
echo "1. 檢查WebSocket服務狀態..."
docker-compose ps | grep websocket

# 檢查網路連通性  
echo "2. 檢查網路連通性..."
curl -s -o /dev/null -w "%{http_code}" https://stream.binance.com:9443/ws/btcusdt@kline_1m
curl -s -o /dev/null -w "%{http_code}" https://fstream.binance.com/ws/btcusdt@kline_1m

# 檢查WebSocket健康狀態
echo "3. 檢查WebSocket健康狀態..."
curl -s http://localhost:8082/health | jq .websocket_connected
curl -s http://localhost:8083/health | jq .websocket_connected

# 檢查最近的錯誤
echo "4. 檢查最近的WebSocket錯誤..."
docker-compose logs --since="1h" kline-websocket | grep -i error | tail -5
docker-compose logs --since="1h" liquidation-websocket | grep -i error | tail -5

# 檢查系統資源
echo "5. 檢查系統資源使用..."
docker stats --no-stream | grep websocket

echo "✅ WebSocket 系統診斷完成"
```

## 🚨 緊急恢復程序

### WebSocket服務完全停止
```bash
# 1. 緊急重啟所有WebSocket服務
docker-compose stop kline-websocket liquidation-websocket
docker-compose rm -f kline-websocket liquidation-websocket
docker-compose up -d kline-websocket liquidation-websocket

# 2. 檢查恢復狀態
sleep 30
docker-compose logs kline-websocket | tail -10
docker-compose logs liquidation-websocket | tail -10

# 3. 驗證資料收集恢復
curl http://localhost:8082/health
curl http://localhost:8083/health
```

### 資料遺失檢測與恢復
```bash
# 1. 檢查資料完整性
docker exec -it shared-mongo mongosh
db.market_data.count({
  timestamp: {
    $gte: new Date("2023-09-13T19:00:00Z"),
    $lt: new Date("2023-09-13T20:00:00Z") 
  }
})

# 2. 如果發現資料遺失，檢查備份REST API是否正常
curl http://localhost:8081/health

# 3. 手動觸發資料補齊 (如果有相關腳本)
docker exec data-fetcher python3 backfill_missing_data.py --start="2023-09-13T19:00:00Z" --end="2023-09-13T20:00:00Z"
```

這份故障排除指南涵蓋了WebSocket系統的常見問題、診斷方法和解決方案，幫助您快速定位和解決WebSocket相關問題。