#!/bin/bash
# 全量收集部署腳本 - Master VM

set -e

echo "=== AutoTrader 全量收集系統部署 ==="
echo "收集所有 Binance Perpetual Contracts (~400+ symbols)"
echo ""

# 參數檢查
if [ -z "$1" ]; then
    echo "用法: $0 <master-vm-ip> [num-slaves]"
    echo "範例: $0 10.0.1.100 5"
    echo ""
    echo "參數說明:"
    echo "  master-vm-ip: Master VM 的 IP 地址"
    echo "  num-slaves: Slave VM 數量 (預設: 5)"
    exit 1
fi

MASTER_VM_IP="$1"
NUM_SLAVES="${2:-5}"

echo "配置參數:"
echo "- Master VM IP: $MASTER_VM_IP"
echo "- Slave 數量: $NUM_SLAVES"
echo ""

# 檢查必要檔案
CONFIG_DIR="../../Config/master"
if [ ! -f "$CONFIG_DIR/master_full_collection.env" ]; then
    echo "Error: $CONFIG_DIR/master_full_collection.env not found!"
    echo "請先建立 Master 全量收集配置檔案。"
    exit 1
fi

# 載入配置並更新 IP
cp "$CONFIG_DIR/master_full_collection.env" "$CONFIG_DIR/master.env"
sed -i.bak "s/MASTER_VM_IP=.*/MASTER_VM_IP=$MASTER_VM_IP/" "$CONFIG_DIR/master.env"
sed -i.bak "s/NUM_SLAVES=.*/NUM_SLAVES=$NUM_SLAVES/" "$CONFIG_DIR/master.env"

echo "✅ 配置檔案已更新"

# 設定環境變數
export MASTER_VM_IP="$MASTER_VM_IP"
export NUM_SLAVES="$NUM_SLAVES"

source "$CONFIG_DIR/master.env"

echo "🔍 配置檢查..."
echo "- Master Port: ${MASTER_PORT:-8080}"
echo "- MongoDB URI: ${MONGO_URI}"
echo "- MongoDB Database: ${MONGO_DB_NAME}"
echo "- Slaves 數量: ${NUM_SLAVES}"

# 建立必要的目錄
echo "📁 建立目錄結構..."
mkdir -p ../../Config/master/logs
mkdir -p ../../Config/master/data
mkdir -p ../../Config/slaves

# 檢查 Docker 和網路
echo "🐳 檢查 Docker 環境..."
if ! docker --version > /dev/null 2>&1; then
    echo "❌ Docker 未安裝或未啟動"
    exit 1
fi

# 停止現有服務 (如果有)
echo "🛑 停止現有服務..."
docker-compose -f docker-compose.master.yml down 2>/dev/null || true

# 生成全量 Symbol 分配
echo "📊 生成全量 Symbol 分配..."
echo "正在分析所有 Binance Perpetual Contracts..."

docker-compose -f docker-compose.master.yml --profile setup up full-symbol-distributor

if [ $? -eq 0 ]; then
    echo "✅ Symbol 分配完成"
else
    echo "❌ Symbol 分配失敗"
    echo "請檢查網路連接和 Binance API 狀態"
    exit 1
fi

# 檢查生成的配置檔案
echo "🔍 檢查生成的配置..."
if [ ! -f "../../Config/slaves/slave-1.env" ]; then
    echo "❌ 配置檔案生成失敗"
    exit 1
fi

# 統計資訊
echo ""
echo "📈 分配統計:"
for i in $(seq 1 $NUM_SLAVES); do
    if [ -f "../../Config/slaves/slave-$i.env" ]; then
        SYMBOL_COUNT=$(grep "SYMBOL_COUNT=" "../../Config/slaves/slave-$i.env" | cut -d'=' -f2)
        echo "  - Slave-$i: $SYMBOL_COUNT symbols"
    fi
done

# 啟動 Master 服務
echo ""
echo "🚀 啟動 Master 服務..."
docker-compose -f docker-compose.master.yml up -d

# 等待服務啟動
echo "⏳ 等待服務啟動..."
sleep 15

# 檢查服務狀態
echo "🔍 檢查服務狀態..."
docker-compose -f docker-compose.master.yml ps

# 檢查 MongoDB 連接
echo "🗄️  測試 MongoDB 連接..."
sleep 5
if docker exec shared-mongo mongosh --eval "db.runCommand('ping')" > /dev/null 2>&1; then
    echo "✅ MongoDB 運行正常"
else
    echo "❌ MongoDB 連接失敗"
    docker-compose -f docker-compose.master.yml logs shared-mongo
    exit 1
fi

# 建立 MongoDB 使用者
echo "👤 建立 MongoDB 使用者..."
docker exec shared-mongo mongosh admin --eval "
db.createUser({
  user: 'trader_user',
  pwd: 'trader_pass_2024',
  roles: [
    { role: 'readWrite', db: 'trading_data' },
    { role: 'read', db: 'admin' }
  ]
})
" 2>/dev/null || echo "使用者可能已存在"

# 測試 Master API
echo "🌐 測試 Master API..."
MASTER_URL="http://localhost:${MASTER_PORT:-8080}"
for i in {1..5}; do
    if curl -s "$MASTER_URL/api/status" > /dev/null; then
        echo "✅ Master API 運行正常"
        break
    elif [ $i -eq 5 ]; then
        echo "❌ Master API 無法連接"
        echo "請檢查日誌: docker-compose -f docker-compose.master.yml logs"
        exit 1
    else
        echo "等待 API 啟動... ($i/5)"
        sleep 5
    fi
done

# 產生 Slave 部署指令
echo ""
echo "📝 產生 Slave 部署指令..."
cat > ../../Config/slaves/deploy_all_slaves.sh << EOF
#!/bin/bash
# 自動生成的 Slave 部署指令

echo "=== 部署所有 Slaves ==="
echo "Master IP: $MASTER_VM_IP"
echo "Slaves 數量: $NUM_SLAVES"
echo ""

for i in \$(seq 1 $NUM_SLAVES); do
    echo "部署 slave-\$i..."
    
    # 複製配置檔案到對應的 VM
    echo "scp slave-\$i.env root@slave-vm-\$i:/root/config/"
    echo "scp ../deployment/docker-compose.slave.yml root@slave-vm-\$i:/root/"
    echo "scp ../deployment/deploy_slave.sh root@slave-vm-\$i:/root/"
    echo ""
    
    # 在遠端 VM 執行部署
    echo "ssh root@slave-vm-\$i 'cd /root && ./deploy_slave.sh slave-\$i'"
    echo ""
done

echo "所有 Slaves 部署完成！"
echo "檢查狀態: curl $MASTER_URL/api/slaves | jq ."
EOF

chmod +x ../../Config/slaves/deploy_all_slaves.sh

# 顯示統計資訊
echo ""
echo "🎉 Master VM 全量收集系統部署完成！"
echo ""
echo "📊 系統統計:"
echo "- Master IP: $MASTER_VM_IP"
echo "- Master API: $MASTER_URL"
echo "- Master Dashboard: $MASTER_URL/dashboard.html"
echo "- MongoDB: $MASTER_VM_IP:27017"
echo "- Slaves 數量: $NUM_SLAVES"

# 計算總 symbols
TOTAL_SYMBOLS=0
for i in $(seq 1 $NUM_SLAVES); do
    if [ -f "../../Config/slaves/slave-$i.env" ]; then
        SYMBOL_COUNT=$(grep "SYMBOL_COUNT=" "../../Config/slaves/slave-$i.env" | cut -d'=' -f2)
        TOTAL_SYMBOLS=$((TOTAL_SYMBOLS + SYMBOL_COUNT))
    fi
done

echo "- 總 Symbols: $TOTAL_SYMBOLS"
echo ""

# 估算負載
REQUESTS_PER_MINUTE=$((TOTAL_SYMBOLS * 5))
REQUESTS_PER_SLAVE=$((REQUESTS_PER_MINUTE / NUM_SLAVES))

echo "⚡ 負載估算:"
echo "- 每分鐘總請求: ~$REQUESTS_PER_MINUTE"
echo "- 每 Slave 每分鐘: ~$REQUESTS_PER_SLAVE 請求"
echo "- Binance 限制: 1200 請求/分鐘/IP"

if [ $REQUESTS_PER_SLAVE -gt 1000 ]; then
    echo "- 狀態: ⚠️  接近 API 限制"
    echo "- 建議: 增加 Slave 數量或調整收集頻率"
else
    echo "- 狀態: ✅ API 使用率安全"
fi

echo ""
echo "📁 配置檔案位置:"
echo "- Master 配置: ../../Config/master/master.env"
echo "- Slave 配置: ../../Config/slaves/slave-*.env"
echo "- 部署指令: ../../Config/slaves/deploy_all_slaves.sh"
echo ""
echo "🚀 下一步:"
echo "1. 將 Slave 配置檔案複製到各 VM"
echo "2. 執行 ../../Config/slaves/deploy_all_slaves.sh"
echo "3. 或手動在每台 Slave VM 執行: ./deploy_slave.sh slave-N"
echo ""
echo "🔧 管理指令:"
echo "- 查看 Master 日誌: docker-compose -f docker-compose.master.yml logs"
echo "- 查看系統狀態: curl $MASTER_URL/api/status | jq ."
echo "- 查看 Slaves 狀態: curl $MASTER_URL/api/slaves | jq ."
echo "- 停止服務: docker-compose -f docker-compose.master.yml down"