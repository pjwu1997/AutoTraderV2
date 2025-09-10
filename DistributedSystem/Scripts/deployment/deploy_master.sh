#!/bin/bash
# Master VM 部署腳本

set -e

echo "=== 部署 Master VM ==="

# 檢查必要檔案
CONFIG_DIR="../../Config/master"
if [ ! -f "$CONFIG_DIR/master.env" ]; then
    echo "Error: $CONFIG_DIR/master.env not found!"
    echo "Please create master configuration first."
    exit 1
fi

# 載入配置
source "$CONFIG_DIR/master.env"

echo "配置檢查..."
echo "- Master Port: ${MASTER_PORT:-8080}"
echo "- MongoDB URI: ${MONGO_URI}"
echo "- MongoDB Database: ${MONGO_DB_NAME}"

# 建立必要的目錄
echo "建立目錄結構..."
mkdir -p ../../Config/master/logs
mkdir -p ../../Config/master/data

# 產生 Symbol 分配 (如果不存在)
if [ ! -d "../../Config/slaves" ]; then
    echo "產生 Symbol 分配..."
    docker-compose -f docker-compose.master.yml --profile setup up symbol-distributor
    echo "Symbol 分配完成"
fi

# 啟動 Master 服務
echo "啟動 Master 服務..."
docker-compose -f docker-compose.master.yml up -d

# 等待服務啟動
echo "等待服務啟動..."
sleep 10

# 檢查服務狀態
echo "檢查服務狀態..."
docker-compose -f docker-compose.master.yml ps

# 測試 Master API
echo "測試 Master API..."
MASTER_URL="http://localhost:${MASTER_PORT:-8080}"
if curl -s "$MASTER_URL/api/status" > /dev/null; then
    echo "✅ Master API 運行正常"
    echo "📊 Dashboard: $MASTER_URL/dashboard.html"
else
    echo "❌ Master API 無法連接"
    echo "請檢查日誌: docker-compose -f docker-compose.master.yml logs"
    exit 1
fi

# 測試 MongoDB 連接
echo "測試 MongoDB 連接..."
if docker exec shared-mongo mongosh --eval "db.runCommand('ping')" > /dev/null 2>&1; then
    echo "✅ MongoDB 運行正常"
else
    echo "❌ MongoDB 連接失敗"
    docker-compose -f docker-compose.master.yml logs shared-mongo
    exit 1
fi

echo ""
echo "🎉 Master VM 部署完成！"
echo ""
echo "📋 服務資訊:"
echo "- Master Dashboard: $MASTER_URL/dashboard.html"
echo "- Master API: $MASTER_URL/api/status"
echo "- MongoDB: localhost:27017"
echo "- Redis: localhost:6379"
echo ""
echo "📁 配置檔案已產生在 ../../Config/slaves/"
echo "請將這些檔案複製到各個 Slave VM"
echo ""
echo "🔧 管理指令:"
echo "- 查看日誌: docker-compose -f docker-compose.master.yml logs"
echo "- 停止服務: docker-compose -f docker-compose.master.yml down"
echo "- 重啟服務: docker-compose -f docker-compose.master.yml restart"