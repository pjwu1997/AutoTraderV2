#!/bin/bash
# Slave VM 部署腳本

set -e

# 參數檢查
if [ -z "$1" ]; then
    echo "用法: $0 <slave-id>"
    echo "範例: $0 slave-1"
    echo ""
    echo "可用的 Slave IDs:"
    ls ../../Config/slaves/*.env 2>/dev/null | sed 's/.*slaves\///; s/\.env//' | sed 's/^/  /' || echo "  (無配置檔案)"
    exit 1
fi

SLAVE_ID="$1"
CONFIG_FILE="../../Config/slaves/${SLAVE_ID}.env"

echo "=== 部署 Slave VM: $SLAVE_ID ==="

# 檢查配置檔案
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: 配置檔案 $CONFIG_FILE 不存在!"
    echo "請先在 Master VM 上執行 symbol 分配，或手動建立配置檔案。"
    exit 1
fi

# 載入配置
source "$CONFIG_FILE"

echo "配置檢查..."
echo "- Slave ID: $SLAVE_ID" 
echo "- Symbol Count: ${SYMBOL_COUNT:-0}"
echo "- Master URL: ${MASTER_URL}"
echo "- MongoDB URI: ${MONGO_URI}"

# 檢查必要環境變數
if [ -z "$SYMBOLS" ]; then
    echo "Error: SYMBOLS 環境變數未設定!"
    exit 1
fi

if [ -z "$MASTER_URL" ]; then
    echo "Error: MASTER_URL 環境變數未設定!"
    exit 1
fi

# 設定額外的環境變數
export SLAVE_ID
export HEALTH_PORT=$((8081 + ${SLAVE_ID#slave-}))

echo "- Health Port: $HEALTH_PORT"

# 測試與 Master 的連接
echo "測試 Master 連接..."
if curl -s "${MASTER_URL}/api/status" > /dev/null; then
    echo "✅ Master 連接正常"
else
    echo "⚠️  無法連接到 Master，將繼續部署但可能無法註冊"
fi

# 測試 MongoDB 連接 (如果是外部的話)
if [[ "$MONGO_URI" != *"localhost"* && "$MONGO_URI" != *"127.0.0.1"* ]]; then
    echo "測試 MongoDB 連接..."
    MONGO_HOST=$(echo "$MONGO_URI" | sed -n 's/.*\/\/\([^:\/]*\).*/\1/p')
    if nc -z "$MONGO_HOST" 27017 2>/dev/null; then
        echo "✅ MongoDB 連接正常"
    else
        echo "⚠️  無法連接到 MongoDB，請確認網路設定"
    fi
fi

# 建立必要目錄
echo "建立目錄結構..."
mkdir -p logs
mkdir -p data

# 啟動 Slave 服務
echo "啟動 Slave 服務..."
docker-compose -f docker-compose.slave.yml up -d

# 等待服務啟動
echo "等待服務啟動..."
sleep 15

# 檢查服務狀態
echo "檢查服務狀態..."
docker-compose -f docker-compose.slave.yml ps

# 測試健康檢查
echo "測試健康檢查..."
sleep 5
if curl -s "http://localhost:$HEALTH_PORT/health" > /dev/null; then
    echo "✅ 健康檢查服務正常"
else
    echo "❌ 健康檢查服務無法連接"
    echo "檢查日誌: docker-compose -f docker-compose.slave.yml logs health-checker"
fi

# 檢查各服務狀態
echo ""
echo "🔍 檢查各服務狀態..."

SERVICES=("unified-collector" "kline-websocket" "liquidation-websocket" "health-checker")
for service in "${SERVICES[@]}"; do
    container_name="${SLAVE_ID}-${service}"
    if docker ps --format "table {{.Names}}" | grep -q "$container_name"; then
        status="✅ 運行中"
    else
        status="❌ 停止"
    fi
    echo "- $service: $status"
done

echo ""
echo "🎉 Slave VM 部署完成！"
echo ""
echo "📋 服務資訊:"
echo "- Slave ID: $SLAVE_ID"
echo "- 處理 Symbols: ${SYMBOL_COUNT:-0} 個"
echo "- 健康檢查: http://localhost:$HEALTH_PORT/health"
echo "- Master URL: $MASTER_URL"
echo ""
echo "📊 監控指令:"
echo "- 查看所有日誌: docker-compose -f docker-compose.slave.yml logs"
echo "- 查看統一收集器: docker-compose -f docker-compose.slave.yml logs unified-collector"
echo "- 重啟服務: docker-compose -f docker-compose.slave.yml restart"
echo "- 停止服務: docker-compose -f docker-compose.slave.yml down"
echo ""
echo "🔗 如需檢查 Master 狀態:"
echo "curl $MASTER_URL/api/slaves | jq ."