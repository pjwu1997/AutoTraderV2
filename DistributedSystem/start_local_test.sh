#!/bin/bash

# AutoTrader 本地測試環境啟動腳本
# 使用方法: ./start_local_test.sh [選項]
#
# 選項:
#   --build    重新構建映像檔
#   --clean    清理舊資料重新開始
#   --stop     停止服務
#   --status   檢查服務狀態

set -e  # 遇到錯誤立即退出

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 輔助函數
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 檢查 Docker 是否安裝
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker 未安裝! 請先安裝 Docker"
        exit 1
    fi
    
    if ! command -v docker compose &> /dev/null; then
        print_error "Docker Compose 未安裝! 請先安裝 Docker Compose"
        exit 1
    fi
    
    # 檢查 Docker daemon 是否運行
    if ! docker info &> /dev/null; then
        print_error "Docker daemon 未運行! 請啟動 Docker"
        exit 1
    fi
    
    print_success "Docker 環境檢查通過"
}

# 檢查必要檔案
check_files() {
    local required_files=(
        "docker-compose.local.yml"
        "Scripts/deployment/mongodb.conf"
        "Config/local/master-local.env"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            print_error "缺少必要檔案: $file"
            print_warning "請先運行配置生成腳本"
            exit 1
        fi
    done
    
    print_success "必要檔案檢查通過"
}

# 生成測試配置 (如果不存在)
generate_config_if_needed() {
    if [[ ! -f "Config/local/test_summary.json" ]]; then
        print_status "生成測試配置..."
        if [[ -f "Scripts/testing/generate_test_config.py" ]]; then
            cd Scripts/testing
            python3 generate_test_config.py
            cd ../..
            print_success "測試配置已生成"
        else
            print_warning "配置生成腳本不存在, 將使用預設配置"
        fi
    fi
}

# 停止服務
stop_services() {
    print_status "停止 AutoTrader 分散式測試服務 (docker-compose.local.yml)..."
    docker compose -f docker-compose.local.yml down
    print_success "服務已停止"
}

# 清理環境
clean_environment() {
    print_warning "這將刪除所有測試資料!"
    read -p "確定要繼續嗎? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "清理測試環境..."
        docker compose -f docker-compose.local.yml down -v
        docker system prune -f
        print_success "環境已清理"
    else
        print_status "取消清理操作"
    fi
}

# 檢查服務狀態
check_status() {
    print_status "檢查服務狀態..."
    echo
    
    # Docker 容器狀態
    echo "=== Docker 容器狀態 ==="
    docker compose -f docker-compose.local.yml ps
    echo
    
    # 檢查服務健康狀況
    echo "=== 服務健康檢查 ==="
    
    # Master API
    echo -n "Master API (8080): "
    if curl -s -f http://localhost:8080/api/status &> /dev/null; then
        echo -e "${GREEN}✓ 正常${NC}"
    else
        echo -e "${RED}✗ 異常${NC}"
    fi
    
    # Slaves Health Check
    for i in {1..3}; do
        port=$((8080 + i))
        echo -n "Slave-$i ($port): "
        if curl -s -f http://localhost:$port/health &> /dev/null; then
            echo -e "${GREEN}✓ 正常${NC}"
        else
            echo -e "${RED}✗ 異常${NC}"
        fi
    done
    
    # MongoDB
    echo -n "MongoDB (27017): "
    if docker exec autotrader-mongo-local mongosh --quiet --eval "db.adminCommand('ping')" &> /dev/null; then
        echo -e "${GREEN}✓ 正常${NC}"
    else
        echo -e "${RED}✗ 異常${NC}"
    fi
    
    echo
    echo "=== 快速存取連結 ==="
    echo "• Master Dashboard: http://localhost:8080"
    echo "• Master API Status: http://localhost:8080/api/status"
    echo "• Slave-1 Health: http://localhost:8081/health"
    echo "• Slave-2 Health: http://localhost:8082/health"  
    echo "• Slave-3 Health: http://localhost:8083/health"
}

# 啟動服務
start_services() {
    local build_flag=""
    
    if [[ "$1" == "--build" ]]; then
        build_flag="--build"
        print_status "重新構建映像檔並啟動服務..."
    else
        print_status "啟動 AutoTrader 分散式測試服務 (使用 docker-compose.local.yml)..."
    fi
    
    # 檢查端口是否被佔用
    local ports=(8080 8081 8082 8083 27017)
    for port in "${ports[@]}"; do
        if netstat -tuln 2>/dev/null | grep -q ":$port "; then
            print_warning "端口 $port 可能已被佔用"
        fi
    done
    
    # 啟動服務
    docker compose -f docker-compose.local.yml up -d $build_flag
    
    print_success "服務啟動完成!"
    print_status "等待服務初始化... (30秒)"
    
    # 等待服務啟動
    for i in {1..30}; do
        echo -n "."
        sleep 1
    done
    echo
    
    # 檢查服務狀態
    check_status
    
    echo
    print_success "🎉 AutoTrader 本地測試環境已就緒!"
    echo
    print_status "查看即時日誌: docker compose -f docker-compose.local.yml logs -f"
    print_status "停止服務: ./start_local_test.sh --stop"
    print_status "檢查狀態: ./start_local_test.sh --status"
}

# 顯示使用說明
show_usage() {
    echo "AutoTrader 本地測試環境管理腳本"
    echo
    echo "使用方法:"
    echo "  $0 [選項]"
    echo
    echo "選項:"
    echo "  (無選項)   啟動服務"
    echo "  --build    重新構建映像檔並啟動"
    echo "  --clean    清理舊資料重新開始"
    echo "  --stop     停止服務"
    echo "  --status   檢查服務狀態"
    echo "  --help     顯示此說明"
    echo
    echo "範例:"
    echo "  $0              # 啟動服務"
    echo "  $0 --build      # 重新構建並啟動"
    echo "  $0 --status     # 檢查狀態"
    echo "  $0 --stop       # 停止服務"
}

# 主程序
main() {
    echo "🚀 AutoTrader 分散式系統本地測試環境"
    echo "====================================="
    echo "📍 配置檔案: docker-compose.local.yml"
    echo "🎯 系統類型: Master + 3 Slaves 分散式架構"
    echo "======================================"
    
    # 檢查是否在正確目錄
    if [[ ! -f "docker-compose.local.yml" ]]; then
        print_error "請在 DistributedSystem 目錄下執行此腳本"
        exit 1
    fi
    
    case "${1:-start}" in
        "start"|"")
            check_docker
            check_files
            generate_config_if_needed
            start_services
            ;;
        "--build")
            check_docker
            check_files
            generate_config_if_needed
            start_services --build
            ;;
        "--stop")
            stop_services
            ;;
        "--clean")
            clean_environment
            ;;
        "--status")
            check_status
            ;;
        "--help"|"-h")
            show_usage
            ;;
        *)
            print_error "未知選項: $1"
            show_usage
            exit 1
            ;;
    esac
}

# 執行主程序
main "$@"