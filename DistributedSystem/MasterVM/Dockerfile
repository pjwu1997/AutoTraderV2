FROM python:3.11-slim

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 設定工作目錄
WORKDIR /app

# 複製統一的 requirements
COPY requirements.txt .

# 安裝 Python 依賴
RUN pip install --no-cache-dir -r requirements.txt

# 複製 MasterVM 原始碼
COPY MasterVM/src/ src/
COPY MasterVM/templates/ templates/

# 複製共用模組
COPY Common/ Common/

# 建立日誌目錄
RUN mkdir -p /app/logs


# 設定環境變數
ENV PYTHONPATH="/app:/app/Common"
ENV PYTHONUNBUFFERED=1

# 暴露端口
EXPOSE 8080

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/api/status || exit 1

# 啟動命令
CMD ["python", "src/master_coordinator.py"]