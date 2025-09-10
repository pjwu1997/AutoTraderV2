FROM python:3.11-slim

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# 設定工作目錄
WORKDIR /app

# 複製 requirements
COPY requirements.txt .

# 安裝 Python 依賴
RUN pip install --no-cache-dir -r requirements.txt

# 複製應用程式碼
COPY src/ src/
COPY config/ config/
COPY templates/ templates/

# 複製共用模組
COPY ../Common/ Common/

# 建立日誌目錄
RUN mkdir -p logs

# 暴露端口
EXPOSE 8080

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/api/status || exit 1

# 啟動命令
CMD ["python", "src/master_coordinator.py"]