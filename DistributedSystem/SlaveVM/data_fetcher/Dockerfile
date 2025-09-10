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

# 複製原始 DataFetcher (依賴)
COPY ../../../DataFetcher/ DataFetcher/

# 複製分散式資料收集器
COPY distributed_data_fetcher.py .

# 複製共用模組
COPY ../../Common/ Common/

# 建立日誌和資料目錄
RUN mkdir -p logs data

# 設定環境變數
ENV PYTHONPATH="/app:/app/DataFetcher"
ENV PYTHONUNBUFFERED=1

# 健康檢查
HEALTHCHECK --interval=60s --timeout=30s --start-period=10s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8081/health', timeout=5)" || exit 1

# 啟動命令
CMD ["python", "distributed_data_fetcher.py"]