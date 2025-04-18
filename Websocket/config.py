# config.py
# 集中管理 WebSocket 和 MongoDB 的配置變數

# MongoDB 設定
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB_NAME = "multikline_poc"

# 交易對清單
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "ADAUSDT",
    "BIGTIMEUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "SOLUSDT",
    "VINEUSDT",
    "FARTCOINUSDT",
    "ARKUSDT",
    "ALCHUSDT",
]

# LiquidationWebSocket 設定
LIQUIDATION_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
LIQUIDATION_CLEANUP_MINUTES = 5  # 清理 aggregated_data 中早於 X 分鐘的資料

# KlineWebSocket 設定
KLINE_INTERVAL = "1m"
KLINE_SPOT_WS_URL = "wss://stream.binance.com:9443/stream?streams={streams}"
KLINE_FUTURES_WS_URL = "wss://fstream.binance.com/stream?streams={streams}"