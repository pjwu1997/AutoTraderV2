#!/usr/bin/env python3
"""
生成本地測試用的 symbol 配置
"""
import os
import json

def generate_test_symbols():
    """生成測試用的 symbols 分配"""
    
    # 測試用 symbols (選擇主要的加密貨幣)
    test_symbols = {
        'slave-1': [
            'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 
            'DOTUSDT', 'LINKUSDT', 'UNIUSDT', 'LTCUSDT', 'BCHUSDT',
            'AVAXUSDT', 'ATOMUSDT', 'ALGOUSDT', 'VETUSDT', 'MATICUSDT',
            'FILUSDT', 'AAVEUSDT'
        ],
        'slave-2': [
            'BNBUSDT', 'TRXUSDT', 'EOSUSDT', 'XLMUSDT', 'XMRUSDT',
            'DASHUSDT', 'ETCUSDT', 'IOTAUSDT', 'NEOUSDT', 'ONTUSDT',
            'QTUMUSDT', 'ICXUSDT', 'LSKUSDT', 'NANOUSDT', 'ZILUSDT',
            'BATUSDT', 'ENJUSDT'
        ],
        'slave-3': [
            'CHZUSDT', 'HBARUSDT', 'STXUSDT', 'CRVUSDT', 'COMPUSDT',
            'YFIUSDT', 'SNXUSDT', 'UMAUSDT', 'BALUSDT', 'CVCUSDT',
            'STORJUSDT', 'KNCUSDT', 'LRCUSDT', 'BANDUSDT', 'RLCUSDT',
            'NMRUSDT'
        ]
    }
    
    # 創建配置目錄
    config_dir = '../Config/local'
    os.makedirs(config_dir, exist_ok=True)
    
    # 生成各 slave 的環境變數檔案
    for slave_id, symbols in test_symbols.items():
        env_content = f"""# 本地測試配置 for {slave_id}
SLAVE_ID={slave_id}
SYMBOLS={','.join(symbols)}
SYMBOL_COUNT={len(symbols)}

# Master 連接 (Docker 容器名稱)
MASTER_URL=http://master:8080
MONGO_URI=mongodb://mongodb:27017/
MONGO_DB_NAME=trading_data_test

# 測試設定
TEST_MODE=true
LOG_LEVEL=DEBUG
TIMEFRAME=5m
FETCH_INTERVAL=120  # 2分鐘收集一次 (測試用)
BATCH_SIZE=5        # 小批次測試
RATE_LIMIT_DELAY=1.0  # 較長的延遲避免API限制
MAX_RETRIES=2

# Health Check
HEALTH_PORT=8081
HEARTBEAT_INTERVAL=10
"""
        
        env_file = f"{config_dir}/{slave_id}-local.env"
        with open(env_file, 'w') as f:
            f.write(env_content)
        print(f"✅ 生成 {env_file}")
    
    # 生成測試總覽
    summary = {
        "test_config": "LOCAL_TESTING",
        "total_slaves": len(test_symbols),
        "total_symbols": sum(len(symbols) for symbols in test_symbols.values()),
        "symbols_per_slave": {
            slave_id: {
                "count": len(symbols),
                "symbols": symbols
            }
            for slave_id, symbols in test_symbols.items()
        }
    }
    
    summary_file = f"{config_dir}/test_summary.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"✅ 生成 {summary_file}")
    
    print(f"\n📊 測試配置摘要:")
    print(f"   - 總 Slaves: {len(test_symbols)}")
    print(f"   - 總 Symbols: {sum(len(symbols) for symbols in test_symbols.values())}")
    print(f"   - 每 Slave 約: {sum(len(symbols) for symbols in test_symbols.values()) // len(test_symbols)} symbols")

if __name__ == "__main__":
    print("🧪 生成本地測試配置...")
    generate_test_symbols()
    print("✅ 完成！")