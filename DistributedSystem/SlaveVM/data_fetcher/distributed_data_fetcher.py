#!/usr/bin/env python3
"""
分散式資料收集器 - 基於原本的 DataFetcher，加入分散式功能
每個 Slave 處理分配的 symbols，保持完整的資料收集功能
"""

import asyncio
import os
import sys
import time
import logging
import schedule
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from pymongo import MongoClient

# 引入原本的 DataFetcher
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from DataFetcher.data_fetcher import DataFetcher
from schema_compatible_collector import SchemaCompatibleCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DistributedDataFetcher:
    """
    分散式資料收集器 - 基於原本的 DataFetcher 邏輯
    """
    
    def __init__(self, slave_id: str, symbols: List[str], master_url: str, **kwargs):
        self.slave_id = slave_id
        self.assigned_symbols = symbols
        self.master_url = master_url
        self.error_count = 0
        self.symbols_processed = 0
        self.start_time = datetime.utcnow()
        
        # 初始化增強版資料收集器 (基於原本 DataFetcher)
        self.data_fetcher = SchemaCompatibleCollector(slave_id=slave_id, **kwargs)
        
        logger.info(f"Initialized {slave_id} with {len(symbols)} symbols")
        logger.info(f"Sample symbols: {symbols[:5]}...")
        logger.info("Enhanced DataFetcher initialized (preserves original logic)")
    
    def register_with_master(self) -> bool:
        """向 Master 註冊"""
        try:
            registration_data = {
                "slave_id": self.slave_id,
                "ip_address": self.get_local_ip(),
                "assigned_symbols": self.assigned_symbols,
                "status": "online"
            }
            
            response = requests.post(
                f"{self.master_url}/api/register",
                json=registration_data,
                timeout=10
            )
            
            if response.status_code == 200:
                config = response.json().get("config", {})
                logger.info(f"Successfully registered with master")
                return True
            else:
                logger.error(f"Failed to register with master: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error registering with master: {e}")
            return False
    
    def get_local_ip(self) -> str:
        """獲取本機 IP"""
        try:
            import socket
            hostname = socket.gethostname()
            return socket.gethostbyname(hostname)
        except:
            return "unknown"
    
    def send_heartbeat(self):
        """發送心跳到 Master"""
        try:
            import psutil
            
            health_data = {
                "status": "online",
                "timestamp": datetime.utcnow().isoformat(),
                "cpu_usage": psutil.cpu_percent(interval=1),
                "memory_usage": psutil.virtual_memory().percent,
                "symbols_processed": self.symbols_processed,
                "error_count": self.error_count,
                "uptime_seconds": (datetime.utcnow() - self.start_time).total_seconds()
            }
            
            response = requests.post(
                f"{self.master_url}/api/heartbeat/{self.slave_id}",
                json=health_data,
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                if "warning" in result:
                    logger.warning(f"Master warning: {result['warning']}")
                return True
            else:
                logger.warning(f"Heartbeat failed: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
            return False
    
    def fetch_and_store_symbol(self, symbol: str):
        """為單一 symbol 收集並儲存資料 - 使用原本的 fetch_and_store 邏輯"""
        try:
            logger.debug(f"Processing {symbol} with enhanced data fetcher...")
            
            # 使用增強版資料收集器 (保持原本邏輯)
            self.data_fetcher.fetch_and_store(symbol)
            
            self.symbols_processed += 1
            logger.debug(f"Successfully processed {symbol} with enhanced data")
            
        except Exception as e:
            self.error_count += 1
            logger.error(f"Error processing {symbol}: {e}")
    
    def run_collection_cycle(self):
        """運行一個完整的收集週期"""
        cycle_start = time.time()
        logger.info(f"Starting collection cycle for {len(self.assigned_symbols)} symbols")
        
        successful_count = 0
        for symbol in self.assigned_symbols:
            try:
                self.fetch_and_store_symbol(symbol)
                successful_count += 1
                
                # 每處理 10 個 symbols 休息一下，避免 API 限制
                if successful_count % 10 == 0:
                    time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
        
        cycle_duration = time.time() - cycle_start
        logger.info(f"Collection cycle completed: {successful_count}/{len(self.assigned_symbols)} symbols in {cycle_duration:.1f}s")
        
        # 發送心跳
        self.send_heartbeat()
    
    def start_scheduled_collection(self):
        """啟動定時資料收集"""
        logger.info("Starting scheduled data collection...")
        
        # 向 Master 註冊
        if not self.register_with_master():
            logger.error("Failed to register with master, continuing anyway...")
        
        # 設定定時任務 - 每分鐘執行
        schedule.every(1).minutes.do(self.run_collection_cycle)
        
        # 設定心跳任務 - 每30秒執行
        schedule.every(30).seconds.do(self.send_heartbeat)
        
        logger.info("Scheduled collection started. Press Ctrl+C to stop.")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(5)  # 每5秒檢查一次
        except KeyboardInterrupt:
            logger.info("Stopping data collection...")
        except Exception as e:
            logger.error(f"Error in scheduled collection: {e}")
            raise

def load_config_from_env() -> Dict:
    """從環境變數載入配置"""
    return {
        "slave_id": os.getenv("SLAVE_ID", "slave-unknown"),
        "symbols": os.getenv("SYMBOLS", "").split(",") if os.getenv("SYMBOLS") else [],
        "master_url": os.getenv("MASTER_URL", "http://master-vm:8080"),
        "exchange_name": os.getenv("EXCHANGE_NAME", "binance"),
        "db_uri": os.getenv("MONGO_URI", "mongodb://shared-mongo:27017/"),
        "db_name": os.getenv("MONGO_DB_NAME", "trading_data"),
        "timeframe": os.getenv("TIMEFRAME", "1m")  # Changed to 1m for precision matching WebSocket data
    }

def main():
    """主程序入口"""
    config = load_config_from_env()
    
    logger.info(f"Starting Distributed Data Fetcher")
    logger.info(f"Slave ID: {config['slave_id']}")
    logger.info(f"Symbols: {len(config['symbols'])} assigned")
    logger.info(f"Master: {config['master_url']}")
    
    if not config["symbols"] or config["symbols"] == [""]:
        logger.error("No symbols assigned! Check SYMBOLS environment variable.")
        sys.exit(1)
    
    # 創建分散式資料收集器
    fetcher = DistributedDataFetcher(
        slave_id=config["slave_id"],
        symbols=config["symbols"],
        master_url=config["master_url"],
        exchange_name=config["exchange_name"],
        db_uri=config["db_uri"],
        db_name=config["db_name"],
        timeframe=config["timeframe"]
    )
    
    # 啟動定時收集
    fetcher.start_scheduled_collection()

if __name__ == "__main__":
    main()