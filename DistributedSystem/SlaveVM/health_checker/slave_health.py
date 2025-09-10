#!/usr/bin/env python3
"""
Slave 健康檢查器 - 監控本機狀態並回報給 Master
"""

import asyncio
import json
import os
import sys
import psutil
import time
from datetime import datetime
from typing import Dict, Optional
from aiohttp import web
import aiohttp
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from Common.models.data_models import HealthStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SlaveHealthChecker:
    def __init__(self, slave_id: str, master_url: str, port: int = 8081):
        self.slave_id = slave_id
        self.master_url = master_url
        self.port = port
        self.start_time = datetime.utcnow()
        self.last_successful_fetch = None
        self.error_count = 0
        self.symbols_processed = 0
        
    def get_system_health(self) -> HealthStatus:
        """獲取系統健康狀況"""
        try:
            # CPU 使用率
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # 記憶體使用率
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # 磁碟使用率
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # 網路連通性測試
            network_status = self.test_network_connectivity()
            
            # MongoDB 連接測試
            mongo_connection = self.test_mongo_connection()
            
            return HealthStatus(
                timestamp=datetime.utcnow(),
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                disk_usage=disk_usage,
                network_status=network_status,
                mongo_connection=mongo_connection,
                symbols_processed=self.symbols_processed,
                error_count=self.error_count,
                last_successful_fetch=self.last_successful_fetch
            )
            
        except Exception as e:
            logger.error(f"Error getting system health: {e}")
            return HealthStatus(
                timestamp=datetime.utcnow(),
                cpu_usage=0,
                memory_usage=0,
                disk_usage=0,
                network_status=False,
                mongo_connection=False,
                symbols_processed=self.symbols_processed,
                error_count=self.error_count + 1
            )
    
    def test_network_connectivity(self) -> bool:
        """測試網路連通性"""
        try:
            import socket
            socket.create_connection(("8.8.8.8", 53), timeout=3)
            return True
        except:
            return False
    
    def test_mongo_connection(self) -> bool:
        """測試 MongoDB 連接"""
        try:
            from pymongo import MongoClient
            mongo_uri = os.getenv("MONGO_URI", "mongodb://shared-mongo:27017/")
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
            client.server_info()  # 觸發連接測試
            client.close()
            return True
        except Exception as e:
            logger.debug(f"MongoDB connection test failed: {e}")
            return False
    
    def test_binance_api(self) -> bool:
        """測試 Binance API 連通性"""
        try:
            import requests
            response = requests.get("https://api.binance.com/api/v3/ping", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    async def send_heartbeat_to_master(self, health_status: HealthStatus) -> bool:
        """發送心跳到 Master"""
        try:
            async with aiohttp.ClientSession() as session:
                data = health_status.to_dict()
                
                async with session.post(
                    f"{self.master_url}/api/heartbeat/{self.slave_id}",
                    json=data,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        if "warning" in result:
                            logger.warning(f"Master warning: {result['warning']}")
                        return True
                    else:
                        logger.warning(f"Heartbeat failed: HTTP {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error sending heartbeat to master: {e}")
            return False
    
    def update_stats(self, symbols_processed: int = 0, error_count: int = 0):
        """更新統計資料"""
        if symbols_processed > 0:
            self.symbols_processed += symbols_processed
            self.last_successful_fetch = datetime.utcnow()
        
        if error_count > 0:
            self.error_count += error_count

# Web API 處理器
async def handle_health(request):
    """健康檢查端點"""
    health_checker = request.app["health_checker"]
    
    try:
        health_status = health_checker.get_system_health()
        
        # 額外檢查 Binance API
        binance_status = health_checker.test_binance_api()
        
        response_data = health_status.to_dict()
        response_data["binance_api"] = binance_status
        response_data["uptime_seconds"] = (datetime.utcnow() - health_checker.start_time).total_seconds()
        
        return web.json_response(response_data)
        
    except Exception as e:
        logger.error(f"Error in health check: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def handle_stats_update(request):
    """更新統計資料端點"""
    health_checker = request.app["health_checker"]
    data = await request.json()
    
    try:
        symbols_processed = data.get("symbols_processed", 0)
        error_count = data.get("error_count", 0)
        
        health_checker.update_stats(symbols_processed, error_count)
        
        return web.json_response({"status": "updated"})
        
    except Exception as e:
        logger.error(f"Error updating stats: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def periodic_heartbeat(app):
    """定期心跳任務"""
    health_checker = app["health_checker"]
    
    while True:
        try:
            health_status = health_checker.get_system_health()
            await health_checker.send_heartbeat_to_master(health_status)
            
            # 記錄健康狀況
            if health_status.cpu_usage > 80:
                logger.warning(f"High CPU usage: {health_status.cpu_usage:.1f}%")
            if health_status.memory_usage > 80:
                logger.warning(f"High memory usage: {health_status.memory_usage:.1f}%")
            if not health_status.mongo_connection:
                logger.error("MongoDB connection failed")
                
        except Exception as e:
            logger.error(f"Error in periodic heartbeat: {e}")
        
        await asyncio.sleep(30)  # 每30秒發送一次心跳

async def init_health_app(slave_id: str, master_url: str, port: int = 8081):
    """初始化健康檢查應用"""
    app = web.Application()
    
    # 初始化健康檢查器
    health_checker = SlaveHealthChecker(slave_id, master_url, port)
    app["health_checker"] = health_checker
    
    # 註冊路由
    app.router.add_get("/health", handle_health)
    app.router.add_post("/stats", handle_stats_update)
    
    # 啟動定期心跳任務
    asyncio.create_task(periodic_heartbeat(app))
    
    return app

async def main():
    """主程序"""
    # 從環境變數載入配置
    slave_id = os.getenv("SLAVE_ID", "slave-unknown")
    master_url = os.getenv("MASTER_URL", "http://master-vm:8080")
    port = int(os.getenv("HEALTH_PORT", "8081"))
    
    logger.info(f"Starting Health Checker for {slave_id}")
    logger.info(f"Master URL: {master_url}")
    logger.info(f"Health port: {port}")
    
    app = await init_health_app(slave_id, master_url, port)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logger.info(f"Health checker started on port {port}")
    logger.info(f"Health endpoint: http://localhost:{port}/health")
    
    try:
        await asyncio.Future()  # 永遠等待
    except KeyboardInterrupt:
        logger.info("Shutting down health checker...")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())