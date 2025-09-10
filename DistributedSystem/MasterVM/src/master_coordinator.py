#!/usr/bin/env python3
"""
Master VM 協調器 - 監控所有 Slave VM 狀態，不直接收集資料
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import aiohttp
from aiohttp import web
import sys
import os

# 加入共用模組路徑
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from Common.models.data_models import SlaveInfo, HealthStatus, SystemOverview

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MasterCoordinator:
    def __init__(self, config_path: str = "config/master_config.json"):
        self.slaves: Dict[str, SlaveInfo] = {}
        self.config = self.load_config(config_path)
        self.session: Optional[aiohttp.ClientSession] = None
        self.monitoring_task: Optional[asyncio.Task] = None
        
    def load_config(self, config_path: str) -> dict:
        """載入 Master 配置"""
        default_config = {
            "master_port": 8080,
            "heartbeat_interval": 30,
            "heartbeat_timeout": 120,
            "slave_endpoints": [
                "http://slave-1:8081",
                "http://slave-2:8081", 
                "http://slave-3:8081",
                "http://slave-4:8081",
                "http://slave-5:8081"
            ],
            "mongo_uri": "mongodb://shared-mongo:27017/",
            "mongo_db": "trading_data"
        }
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                return {**default_config, **config}
        except FileNotFoundError:
            logger.warning(f"Config file {config_path} not found, using defaults")
            return default_config
    
    async def initialize(self):
        """初始化 Master 服務"""
        logger.info("Initializing Master Coordinator...")
        
        # 建立 HTTP 客戶端 session
        self.session = aiohttp.ClientSession()
        
        # 啟動監控任務
        self.monitoring_task = asyncio.create_task(self.monitor_slaves())
        
        logger.info("Master Coordinator initialized")
    
    async def register_slave(self, request_data: dict) -> dict:
        """註冊新的 Slave"""
        slave_id = request_data.get("slave_id")
        if not slave_id:
            raise ValueError("Missing slave_id")
        
        slave_info = SlaveInfo(
            slave_id=slave_id,
            ip_address=request_data.get("ip_address", "unknown"),
            assigned_symbols=request_data.get("assigned_symbols", []),
            status="online",
            last_heartbeat=datetime.utcnow(),
            cpu_usage=request_data.get("cpu_usage", 0),
            memory_usage=request_data.get("memory_usage", 0)
        )
        
        self.slaves[slave_id] = slave_info
        logger.info(f"Registered slave {slave_id} with {len(slave_info.assigned_symbols)} symbols")
        
        return {
            "status": "registered",
            "slave_id": slave_id,
            "config": {
                "heartbeat_interval": self.config["heartbeat_interval"],
                "fetch_interval": 60,
                "mongo_uri": self.config["mongo_uri"],
                "mongo_db": self.config["mongo_db"]
            }
        }
    
    async def handle_heartbeat(self, slave_id: str, health_data: dict) -> dict:
        """處理 Slave 心跳"""
        if slave_id not in self.slaves:
            return {"status": "error", "message": "Slave not registered"}
        
        slave = self.slaves[slave_id]
        slave.last_heartbeat = datetime.utcnow()
        slave.status = health_data.get("status", "online")
        slave.cpu_usage = health_data.get("cpu_usage", 0)
        slave.memory_usage = health_data.get("memory_usage", 0)
        slave.error_count = health_data.get("error_count", 0)
        slave.symbols_processed = health_data.get("symbols_processed", 0)
        
        # 檢查健康狀況
        response = {"status": "ok", "timestamp": datetime.utcnow().isoformat()}
        
        # 如果錯誤太多，建議減少負載
        if slave.error_count > 20:
            response["warning"] = "High error count detected"
            response["suggestion"] = "Consider reducing symbol load"
        
        # 如果 CPU/記憶體使用率太高
        if slave.cpu_usage > 80 or slave.memory_usage > 80:
            response["warning"] = "High resource usage"
            response["suggestion"] = "Consider scaling resources"
        
        return response
    
    async def monitor_slaves(self):
        """監控所有 Slave 狀態"""
        logger.info("Starting slave monitoring...")
        
        while True:
            try:
                timeout_threshold = datetime.utcnow() - timedelta(seconds=self.config["heartbeat_timeout"])
                
                for slave_id, slave in self.slaves.items():
                    # 檢查超時
                    if slave.last_heartbeat < timeout_threshold:
                        if slave.status != "offline":
                            slave.status = "offline"
                            logger.warning(f"Slave {slave_id} went offline (last heartbeat: {slave.last_heartbeat})")
                    
                    # 嘗試主動健康檢查
                    await self.ping_slave(slave_id)
                
                # 記錄狀態摘要
                online_count = sum(1 for s in self.slaves.values() if s.status == "online")
                logger.info(f"Slave status: {online_count}/{len(self.slaves)} online")
                
            except Exception as e:
                logger.error(f"Error in slave monitoring: {e}")
            
            await asyncio.sleep(self.config["heartbeat_interval"])
    
    async def ping_slave(self, slave_id: str):
        """主動 ping slave 檢查連通性"""
        if not self.session:
            return
        
        slave = self.slaves.get(slave_id)
        if not slave:
            return
        
        try:
            # 根據 slave_id 找到對應的端點
            endpoint_index = int(slave_id.split('-')[1]) - 1
            if endpoint_index < len(self.config["slave_endpoints"]):
                endpoint = self.config["slave_endpoints"][endpoint_index]
                
                async with self.session.get(f"{endpoint}/health", timeout=5) as response:
                    if response.status == 200:
                        health_data = await response.json()
                        await self.handle_heartbeat(slave_id, health_data)
                    else:
                        logger.warning(f"Slave {slave_id} health check failed: HTTP {response.status}")
                        
        except Exception as e:
            logger.warning(f"Failed to ping slave {slave_id}: {e}")
    
    async def get_system_overview(self) -> SystemOverview:
        """獲取系統總覽"""
        online_slaves = [s for s in self.slaves.values() if s.status == "online"]
        
        avg_cpu = sum(s.cpu_usage for s in online_slaves) / len(online_slaves) if online_slaves else 0
        avg_memory = sum(s.memory_usage for s in online_slaves) / len(online_slaves) if online_slaves else 0
        total_errors = sum(s.error_count for s in self.slaves.values())
        total_symbols = sum(len(s.assigned_symbols) for s in self.slaves.values())
        
        return SystemOverview(
            timestamp=datetime.utcnow(),
            total_slaves=len(self.slaves),
            online_slaves=len(online_slaves),
            total_symbols=total_symbols,
            avg_cpu=avg_cpu,
            avg_memory=avg_memory,
            total_errors=total_errors,
            data_points_last_hour=0  # 需要從 MongoDB 查詢
        )
    
    async def cleanup(self):
        """清理資源"""
        if self.monitoring_task:
            self.monitoring_task.cancel()
        if self.session:
            await self.session.close()

# Web API 處理器
async def handle_register(request):
    coordinator = request.app["coordinator"]
    data = await request.json()
    
    try:
        result = await coordinator.register_slave(data)
        return web.json_response(result)
    except Exception as e:
        logger.error(f"Error registering slave: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def handle_heartbeat(request):
    coordinator = request.app["coordinator"]
    slave_id = request.match_info["slave_id"]
    data = await request.json()
    
    try:
        result = await coordinator.handle_heartbeat(slave_id, data)
        return web.json_response(result)
    except Exception as e:
        logger.error(f"Error handling heartbeat: {e}")
        return web.json_response({"error": str(e)}, status=400)

async def handle_status(request):
    coordinator = request.app["coordinator"]
    
    try:
        overview = await coordinator.get_system_overview()
        return web.json_response(overview.to_dict())
    except Exception as e:
        logger.error(f"Error getting status: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def handle_slaves(request):
    coordinator = request.app["coordinator"]
    
    try:
        slaves_data = {
            slave_id: slave.to_dict() 
            for slave_id, slave in coordinator.slaves.items()
        }
        return web.json_response(slaves_data)
    except Exception as e:
        logger.error(f"Error getting slaves: {e}")
        return web.json_response({"error": str(e)}, status=500)

async def init_app():
    """初始化 web 應用"""
    app = web.Application()
    
    # 初始化 coordinator
    coordinator = MasterCoordinator()
    await coordinator.initialize()
    app["coordinator"] = coordinator
    
    # 註冊路由
    app.router.add_post("/api/register", handle_register)
    app.router.add_post("/api/heartbeat/{slave_id}", handle_heartbeat)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/slaves", handle_slaves)
    
    # 靜態檔案服務 (Dashboard)
    app.router.add_static("/", path="templates", name="static")
    
    # 清理處理
    async def cleanup_handler(app):
        await coordinator.cleanup()
    
    app.on_cleanup.append(cleanup_handler)
    
    return app

async def main():
    """主程序"""
    app = await init_app()
    
    coordinator = app["coordinator"]
    port = coordinator.config["master_port"]
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logger.info(f"Master Coordinator started on port {port}")
    logger.info(f"Dashboard: http://localhost:{port}/dashboard.html")
    
    try:
        await asyncio.Future()  # 永遠等待
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())