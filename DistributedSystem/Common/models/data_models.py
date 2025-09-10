#!/usr/bin/env python3
"""
共用資料模型定義
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
import json

@dataclass
class SlaveInfo:
    slave_id: str
    ip_address: str
    assigned_symbols: List[str]
    status: str  # 'online', 'offline', 'error'
    last_heartbeat: datetime
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    error_count: int = 0
    symbols_processed: int = 0
    
    def to_dict(self) -> dict:
        return {
            'slave_id': self.slave_id,
            'ip_address': self.ip_address,
            'assigned_symbols': self.assigned_symbols,
            'status': self.status,
            'last_heartbeat': self.last_heartbeat.isoformat(),
            'cpu_usage': self.cpu_usage,
            'memory_usage': self.memory_usage,
            'error_count': self.error_count,
            'symbols_processed': self.symbols_processed
        }

@dataclass
class HealthStatus:
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_status: bool
    mongo_connection: bool
    symbols_processed: int
    error_count: int
    last_successful_fetch: Optional[datetime] = None
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp.isoformat(),
            'cpu_usage': self.cpu_usage,
            'memory_usage': self.memory_usage,
            'disk_usage': self.disk_usage,
            'network_status': self.network_status,
            'mongo_connection': self.mongo_connection,
            'symbols_processed': self.symbols_processed,
            'error_count': self.error_count,
            'last_successful_fetch': self.last_successful_fetch.isoformat() if self.last_successful_fetch else None
        }

@dataclass
class MarketDataPoint:
    symbol: str
    timestamp: datetime
    exchange: str
    ohlcv: Optional[dict] = None
    spot_cvd: Optional[dict] = None
    long_short_ratio: Optional[dict] = None
    cvd: Optional[dict] = None
    funding_rate: Optional[dict] = None
    
    def to_dict(self) -> dict:
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp.isoformat(),
            'exchange': self.exchange,
            'ohlcv': self.ohlcv,
            'spot_cvd': self.spot_cvd,
            'long_short_ratio': self.long_short_ratio,
            'cvd': self.cvd,
            'funding_rate': self.funding_rate
        }

@dataclass
class SystemOverview:
    timestamp: datetime
    total_slaves: int
    online_slaves: int
    total_symbols: int
    avg_cpu: float
    avg_memory: float
    total_errors: int
    data_points_last_hour: int
    
    def to_dict(self) -> dict:
        return {
            'timestamp': self.timestamp.isoformat(),
            'total_slaves': self.total_slaves,
            'online_slaves': self.online_slaves,
            'total_symbols': self.total_symbols,
            'avg_cpu': self.avg_cpu,
            'avg_memory': self.avg_memory,
            'total_errors': self.total_errors,
            'data_points_last_hour': self.data_points_last_hour
        }