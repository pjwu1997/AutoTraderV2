#!/usr/bin/env python3
"""
增強版分散式資料收集器 - 基於原本 DataFetcher，加入多空比和利率資料
保持原本邏輯，只擴展 schema
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from DataFetcher.data_fetcher import DataFetcher
from enhanced_long_short_collector import EnhancedLongShortCollector
from enhanced_interest_collector import EnhancedInterestCollector
import datetime
import logging
from typing import List

logger = logging.getLogger(__name__)

class EnhancedDataFetcher(DataFetcher):
    """
    增強版資料收集器 - 基於原本的 DataFetcher
    保持所有原本的邏輯，只是在 schema 中加入新的資料
    """
    
    def __init__(self, slave_id: str, **kwargs):
        # 使用原本的初始化邏輯
        super().__init__(**kwargs)
        
        self.slave_id = slave_id
        
        # 初始化增強收集器
        self.long_short_collector = EnhancedLongShortCollector()
        self.interest_collector = EnhancedInterestCollector()
        
        logger.info(f"Enhanced DataFetcher initialized for {slave_id}")
    
    def fetch_enhanced_long_short_ratio(self, symbol, since):
        """
        增強版多空比收集 - 替代原本的 fetch_long_short_ratio
        """
        try:
            # 先嘗試原本的方法
            original_result = super().fetch_long_short_ratio(symbol, since)
            
            # 再收集增強版資料
            enhanced_data = self.long_short_collector.fetch_all_long_short_data(
                symbol, period=self.timeframe, limit=1
            )
            
            # 合併結果
            result = []
            if original_result:
                result = original_result
            elif enhanced_data:
                # 如果原本方法失敗，使用增強版資料
                if enhanced_data.get("global_account_ratio"):
                    latest = enhanced_data["global_account_ratio"][-1]
                    result = [{
                        "timestamp": latest["timestamp"],
                        "long_short_ratio": latest["long_short_ratio"]
                    }]
            
            return result
            
        except Exception as e:
            logger.error(f"Error in enhanced long-short ratio for {symbol}: {e}")
            return []
    
    def fetch_and_store(self, symbol):
        """
        擴展原本的 fetch_and_store 方法
        保持完全相同的邏輯，只是在最後儲存時加入更多資料
        """
        # === 完全使用原本的邏輯 ===
        # Ensure we fetch at least 2 candles by subtracting twice the timeframe duration
        min_candles = 2
        timeframe_minutes = int(self.timeframe[:-1])  # Extract the numeric part of the timeframe (e.g., 5m -> 5)
        current_time = datetime.datetime.utcnow()
        since = int((datetime.datetime.utcnow() - datetime.timedelta(minutes=timeframe_minutes * (min_candles + 1))).timestamp() * 1000)
        minutes = (current_time.minute // 5) * 5
        timestamp = current_time.replace(minute=minutes, second=0, microsecond=0)
        
        # Fetch data - 使用原本的方法
        print(f"Fetching OHLCV data for {symbol}...")
        ohlcv = self.fetch_ohlcv(symbol, since)

        print(f"Fetching spot CVD for {symbol}...")
        spot_cvd = self.get_spot_cvd(symbol[:-5], since, period=self.timeframe)

        print(f"Fetching long-short ratio for {symbol}...")
        long_short_ratio = self.fetch_enhanced_long_short_ratio(symbol, since)  # 使用增強版

        print(f"Calculating CVD for {symbol}...")
        cvd = self.fetch_cvd(symbol, since)

        print(f"Fetching Funding Rate for {symbol}...")
        fundings = self.fetch_funding_rate(symbol, since)
        
        # === 新增：收集增強版資料 ===
        print(f"Fetching enhanced market data for {symbol}...")
        
        # 收集完整的多空比資料
        enhanced_long_short = self.long_short_collector.fetch_all_long_short_data(
            symbol, period=self.timeframe, limit=1
        )
        
        # 收集利率和未平倉合約量資料
        enhanced_interest = self.interest_collector.fetch_all_interest_data(
            symbol, include_margin=False  # 避免需要權限的API
        )

        # === 使用原本的資料結構，但擴展內容 ===
        # Handle potential None values - 保持原本邏輯
        data = {
            "symbol": symbol,
            "exchange": self.exchange.id,
            "ohlcv": ohlcv[-1] if ohlcv else None,
            "spot_cvd": spot_cvd[-1] if spot_cvd else None,
            "long_short_ratio": long_short_ratio[-1] if long_short_ratio else None,
            "cvd": cvd[-1] if cvd else None,
            "funding_rate": fundings[-1] if fundings else None,
            "timestamp": timestamp,
            
            # === 新增：增強版資料 ===
            "enhanced_long_short": self._format_enhanced_long_short(enhanced_long_short),
            "enhanced_interest": self._format_enhanced_interest(enhanced_interest),
            "collector_id": self.slave_id
        }

        # Store data in MongoDB - 使用原本的方法
        print(f"Storing enhanced data for {symbol}...")
        self.store_data("market_data", symbol, data)
    
    def _format_enhanced_long_short(self, enhanced_data):
        """格式化增強版多空比資料"""
        try:
            if not enhanced_data:
                return {}
            
            result = {}
            
            # 全域帳戶多空比
            if enhanced_data.get("global_account_ratio"):
                latest = enhanced_data["global_account_ratio"][-1]
                result["global"] = {
                    "long_short_ratio": latest["long_short_ratio"],
                    "long_account": latest["long_account"],
                    "short_account": latest["short_account"],
                    "timestamp": latest["timestamp"]
                }
            
            # 頂級交易者帳戶多空比
            if enhanced_data.get("top_trader_account_ratio"):
                latest = enhanced_data["top_trader_account_ratio"][-1]
                result["top_trader_account"] = {
                    "long_short_ratio": latest["long_short_ratio"],
                    "long_account": latest["long_account"],
                    "short_account": latest["short_account"],
                    "timestamp": latest["timestamp"]
                }
            
            # 頂級交易者倉位多空比
            if enhanced_data.get("top_trader_position_ratio"):
                latest = enhanced_data["top_trader_position_ratio"][-1]
                result["top_trader_position"] = {
                    "long_short_ratio": latest["long_short_ratio"],
                    "long_position": latest.get("long_position", 0),
                    "short_position": latest.get("short_position", 0),
                    "timestamp": latest["timestamp"]
                }
            
            # Taker 買賣比例
            if enhanced_data.get("taker_buy_sell_ratio"):
                latest = enhanced_data["taker_buy_sell_ratio"][-1]
                result["taker_buy_sell"] = {
                    "buy_sell_ratio": latest["buy_sell_ratio"],
                    "buy_volume": latest["buy_vol"],
                    "sell_volume": latest["sell_vol"],
                    "timestamp": latest["timestamp"]
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Error formatting enhanced long-short data: {e}")
            return {}
    
    def _format_enhanced_interest(self, enhanced_data):
        """格式化增強版利率資料"""
        try:
            if not enhanced_data:
                return {}
            
            result = {}
            
            # 當前未平倉合約量
            if enhanced_data.get("current_open_interest"):
                current_oi = enhanced_data["current_open_interest"]
                result["current_open_interest"] = {
                    "open_interest": current_oi["open_interest"],
                    "open_interest_value": current_oi.get("open_interest_value", 0),
                    "timestamp": current_oi["timestamp"]
                }
            
            # 歷史未平倉合約量變化
            if enhanced_data.get("historical_open_interest"):
                historical = enhanced_data["historical_open_interest"]
                if len(historical) >= 2:
                    latest = historical[-1]
                    previous = historical[-2]
                    change = latest["open_interest"] - previous["open_interest"]
                    change_percent = (change / previous["open_interest"] * 100) if previous["open_interest"] > 0 else 0
                    
                    result["open_interest_change"] = {
                        "change_absolute": change,
                        "change_percent": change_percent,
                        "trend": "increasing" if change > 0 else "decreasing" if change < 0 else "stable"
                    }
            
            return result
            
        except Exception as e:
            logger.error(f"Error formatting enhanced interest data: {e}")
            return {}

# 測試函數
def test_enhanced_fetcher():
    """測試增強版資料收集器"""
    print("🧪 測試增強版資料收集器...")
    
    fetcher = EnhancedDataFetcher(
        slave_id="test-slave",
        exchange_name="binance",
        timeframe="5m"
    )
    
    test_symbol = "BTC/USDT:USDT"
    
    try:
        # 測試收集和儲存
        fetcher.fetch_and_store(test_symbol)
        print("✅ 增強版資料收集成功!")
        
    except Exception as e:
        print(f"❌ 測試失敗: {e}")

if __name__ == "__main__":
    test_enhanced_fetcher()