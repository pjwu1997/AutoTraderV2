#!/usr/bin/env python3
"""
增強版多空比收集器 - 支援多種 Binance Long-Short Ratio API
"""

import requests
import ccxt
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class EnhancedLongShortCollector:
    """
    多空比資料收集器 - 支援多種 Binance API 端點
    """
    
    def __init__(self, exchange_name: str = "binance"):
        self.exchange = getattr(ccxt, exchange_name)()
        self.base_url = "https://fapi.binance.com"
        
    def fetch_global_long_short_ratio(self, symbol: str, period: str = "5m", limit: int = 30) -> List[Dict]:
        """
        獲取全域多空比 (所有用戶)
        API: /futures/data/globalLongShortAccountRatio
        """
        try:
            # 轉換 symbol 格式 (移除 :USDT 後綴)
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.base_url}/futures/data/globalLongShortAccountRatio"
            params = {
                "symbol": binance_symbol,
                "period": period,  # 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
                "limit": limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = []
            for item in data:
                result.append({
                    "timestamp": int(item["timestamp"]),
                    "long_short_ratio": float(item["longShortRatio"]),
                    "long_account": float(item["longAccount"]),
                    "short_account": float(item["shortAccount"]),
                    "ratio_type": "global_account"
                })
            
            logger.debug(f"Fetched {len(result)} global long-short ratio records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching global long-short ratio for {symbol}: {e}")
            return []
    
    def fetch_top_trader_account_ratio(self, symbol: str, period: str = "5m", limit: int = 30) -> List[Dict]:
        """
        獲取頂級交易者帳戶多空比 (前20%用戶按保證金餘額)
        API: /futures/data/topLongShortAccountRatio
        """
        try:
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.base_url}/futures/data/topLongShortAccountRatio"
            params = {
                "symbol": binance_symbol,
                "period": period,
                "limit": limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = []
            for item in data:
                result.append({
                    "timestamp": int(item["timestamp"]),
                    "long_short_ratio": float(item["longShortRatio"]),
                    "long_account": float(item["longAccount"]),
                    "short_account": float(item["shortAccount"]),
                    "ratio_type": "top_trader_account"
                })
            
            logger.debug(f"Fetched {len(result)} top trader account ratio records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching top trader account ratio for {symbol}: {e}")
            return []
    
    def fetch_top_trader_position_ratio(self, symbol: str, period: str = "5m", limit: int = 30) -> List[Dict]:
        """
        獲取頂級交易者倉位多空比 (前20%用戶按保證金餘額)
        API: /futures/data/topLongShortPositionRatio
        """
        try:
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.base_url}/futures/data/topLongShortPositionRatio"
            params = {
                "symbol": binance_symbol,
                "period": period,
                "limit": limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = []
            for item in data:
                result.append({
                    "timestamp": int(item["timestamp"]),
                    "long_short_ratio": float(item["longShortRatio"]),
                    "long_position": float(item["longPosition"]),
                    "short_position": float(item["shortPosition"]),
                    "ratio_type": "top_trader_position"
                })
            
            logger.debug(f"Fetched {len(result)} top trader position ratio records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching top trader position ratio for {symbol}: {e}")
            return []
    
    def fetch_taker_long_short_ratio(self, symbol: str, period: str = "5m", limit: int = 30) -> List[Dict]:
        """
        獲取 Taker 買賣成交量比例 (主動買入vs主動賣出)
        API: /futures/data/takerlongshortRatio
        """
        try:
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.base_url}/futures/data/takerlongshortRatio"
            params = {
                "symbol": binance_symbol,
                "period": period,
                "limit": limit
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = []
            for item in data:
                result.append({
                    "timestamp": int(item["timestamp"]),
                    "buy_sell_ratio": float(item["buySellRatio"]),
                    "buy_vol": float(item["buyVol"]),
                    "sell_vol": float(item["sellVol"]),
                    "ratio_type": "taker_buy_sell"
                })
            
            logger.debug(f"Fetched {len(result)} taker long-short ratio records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching taker long-short ratio for {symbol}: {e}")
            return []
    
    def fetch_ccxt_long_short_ratio(self, symbol: str, since: int, timeframe: str = "5m") -> List[Dict]:
        """
        使用 CCXT 獲取多空比 (備用方法)
        """
        try:
            if not self.exchange.has.get("fetchLongShortRatioHistory", False):
                logger.warning(f"{self.exchange.id} does not support fetchLongShortRatioHistory")
                return []
            
            # 使用 CCXT 的統一 API
            long_short_data = self.exchange.fetchLongShortRatioHistory(
                symbol, 
                timeframe=timeframe,
                since=since,
                limit=30
            )
            
            result = []
            for item in long_short_data:
                result.append({
                    "timestamp": item["timestamp"],
                    "long_short_ratio": item["longShortRatio"],
                    "ratio_type": "ccxt_unified"
                })
            
            logger.debug(f"Fetched {len(result)} CCXT long-short ratio records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching CCXT long-short ratio for {symbol}: {e}")
            return []
    
    def fetch_all_long_short_data(self, symbol: str, period: str = "5m", limit: int = 10) -> Dict:
        """
        獲取所有類型的多空比資料
        """
        logger.info(f"Fetching all long-short data for {symbol}")
        
        result = {
            "symbol": symbol,
            "timestamp": datetime.utcnow(),
            "global_account_ratio": [],
            "top_trader_account_ratio": [],
            "top_trader_position_ratio": [],
            "taker_buy_sell_ratio": []
        }
        
        # 1. 全域帳戶多空比
        try:
            global_data = self.fetch_global_long_short_ratio(symbol, period, limit)
            if global_data:
                result["global_account_ratio"] = global_data
                logger.info(f"✅ Global account ratio: {len(global_data)} records")
        except Exception as e:
            logger.error(f"Failed to fetch global account ratio: {e}")
        
        # 2. 頂級交易者帳戶多空比  
        try:
            top_account_data = self.fetch_top_trader_account_ratio(symbol, period, limit)
            if top_account_data:
                result["top_trader_account_ratio"] = top_account_data
                logger.info(f"✅ Top trader account ratio: {len(top_account_data)} records")
        except Exception as e:
            logger.error(f"Failed to fetch top trader account ratio: {e}")
        
        # 3. 頂級交易者倉位多空比
        try:
            top_position_data = self.fetch_top_trader_position_ratio(symbol, period, limit)
            if top_position_data:
                result["top_trader_position_ratio"] = top_position_data
                logger.info(f"✅ Top trader position ratio: {len(top_position_data)} records")
        except Exception as e:
            logger.error(f"Failed to fetch top trader position ratio: {e}")
        
        # 4. Taker 買賣比例
        try:
            taker_data = self.fetch_taker_long_short_ratio(symbol, period, limit)
            if taker_data:
                result["taker_buy_sell_ratio"] = taker_data
                logger.info(f"✅ Taker buy/sell ratio: {len(taker_data)} records")
        except Exception as e:
            logger.error(f"Failed to fetch taker buy/sell ratio: {e}")
        
        return result
    
    def get_latest_ratio_summary(self, symbol: str) -> Dict:
        """
        獲取最新的多空比摘要
        """
        all_data = self.fetch_all_long_short_data(symbol, period="5m", limit=1)
        
        summary = {
            "symbol": symbol,
            "timestamp": datetime.utcnow().isoformat(),
            "latest_ratios": {}
        }
        
        # 提取最新的比例資料
        for ratio_type, data_list in all_data.items():
            if ratio_type not in ["symbol", "timestamp"] and data_list:
                latest = data_list[-1]  # 最新的記錄
                summary["latest_ratios"][ratio_type] = {
                    "ratio": latest.get("long_short_ratio", 0),
                    "timestamp": latest.get("timestamp", 0)
                }
        
        return summary

def test_long_short_collector():
    """
    測試多空比收集器
    """
    print("🧪 測試多空比收集器...")
    
    collector = EnhancedLongShortCollector()
    test_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    
    for symbol in test_symbols:
        print(f"\n📊 測試 {symbol}...")
        
        # 測試所有多空比資料
        all_data = collector.fetch_all_long_short_data(symbol, limit=5)
        
        print(f"結果:")
        for data_type, records in all_data.items():
            if data_type not in ["symbol", "timestamp"] and records:
                print(f"  - {data_type}: {len(records)} 筆記錄")
                if records:
                    latest = records[-1]
                    print(f"    最新比例: {latest.get('long_short_ratio', 'N/A')}")
        
        # 測試摘要
        summary = collector.get_latest_ratio_summary(symbol)
        print(f"📈 最新摘要: {summary['latest_ratios']}")
        
        time.sleep(1)  # 避免 API 限制

if __name__ == "__main__":
    test_long_short_collector()