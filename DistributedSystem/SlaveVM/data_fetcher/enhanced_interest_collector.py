#!/usr/bin/env python3
"""
增強版利率和未平倉合約量收集器
收集 Open Interest 和 Hourly Interest Rate 資料
"""

import requests
import ccxt
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class EnhancedInterestCollector:
    """
    利率和未平倉合約量收集器
    """
    
    def __init__(self, exchange_name: str = "binance"):
        self.exchange = getattr(ccxt, exchange_name)()
        self.futures_base_url = "https://fapi.binance.com"
        self.margin_base_url = "https://api.binance.com"
        
    def fetch_current_open_interest(self, symbol: str) -> Optional[Dict]:
        """
        獲取當前未平倉合約量
        API: /fapi/v1/openInterest
        """
        try:
            # 轉換 symbol 格式
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.futures_base_url}/fapi/v1/openInterest"
            params = {"symbol": binance_symbol}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = {
                "symbol": binance_symbol,
                "open_interest": float(data["openInterest"]),
                "open_interest_value": float(data.get("openInterestValue", 0)),
                "timestamp": int(data.get("time", int(datetime.utcnow().timestamp() * 1000))),
                "data_type": "current_open_interest"
            }
            
            logger.debug(f"Fetched current open interest for {symbol}: {result['open_interest']}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching current open interest for {symbol}: {e}")
            return None
    
    def fetch_historical_open_interest(self, symbol: str, period: str = "1h", limit: int = 24) -> List[Dict]:
        """
        獲取歷史未平倉合約量
        API: /futures/data/openInterestHist
        """
        try:
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.futures_base_url}/futures/data/openInterestHist"
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
                    "symbol": binance_symbol,
                    "open_interest": float(item["sumOpenInterest"]),
                    "open_interest_value": float(item["sumOpenInterestValue"]),
                    "timestamp": int(item["timestamp"]),
                    "count": int(item.get("count", 0)),
                    "data_type": "historical_open_interest"
                })
            
            logger.debug(f"Fetched {len(result)} historical open interest records for {symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical open interest for {symbol}: {e}")
            return []
    
    def fetch_next_hourly_interest_rate(self, assets: List[str] = None) -> List[Dict]:
        """
        獲取下一小時利率 (保證金交易)
        API: /sapi/v1/margin/next-hourly-interest-rate
        """
        try:
            url = f"{self.margin_base_url}/sapi/v1/margin/next-hourly-interest-rate"
            params = {}
            
            # 如果指定資產，則只查詢指定資產
            if assets:
                params["assets"] = ",".join(assets)
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = []
            for item in data:
                result.append({
                    "asset": item["asset"],
                    "next_hourly_interest_rate": float(item["nextHourlyInterestRate"]),
                    "next_hourly_interest_time": int(item["nextHourlyInterestTime"]),
                    "data_type": "next_hourly_interest_rate"
                })
            
            logger.debug(f"Fetched {len(result)} next hourly interest rate records")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching next hourly interest rate: {e}")
            return []
    
    def fetch_current_margin_interest_rate(self, vip_level: int = 0, coin: str = None) -> List[Dict]:
        """
        獲取當前保證金利率
        API: /sapi/v1/margin/interestRateHistory (替代方案)
        """
        try:
            url = f"{self.margin_base_url}/sapi/v1/margin/interestRateHistory"
            params = {"vipLevel": vip_level}
            
            if coin:
                params["coin"] = coin
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            result = []
            for item in data:
                result.append({
                    "asset": item["asset"],
                    "daily_interest_rate": float(item["dailyInterestRate"]),
                    "timestamp": int(item["timestamp"]),
                    "vip_level": int(item["vipLevel"]),
                    "data_type": "current_margin_interest_rate"
                })
            
            logger.debug(f"Fetched {len(result)} current margin interest rate records")
            return result
            
        except Exception as e:
            logger.error(f"Error fetching current margin interest rate: {e}")
            return []
    
    def fetch_all_interest_data(self, symbol: str, include_margin: bool = True) -> Dict:
        """
        獲取所有利率和未平倉合約量資料
        """
        logger.info(f"Fetching all interest data for {symbol}")
        
        result = {
            "symbol": symbol,
            "timestamp": datetime.utcnow(),
            "current_open_interest": None,
            "historical_open_interest": [],
            "next_hourly_interest_rate": [],
            "margin_interest_rates": []
        }
        
        # 1. 當前未平倉合約量
        try:
            current_oi = self.fetch_current_open_interest(symbol)
            if current_oi:
                result["current_open_interest"] = current_oi
                logger.info(f"✅ Current open interest: {current_oi['open_interest']}")
        except Exception as e:
            logger.error(f"Failed to fetch current open interest: {e}")
        
        # 2. 歷史未平倉合約量 (最近24小時)
        try:
            historical_oi = self.fetch_historical_open_interest(symbol, period="1h", limit=24)
            if historical_oi:
                result["historical_open_interest"] = historical_oi
                logger.info(f"✅ Historical open interest: {len(historical_oi)} records")
        except Exception as e:
            logger.error(f"Failed to fetch historical open interest: {e}")
        
        # 3. 下一小時利率 (如果需要保證金資料)
        if include_margin:
            try:
                # 從 symbol 提取基礎資產 (例如 BTC/USDT:USDT -> BTC, USDT)
                base_asset = symbol.split("/")[0] if "/" in symbol else symbol[:3]
                quote_asset = "USDT"
                
                next_rates = self.fetch_next_hourly_interest_rate([base_asset, quote_asset])
                if next_rates:
                    result["next_hourly_interest_rate"] = next_rates
                    logger.info(f"✅ Next hourly interest rates: {len(next_rates)} records")
            except Exception as e:
                logger.error(f"Failed to fetch next hourly interest rate: {e}")
            
            # 4. 當前保證金利率
            try:
                base_asset = symbol.split("/")[0] if "/" in symbol else symbol[:3]
                margin_rates = self.fetch_current_margin_interest_rate(coin=base_asset)
                if margin_rates:
                    result["margin_interest_rates"] = margin_rates
                    logger.info(f"✅ Margin interest rates: {len(margin_rates)} records")
            except Exception as e:
                logger.error(f"Failed to fetch margin interest rates: {e}")
        
        return result
    
    def get_open_interest_summary(self, symbols: List[str]) -> Dict:
        """
        獲取多個 symbols 的未平倉合約量摘要
        """
        logger.info(f"Fetching open interest summary for {len(symbols)} symbols")
        
        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_symbols": len(symbols),
            "open_interest_data": {},
            "top_symbols_by_oi": [],
            "total_open_interest_value": 0
        }
        
        all_oi_data = []
        
        for symbol in symbols:
            try:
                current_oi = self.fetch_current_open_interest(symbol)
                if current_oi:
                    summary["open_interest_data"][symbol] = current_oi
                    summary["total_open_interest_value"] += current_oi.get("open_interest_value", 0)
                    
                    all_oi_data.append({
                        "symbol": symbol,
                        "open_interest": current_oi["open_interest"],
                        "open_interest_value": current_oi.get("open_interest_value", 0)
                    })
                
                # 避免 API 限制
                time.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"Failed to fetch open interest for {symbol}: {e}")
        
        # 按未平倉合約價值排序
        summary["top_symbols_by_oi"] = sorted(
            all_oi_data, 
            key=lambda x: x["open_interest_value"], 
            reverse=True
        )[:20]  # 前20名
        
        return summary
    
    def calculate_open_interest_change(self, symbol: str, hours: int = 24) -> Dict:
        """
        計算未平倉合約量變化
        """
        logger.info(f"Calculating open interest change for {symbol} over {hours} hours")
        
        # 獲取歷史資料
        historical_data = self.fetch_historical_open_interest(symbol, period="1h", limit=hours+1)
        current_data = self.fetch_current_open_interest(symbol)
        
        if not historical_data or not current_data:
            return {"error": "Insufficient data"}
        
        # 計算變化
        oldest_oi = historical_data[0]["open_interest"] if historical_data else 0
        current_oi = current_data["open_interest"]
        
        change_absolute = current_oi - oldest_oi
        change_percentage = (change_absolute / oldest_oi * 100) if oldest_oi > 0 else 0
        
        return {
            "symbol": symbol,
            "period_hours": hours,
            "current_open_interest": current_oi,
            "previous_open_interest": oldest_oi,
            "change_absolute": change_absolute,
            "change_percentage": change_percentage,
            "trend": "increasing" if change_absolute > 0 else "decreasing" if change_absolute < 0 else "stable",
            "data_points": len(historical_data)
        }

def test_interest_collector():
    """
    測試利率和未平倉合約量收集器
    """
    print("🧪 測試利率和未平倉合約量收集器...")
    
    collector = EnhancedInterestCollector()
    test_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT"]
    
    for symbol in test_symbols:
        print(f"\n📊 測試 {symbol}...")
        
        # 測試當前未平倉合約量
        current_oi = collector.fetch_current_open_interest(symbol)
        if current_oi:
            print(f"  ✅ 當前未平倉合約量: {current_oi['open_interest']:,.0f}")
            print(f"     價值: ${current_oi.get('open_interest_value', 0):,.0f}")
        
        # 測試歷史未平倉合約量
        historical_oi = collector.fetch_historical_open_interest(symbol, limit=5)
        if historical_oi:
            print(f"  ✅ 歷史資料: {len(historical_oi)} 筆記錄")
            latest = historical_oi[-1]
            print(f"     最新歷史值: {latest['open_interest']:,.0f}")
        
        # 測試變化計算
        change_data = collector.calculate_open_interest_change(symbol, hours=24)
        if "error" not in change_data:
            print(f"  📈 24小時變化: {change_data['change_percentage']:.2f}% ({change_data['trend']})")
        
        time.sleep(1)  # 避免 API 限制
    
    # 測試利率資料
    print(f"\n💰 測試利率資料...")
    try:
        next_rates = collector.fetch_next_hourly_interest_rate(["BTC", "ETH", "USDT"])
        if next_rates:
            print(f"  ✅ 下一小時利率: {len(next_rates)} 筆記錄")
            for rate in next_rates[:3]:
                print(f"     {rate['asset']}: {rate['next_hourly_interest_rate']:.6f}%")
    except Exception as e:
        print(f"  ⚠️  利率資料: {e}")

if __name__ == "__main__":
    test_interest_collector()