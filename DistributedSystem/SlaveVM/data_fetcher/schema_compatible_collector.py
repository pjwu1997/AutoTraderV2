#!/usr/bin/env python3
"""
兼容現有 DB Schema 的增強資料收集器
整合新功能到現有的資料結構中
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from DataFetcher.data_fetcher import DataFetcher
from enhanced_long_short_collector import EnhancedLongShortCollector
from enhanced_interest_collector import EnhancedInterestCollector
from enhanced_funding_collector import EnhancedFundingCollector
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class SchemaCompatibleCollector(DataFetcher):
    """
    兼容現有 DB Schema 的資料收集器
    擴展原有結構，加入新的多空比和利率資料
    """
    
    def __init__(self, slave_id: str, **kwargs):
        super().__init__(**kwargs)
        self.slave_id = slave_id
        
        # 初始化增強收集器
        self.long_short_collector = EnhancedLongShortCollector()
        self.interest_collector = EnhancedInterestCollector()
        self.funding_collector = EnhancedFundingCollector()
        
        logger.info(f"Schema Compatible Collector initialized for {slave_id}")
    
    def fetch_and_store(self, symbol: str):
        """Override parent fetch_and_store to use enhanced method with per-symbol collections"""
        print(f"🚀 OVERRIDDEN: fetch_and_store called for {symbol}, redirecting to enhanced")
        return self.fetch_and_store_enhanced(symbol)
    
    def fetch_enhanced_long_short_data(self, symbol: str) -> Dict:
        """
        收集增強版多空比資料，兼容現有 schema
        """
        try:
            # 使用增強收集器獲取所有多空比資料
            all_long_short = self.long_short_collector.fetch_all_long_short_data(
                symbol, period="1m", limit=1  # Changed to 1m for precision
            )
            
            # 轉換為兼容格式
            enhanced_data = {}
            
            # 全域帳戶多空比
            if all_long_short.get("global_account_ratio"):
                latest_global = all_long_short["global_account_ratio"][-1]
                enhanced_data["global_long_short_ratio"] = latest_global["long_short_ratio"]
                enhanced_data["global_long_account"] = latest_global["long_account"]
                enhanced_data["global_short_account"] = latest_global["short_account"]
            
            # 頂級交易者帳戶多空比
            if all_long_short.get("top_trader_account_ratio"):
                latest_top_account = all_long_short["top_trader_account_ratio"][-1]
                enhanced_data["top_trader_long_short_ratio"] = latest_top_account["long_short_ratio"]
                enhanced_data["top_trader_long_account"] = latest_top_account["long_account"]
                enhanced_data["top_trader_short_account"] = latest_top_account["short_account"]
            
            # 頂級交易者倉位多空比
            if all_long_short.get("top_trader_position_ratio"):
                latest_top_position = all_long_short["top_trader_position_ratio"][-1]
                enhanced_data["top_trader_position_ratio"] = latest_top_position["long_short_ratio"]
                enhanced_data["top_trader_long_position"] = latest_top_position.get("long_position", 0)
                enhanced_data["top_trader_short_position"] = latest_top_position.get("short_position", 0)
            
            # Taker 買賣比例
            if all_long_short.get("taker_buy_sell_ratio"):
                latest_taker = all_long_short["taker_buy_sell_ratio"][-1]
                enhanced_data["taker_buy_sell_ratio"] = latest_taker["buy_sell_ratio"]
                enhanced_data["taker_buy_volume"] = latest_taker["buy_vol"]
                enhanced_data["taker_sell_volume"] = latest_taker["sell_vol"]
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"Error fetching enhanced long-short data for {symbol}: {e}")
            return {}
    
    def fetch_enhanced_interest_data(self, symbol: str) -> Dict:
        """
        收集利率和未平倉合約量資料，兼容現有 schema
        """
        try:
            # 獲取完整利率和未平倉資料
            interest_data = self.interest_collector.fetch_all_interest_data(
                symbol, include_margin=True
            )
            
            enhanced_data = {}
            
            # 當前未平倉合約量 (更新現有的 open_interest 欄位)
            if interest_data.get("current_open_interest"):
                current_oi = interest_data["current_open_interest"]
                enhanced_data["open_interest"] = current_oi["open_interest"]
                enhanced_data["open_interest_value"] = current_oi.get("open_interest_value", 0)
                enhanced_data["open_interest_timestamp"] = current_oi.get("timestamp", 0)
            
            # 歷史未平倉合約量變化
            if interest_data.get("historical_open_interest"):
                historical_oi = interest_data["historical_open_interest"]
                if len(historical_oi) >= 2:
                    latest = historical_oi[-1]
                    previous = historical_oi[-2]
                    
                    change = latest["open_interest"] - previous["open_interest"]
                    change_percent = (change / previous["open_interest"] * 100) if previous["open_interest"] > 0 else 0
                    
                    enhanced_data["open_interest_change"] = change
                    enhanced_data["open_interest_change_percent"] = change_percent
                    enhanced_data["open_interest_trend"] = "increasing" if change > 0 else "decreasing" if change < 0 else "stable"
            
            # 下一小時利率
            if interest_data.get("next_hourly_interest_rate"):
                for rate_info in interest_data["next_hourly_interest_rate"]:
                    asset = rate_info["asset"]
                    enhanced_data[f"next_hourly_rate_{asset.lower()}"] = rate_info["next_hourly_interest_rate"]
                    enhanced_data[f"next_hourly_time_{asset.lower()}"] = rate_info["next_hourly_interest_time"]
            
            # 保證金利率
            if interest_data.get("margin_interest_rates"):
                for margin_info in interest_data["margin_interest_rates"]:
                    asset = margin_info["asset"]
                    enhanced_data[f"margin_daily_rate_{asset.lower()}"] = margin_info["daily_interest_rate"]
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"Error fetching enhanced interest data for {symbol}: {e}")
            return {}
    
    def fetch_and_store_enhanced(self, symbol: str):
        """
        收集並儲存增強版資料，保持現有 schema 結構
        """
        try:
            logger.info(f"Processing enhanced data for {symbol}")
            
            # 使用原本的方法收集基本資料
            min_candles = 2
            timeframe_minutes = int(self.timeframe[:-1])
            current_time = datetime.utcnow()
            since = int((datetime.utcnow() - timedelta(minutes=timeframe_minutes * (min_candles + 1))).timestamp() * 1000)
            minutes = (current_time.minute // 5) * 5
            timestamp = current_time.replace(minute=minutes, second=0, microsecond=0)
            
            # 轉換 symbol 格式
            futures_symbol = symbol  # 保持 CCXT 格式
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")  # 轉為 Binance 格式
            
            # 收集基本 OHLCV 資料
            ohlcv = self.fetch_ohlcv(futures_symbol, since)
            spot_cvd = self.get_spot_cvd(binance_symbol, since, period=self.timeframe)
            cvd = self.fetch_cvd(futures_symbol, since)
            
            # 收集增強版 funding rate 資料 (current + next)
            enhanced_funding = self.funding_collector.fetch_complete_funding_data(futures_symbol)
            
            # 收集增強版多空比資料
            enhanced_long_short = self.fetch_enhanced_long_short_data(futures_symbol)
            
            # 收集增強版利率資料
            enhanced_interest = self.fetch_enhanced_interest_data(futures_symbol)
            
            # 建立兼容現有 schema 的資料結構
            data = {
                "_id": f"{binance_symbol}_{int(timestamp.timestamp())}",
                "timestamp": timestamp,
                "symbol": binance_symbol,
                
                # Futures 資料 (現有結構 + 增強 funding rates)
                "futures": {
                    "open": str(ohlcv[-1]["open"]) if ohlcv else "0",
                    "high": str(ohlcv[-1]["high"]) if ohlcv else "0", 
                    "low": str(ohlcv[-1]["low"]) if ohlcv else "0",
                    "close": str(ohlcv[-1]["close"]) if ohlcv else "0",
                    "volume": str(ohlcv[-1]["volume"]) if ohlcv else "0",
                    "quote_volume": str(ohlcv[-1]["volume"] * ohlcv[-1]["close"]) if ohlcv else "0",
                    "trade_num": 0,  # 需要額外 API 獲取
                    "taker_buy_base": "0",  # 需要額外 API 獲取
                    "taker_buy_quote": "0",  # 需要額外 API 獲取
                    "cvd": cvd[-1]["cvd"] if cvd else 0,
                    "calculated_volume": ohlcv[-1]["volume"] if ohlcv else 0,
                    
                    # Enhanced funding rate data (current + next)
                    "funding_rate": enhanced_funding.get("current_funding_rate", 0),
                    "next_funding_rate": enhanced_funding.get("next_funding_rate", 0),
                    "next_funding_time": enhanced_funding.get("next_funding_time", 0),
                    "mark_price": enhanced_funding.get("mark_price", 0),
                    "index_price": enhanced_funding.get("index_price", 0),
                    "estimated_settle_price": enhanced_funding.get("estimated_settle_price", 0)
                },
                
                # Long-Short Ratio 資料 (擴展現有結構)
                "long_short_ratio": {
                    # 現有欄位
                    "open_interest": enhanced_interest.get("open_interest", 0),
                    "premium_index": 0,  # 需要額外計算
                    
                    # 新增的增強多空比資料
                    **enhanced_long_short,
                    
                    # 新增的未平倉合約量資料
                    "open_interest_value": enhanced_interest.get("open_interest_value", 0),
                    "open_interest_change": enhanced_interest.get("open_interest_change", 0),
                    "open_interest_change_percent": enhanced_interest.get("open_interest_change_percent", 0),
                    "open_interest_trend": enhanced_interest.get("open_interest_trend", "stable")
                },
                
                # Spot 資料 (現有結構)
                "spot": {
                    "open": str(spot_cvd[-1]["spot_cvd"]) if spot_cvd else "0",
                    "high": "0",
                    "low": "0", 
                    "close": str(spot_cvd[-1]["spot_cvd"]) if spot_cvd else "0",
                    "volume": str(spot_cvd[-1]["spot_volume"]) if spot_cvd else "0",
                    "quote_volume": "0",
                    "trade_num": 0,
                    "taker_buy_base": "0",
                    "taker_buy_quote": "0",
                    "cvd": spot_cvd[-1]["spot_cvd"] if spot_cvd else 0,
                    "calculated_volume": spot_cvd[-1]["spot_volume"] if spot_cvd else 0,
                    "market_cap": 0  # 需要額外 API 獲取
                },
                
                # Spot Margin Fee 資料 (擴展)
                "spot_margin_fee": {
                    "dailyInterestRate": enhanced_interest.get("margin_daily_rate_usdt", 0.00000637),
                    **{k: v for k, v in enhanced_interest.items() if k.startswith("margin_daily_rate_")},
                    **{k: v for k, v in enhanced_interest.items() if k.startswith("next_hourly_rate_")}
                },
                
                # Liquidations 資料 (現有結構)
                "liquidations": {
                    "buy_liquidations": {
                        "total_quantity": 0,
                        "total_dollars": 0,
                        "event_count": 0
                    },
                    "sell_liquidations": {
                        "total_quantity": 0,
                        "total_dollars": 0,
                        "event_count": 0
                    }
                },
                
                # 新增: 收集器資訊
                "collector_info": {
                    "slave_id": self.slave_id,
                    "collection_timestamp": datetime.utcnow().isoformat(),
                    "data_version": "enhanced_v2",
                    "apis_called": ["ohlcv", "funding_rate", "premium_index", "long_short_ratios", "open_interest"]
                }
            }
            
            # 儲存到 MongoDB - 使用每個符號的專屬集合
            collection_name = f"{symbol}_{self.timeframe}"
            print(f"🔥 DEBUG: Storing {symbol} in collection {collection_name} with timeframe {self.timeframe}")
            logger.info(f"🔥 DEBUG: About to store in collection {collection_name}")
            collection = self.db[collection_name]
            collection.insert_one(data)
            
            logger.info(f"Successfully stored enhanced data for {symbol} in {collection_name}")
            return data
            
        except Exception as e:
            logger.error(f"Error in fetch_and_store_enhanced for {symbol}: {e}")
            raise

def test_schema_compatibility():
    """
    測試 schema 兼容性
    """
    print("🧪 測試 Schema 兼容性...")
    
    collector = SchemaCompatibleCollector(
        slave_id="test-slave",
        exchange_name="binance",
        timeframe="1m"  # Changed to 1m for precision
    )
    
    test_symbol = "BTC/USDT:USDT"
    
    try:
        # 測試資料收集
        result = collector.fetch_and_store_enhanced(test_symbol)
        
        print("✅ Schema 兼容性測試成功!")
        print(f"📊 收集的資料結構:")
        print(f"  - Symbol: {result['symbol']}")
        print(f"  - Futures OHLCV: {result['futures']['close']}")
        print(f"  - Open Interest: {result['long_short_ratio']['open_interest']}")
        print(f"  - Global L/S Ratio: {result['long_short_ratio'].get('global_long_short_ratio', 'N/A')}")
        print(f"  - OI Change: {result['long_short_ratio'].get('open_interest_change_percent', 'N/A')}%")
        print(f"  - Collector: {result['collector_info']['slave_id']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Schema 兼容性測試失敗: {e}")
        return False

if __name__ == "__main__":
    test_schema_compatibility()