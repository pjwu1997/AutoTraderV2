#!/usr/bin/env python3
"""
多空比資料分析工具 - 分析 MongoDB 中的多空比資料
"""

from pymongo import MongoClient
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import sys

class LongShortAnalytics:
    def __init__(self, mongo_uri: str = "mongodb://localhost:27017/", db_name: str = "trading_data"):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db.market_data
        
    def create_long_short_indexes(self):
        """建立多空比資料的 MongoDB 索引"""
        print("🔧 建立多空比資料索引...")
        
        indexes = [
            # 基本查詢索引
            ("symbol", 1),
            ("timestamp", -1),
            ("collector_id", 1),
            
            # 組合索引
            [("symbol", 1), ("timestamp", -1)],
            [("collector_id", 1), ("timestamp", -1)],
            
            # 多空比專用索引
            ("long_short_ratios.global_account.timestamp", -1),
            ("long_short_ratios.top_trader_account.timestamp", -1),
            ("long_short_ratios.top_trader_position.timestamp", -1),
        ]
        
        for index in indexes:
            try:
                if isinstance(index, list):
                    self.collection.create_index(index)
                    print(f"✅ 建立組合索引: {index}")
                else:
                    self.collection.create_index(index)
                    print(f"✅ 建立索引: {index}")
            except Exception as e:
                print(f"⚠️  索引建立失敗 {index}: {e}")
    
    def get_latest_long_short_ratios(self, symbols: List[str] = None, limit: int = 10) -> Dict:
        """獲取最新的多空比資料"""
        print("📊 獲取最新多空比資料...")
        
        pipeline = [
            # 過濾條件
            {"$match": {
                "long_short_ratios": {"$exists": True},
                **({"symbol": {"$in": symbols}} if symbols else {})
            }},
            
            # 按 symbol 分組，取最新記錄
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$symbol",
                "latest_doc": {"$first": "$$ROOT"}
            }},
            
            # 限制結果數量
            {"$limit": limit},
            
            # 重新整理格式
            {"$replaceRoot": {"newRoot": "$latest_doc"}}
        ]
        
        results = list(self.collection.aggregate(pipeline))
        
        # 整理結果
        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_symbols": len(results),
            "ratios": {}
        }
        
        for doc in results:
            symbol = doc["symbol"]
            long_short_data = doc.get("long_short_ratios", {})
            
            summary["ratios"][symbol] = {
                "timestamp": doc.get("timestamp", "").isoformat() if hasattr(doc.get("timestamp", ""), 'isoformat') else str(doc.get("timestamp", "")),
                "collector_id": doc.get("collector_id", "unknown"),
                "global_account": self._extract_ratio(long_short_data.get("global_account", [])),
                "top_trader_account": self._extract_ratio(long_short_data.get("top_trader_account", [])),
                "top_trader_position": self._extract_ratio(long_short_data.get("top_trader_position", []))
            }
        
        return summary
    
    def _extract_ratio(self, ratio_list: List) -> Optional[float]:
        """從比例資料列表中提取最新的比例值"""
        if ratio_list and len(ratio_list) > 0:
            latest = ratio_list[-1] if isinstance(ratio_list, list) else ratio_list
            return latest.get("long_short_ratio") if isinstance(latest, dict) else None
        return None
    
    def get_historical_trends(self, symbol: str, hours: int = 24) -> Dict:
        """獲取指定 symbol 的歷史多空比趨勢"""
        print(f"📈 分析 {symbol} 過去 {hours} 小時的多空比趨勢...")
        
        since = datetime.utcnow() - timedelta(hours=hours)
        
        pipeline = [
            {"$match": {
                "symbol": symbol,
                "timestamp": {"$gte": since},
                "long_short_ratios": {"$exists": True}
            }},
            {"$sort": {"timestamp": 1}},
            {"$project": {
                "timestamp": 1,
                "global_account": "$long_short_ratios.global_account",
                "top_trader_account": "$long_short_ratios.top_trader_account", 
                "top_trader_position": "$long_short_ratios.top_trader_position"
            }}
        ]
        
        results = list(self.collection.aggregate(pipeline))
        
        # 處理時間序列資料
        trends = {
            "symbol": symbol,
            "time_range": f"{hours} hours",
            "total_records": len(results),
            "time_series": {
                "timestamps": [],
                "global_account_ratios": [],
                "top_trader_account_ratios": [],
                "top_trader_position_ratios": []
            },
            "statistics": {}
        }
        
        for doc in results:
            timestamp = doc["timestamp"]
            trends["time_series"]["timestamps"].append(
                timestamp.isoformat() if hasattr(timestamp, 'isoformat') else str(timestamp)
            )
            
            # 提取各類型比例
            global_ratio = self._extract_ratio(doc.get("global_account", []))
            top_account_ratio = self._extract_ratio(doc.get("top_trader_account", []))
            top_position_ratio = self._extract_ratio(doc.get("top_trader_position", []))
            
            trends["time_series"]["global_account_ratios"].append(global_ratio)
            trends["time_series"]["top_trader_account_ratios"].append(top_account_ratio)
            trends["time_series"]["top_trader_position_ratios"].append(top_position_ratio)
        
        # 計算統計資料
        for ratio_type in ["global_account_ratios", "top_trader_account_ratios", "top_trader_position_ratios"]:
            ratios = [r for r in trends["time_series"][ratio_type] if r is not None]
            if ratios:
                trends["statistics"][ratio_type] = {
                    "latest": ratios[-1],
                    "average": sum(ratios) / len(ratios),
                    "min": min(ratios),
                    "max": max(ratios),
                    "change_24h": ratios[-1] - ratios[0] if len(ratios) > 1 else 0
                }
        
        return trends
    
    def get_top_symbols_by_ratio_extreme(self, ratio_type: str = "global_account", limit: int = 10) -> Dict:
        """獲取多空比最極端的 symbols"""
        print(f"🔥 尋找 {ratio_type} 多空比最極端的 {limit} 個 symbols...")
        
        # 建立查詢路徑
        ratio_path = f"long_short_ratios.{ratio_type}"
        
        pipeline = [
            {"$match": {ratio_path: {"$exists": True, "$ne": []}}},
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$symbol",
                "latest_doc": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$latest_doc"}},
            {"$addFields": {
                "extracted_ratio": {"$arrayElemAt": [f"${ratio_path}.long_short_ratio", -1]}
            }},
            {"$match": {"extracted_ratio": {"$exists": True, "$ne": None}}},
            {"$sort": {"extracted_ratio": -1}},  # 降序排列
            {"$limit": limit * 2}  # 取更多資料以便分析
        ]
        
        results = list(self.collection.aggregate(pipeline))
        
        # 分為多頭主導和空頭主導
        bullish_symbols = []  # 多頭主導 (ratio > 1)
        bearish_symbols = []  # 空頭主導 (ratio < 1)
        
        for doc in results:
            ratio = doc.get("extracted_ratio", 0)
            symbol_data = {
                "symbol": doc["symbol"],
                "ratio": ratio,
                "timestamp": doc.get("timestamp", "").isoformat() if hasattr(doc.get("timestamp", ""), 'isoformat') else str(doc.get("timestamp", "")),
                "collector_id": doc.get("collector_id", "unknown")
            }
            
            if ratio > 1:
                bullish_symbols.append(symbol_data)
            else:
                bearish_symbols.append(symbol_data)
        
        return {
            "ratio_type": ratio_type,
            "analysis_time": datetime.utcnow().isoformat(),
            "most_bullish": sorted(bullish_symbols, key=lambda x: x["ratio"], reverse=True)[:limit//2],
            "most_bearish": sorted(bearish_symbols, key=lambda x: x["ratio"])[:limit//2],
            "summary": {
                "total_symbols_analyzed": len(results),
                "bullish_count": len(bullish_symbols),
                "bearish_count": len(bearish_symbols)
            }
        }
    
    def generate_market_sentiment_report(self) -> Dict:
        """生成市場情緒報告"""
        print("📊 生成市場情緒報告...")
        
        # 獲取最新資料
        latest_data = self.get_latest_long_short_ratios(limit=50)
        
        # 分析市場情緒
        sentiment_analysis = {
            "report_time": datetime.utcnow().isoformat(),
            "total_symbols": latest_data["total_symbols"],
            "sentiment_breakdown": {
                "global_account": {"bullish": 0, "bearish": 0, "neutral": 0},
                "top_trader_account": {"bullish": 0, "bearish": 0, "neutral": 0},
                "top_trader_position": {"bullish": 0, "bearish": 0, "neutral": 0}
            },
            "extreme_symbols": {},
            "market_overview": {}
        }
        
        # 分析每種比例類型的情緒分佈
        for symbol, data in latest_data["ratios"].items():
            for ratio_type in ["global_account", "top_trader_account", "top_trader_position"]:
                ratio = data.get(ratio_type)
                if ratio is not None:
                    if ratio > 1.2:  # 強烈看多
                        sentiment_analysis["sentiment_breakdown"][ratio_type]["bullish"] += 1
                    elif ratio < 0.8:  # 強烈看空
                        sentiment_analysis["sentiment_breakdown"][ratio_type]["bearish"] += 1
                    else:  # 中性
                        sentiment_analysis["sentiment_breakdown"][ratio_type]["neutral"] += 1
        
        # 獲取極端值
        for ratio_type in ["global_account", "top_trader_account", "top_trader_position"]:
            sentiment_analysis["extreme_symbols"][ratio_type] = self.get_top_symbols_by_ratio_extreme(
                ratio_type, limit=6
            )
        
        return sentiment_analysis

def main():
    if len(sys.argv) < 2:
        print("用法: python long_short_analytics.py <action> [params]")
        print("")
        print("可用動作:")
        print("  setup_indexes              - 建立索引")
        print("  latest [symbol1,symbol2]   - 獲取最新多空比")
        print("  trends <symbol> [hours]    - 獲取歷史趨勢")
        print("  extremes <ratio_type>      - 獲取極端值")
        print("  sentiment                  - 市場情緒報告")
        return
    
    action = sys.argv[1]
    mongo_uri = "mongodb://localhost:27017/"  # 可從環境變數調整
    
    analytics = LongShortAnalytics(mongo_uri)
    
    if action == "setup_indexes":
        analytics.create_long_short_indexes()
        
    elif action == "latest":
        symbols = sys.argv[2].split(",") if len(sys.argv) > 2 else None
        result = analytics.get_latest_long_short_ratios(symbols)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
    elif action == "trends":
        if len(sys.argv) < 3:
            print("錯誤: 需要指定 symbol")
            return
        symbol = sys.argv[2]
        hours = int(sys.argv[3]) if len(sys.argv) > 3 else 24
        result = analytics.get_historical_trends(symbol, hours)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
    elif action == "extremes":
        ratio_type = sys.argv[2] if len(sys.argv) > 2 else "global_account"
        result = analytics.get_top_symbols_by_ratio_extreme(ratio_type)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
    elif action == "sentiment":
        result = analytics.generate_market_sentiment_report()
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
    else:
        print(f"未知動作: {action}")

if __name__ == "__main__":
    main()