#!/usr/bin/env python3
"""
測試利率和未平倉合約量收集功能
"""

import sys
import os
sys.path.append('SlaveVM/data_fetcher')

from enhanced_interest_collector import EnhancedInterestCollector
import time
import json
from datetime import datetime

def test_comprehensive_interest_collection():
    """測試完整的利率和未平倉合約量收集功能"""
    print("🚀 AutoTrader 利率和未平倉合約量收集測試")
    print("=" * 70)
    
    collector = EnhancedInterestCollector()
    test_symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]
    
    all_results = {}
    
    for symbol in test_symbols:
        print(f"\n🧪 測試 {symbol}...")
        
        results = {}
        
        # 1. 測試當前未平倉合約量
        try:
            current_oi = collector.fetch_current_open_interest(symbol)
            if current_oi:
                results["current_open_interest"] = {
                    "value": current_oi["open_interest"],
                    "value_usd": current_oi.get("open_interest_value", 0),
                    "status": "✅ 成功"
                }
                print(f"  ✅ 當前未平倉合約量: {current_oi['open_interest']:,.0f}")
                print(f"     價值: ${current_oi.get('open_interest_value', 0):,.0f}")
            else:
                results["current_open_interest"] = {"status": "❌ 無資料"}
                print(f"    ❌ 無法獲取當前未平倉合約量")
        except Exception as e:
            results["current_open_interest"] = {"status": f"❌ 錯誤: {str(e)}"}
            print(f"    ❌ 錯誤: {e}")
        
        # 2. 測試歷史未平倉合約量
        try:
            historical_oi = collector.fetch_historical_open_interest(symbol, period="1h", limit=5)
            if historical_oi:
                latest = historical_oi[-1]
                results["historical_open_interest"] = {
                    "records_count": len(historical_oi),
                    "latest_value": latest["open_interest"],
                    "latest_timestamp": datetime.fromtimestamp(latest["timestamp"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "✅ 成功"
                }
                print(f"  ✅ 歷史未平倉合約量: {len(historical_oi)} 筆記錄")
                print(f"     最新值: {latest['open_interest']:,.0f}")
            else:
                results["historical_open_interest"] = {"status": "❌ 無資料"}
                print(f"    ❌ 無法獲取歷史未平倉合約量")
        except Exception as e:
            results["historical_open_interest"] = {"status": f"❌ 錯誤: {str(e)}"}
            print(f"    ❌ 錯誤: {e}")
        
        # 3. 測試未平倉合約量變化
        try:
            change_data = collector.calculate_open_interest_change(symbol, hours=24)
            if "error" not in change_data:
                results["oi_change_24h"] = {
                    "change_percentage": change_data["change_percentage"],
                    "trend": change_data["trend"],
                    "current": change_data["current_open_interest"],
                    "previous": change_data["previous_open_interest"],
                    "status": "✅ 成功"
                }
                print(f"  📈 24小時變化: {change_data['change_percentage']:.2f}% ({change_data['trend']})")
            else:
                results["oi_change_24h"] = {"status": "❌ 資料不足"}
                print(f"    ❌ 無法計算24小時變化")
        except Exception as e:
            results["oi_change_24h"] = {"status": f"❌ 錯誤: {str(e)}"}
            print(f"    ❌ 錯誤: {e}")
        
        all_results[symbol] = results
        time.sleep(1)  # 避免 API 限制
    
    # 4. 測試利率資料 (全局)
    print(f"\n💰 測試利率資料...")
    try:
        next_rates = collector.fetch_next_hourly_interest_rate(["BTC", "ETH", "USDT"])
        if next_rates:
            print(f"  ✅ 下一小時利率: {len(next_rates)} 筆記錄")
            for rate in next_rates:
                next_time = datetime.fromtimestamp(rate["next_hourly_interest_time"] / 1000)
                print(f"     {rate['asset']}: {rate['next_hourly_interest_rate']:.6f}% (下次: {next_time.strftime('%H:%M')})")
        else:
            print(f"    ❌ 無法獲取利率資料")
    except Exception as e:
        print(f"    ⚠️  利率資料錯誤: {e}")
    
    # 5. 測試完整資料收集
    print(f"\n🔍 測試完整資料收集...")
    try:
        full_data = collector.fetch_all_interest_data("BTC/USDT:USDT", include_margin=True)
        print(f"  ✅ 完整資料收集成功")
        print(f"     當前OI: {'有' if full_data.get('current_open_interest') else '無'}")
        print(f"     歷史OI: {len(full_data.get('historical_open_interest', []))} 筆")
        print(f"     利率資料: {len(full_data.get('next_hourly_interest_rate', []))} 筆")
        print(f"     保證金利率: {len(full_data.get('margin_interest_rates', []))} 筆")
    except Exception as e:
        print(f"    ❌ 完整資料收集錯誤: {e}")
    
    # 生成測試報告
    print("\n" + "=" * 70)
    print("📋 測試報告摘要")
    print("=" * 70)
    
    successful_symbols = 0
    total_apis_working = 0
    total_apis_tested = 0
    
    for symbol, results in all_results.items():
        if results:
            successful_symbols += 1
            symbol_apis_working = sum(1 for r in results.values() if isinstance(r, dict) and "✅" in r.get("status", ""))
            total_apis_working += symbol_apis_working
            total_apis_tested += len(results)
            
            print(f"✅ {symbol}: {symbol_apis_working}/{len(results)} APIs 運作正常")
            
            # 顯示關鍵數據
            if results.get("current_open_interest", {}).get("value"):
                oi_value = results["current_open_interest"]["value"]
                print(f"   📊 未平倉合約量: {oi_value:,.0f}")
            
            if results.get("oi_change_24h", {}).get("change_percentage") is not None:
                change = results["oi_change_24h"]["change_percentage"]
                trend = results["oi_change_24h"]["trend"]
                print(f"   📈 24h變化: {change:.2f}% ({trend})")
    
    print(f"\n🎯 總計:")
    print(f"  - 成功測試的 Symbols: {successful_symbols}/{len(test_symbols)}")
    print(f"  - 運作正常的 APIs: {total_apis_working}/{total_apis_tested}")
    if total_apis_tested > 0:
        print(f"  - 整體成功率: {(total_apis_working/total_apis_tested*100):.1f}%")
    
    # 保存詳細結果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"interest_test_report_{timestamp}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "test_timestamp": datetime.now().isoformat(),
            "test_type": "interest_and_open_interest",
            "summary": {
                "successful_symbols": successful_symbols,
                "total_symbols": len(test_symbols),
                "working_apis": total_apis_working,
                "total_apis": total_apis_tested,
                "success_rate": total_apis_working/total_apis_tested*100 if total_apis_tested > 0 else 0
            },
            "detailed_results": all_results
        }, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 詳細報告已保存: {report_file}")
    
    return all_results

def test_open_interest_summary():
    """測試多個 symbols 的未平倉合約量摘要"""
    print("\n📊 測試未平倉合約量摘要功能...")
    
    collector = EnhancedInterestCollector()
    symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]
    
    try:
        summary = collector.get_open_interest_summary(symbols)
        
        print(f"✅ 摘要生成成功:")
        print(f"  - 總 Symbols: {summary['total_symbols']}")
        print(f"  - 總未平倉價值: ${summary['total_open_interest_value']:,.0f}")
        print(f"  - 前3名 (按價值):")
        
        for i, data in enumerate(summary['top_symbols_by_oi'][:3]):
            print(f"    {i+1}. {data['symbol']}: ${data['open_interest_value']:,.0f}")
            
    except Exception as e:
        print(f"❌ 摘要生成失敗: {e}")

if __name__ == "__main__":
    try:
        # 執行完整測試
        test_comprehensive_interest_collection()
        
        # 執行摘要測試
        test_open_interest_summary()
        
        print("\n🎉 利率和未平倉合約量測試完成！")
        
    except KeyboardInterrupt:
        print("\n⏹️  測試被中斷")
    except Exception as e:
        print(f"\n❌ 測試出現意外錯誤: {e}")
        import traceback
        traceback.print_exc()