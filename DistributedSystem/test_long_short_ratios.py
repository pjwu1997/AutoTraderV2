#!/usr/bin/env python3
"""
測試增強版多空比收集功能
"""

import sys
import os
sys.path.append('SlaveVM/data_fetcher')

from enhanced_long_short_collector import EnhancedLongShortCollector
import time
import json
from datetime import datetime

def test_single_symbol(symbol: str):
    """測試單一 symbol 的多空比收集"""
    print(f"\n🧪 測試 {symbol} 的多空比收集...")
    
    collector = EnhancedLongShortCollector()
    
    # 測試各種多空比 API
    tests = [
        ("全域帳戶多空比", collector.fetch_global_long_short_ratio),
        ("頂級交易者帳戶多空比", collector.fetch_top_trader_account_ratio),
        ("頂級交易者倉位多空比", collector.fetch_top_trader_position_ratio)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            print(f"  📊 測試 {test_name}...")
            data = test_func(symbol, period="5m", limit=5)
            
            if data:
                latest = data[-1]
                results[test_name] = {
                    "records_count": len(data),
                    "latest_ratio": latest.get("long_short_ratio", "N/A"),
                    "latest_timestamp": datetime.fromtimestamp(latest.get("timestamp", 0) / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "✅ 成功"
                }
                print(f"    ✅ 成功: {len(data)} 筆記錄，最新比例: {latest.get('long_short_ratio', 'N/A')}")
            else:
                results[test_name] = {"status": "❌ 無資料"}
                print(f"    ❌ 無法獲取資料")
                
        except Exception as e:
            results[test_name] = {"status": f"❌ 錯誤: {str(e)}"}
            print(f"    ❌ 錯誤: {e}")
        
        time.sleep(0.5)  # 避免 API 限制
    
    return results

def test_comprehensive_collection():
    """測試完整的多空比收集功能"""
    print("🚀 AutoTrader 增強版多空比收集測試")
    print("=" * 60)
    
    # 測試的 symbols
    test_symbols = [
        "BTC/USDT:USDT",  # 比特幣
        "ETH/USDT:USDT",  # 以太坊  
        "BNB/USDT:USDT",  # 幣安幣
        "ADA/USDT:USDT"   # 卡爾達諾
    ]
    
    all_results = {}
    
    for symbol in test_symbols:
        try:
            results = test_single_symbol(symbol)
            all_results[symbol] = results
            
            # 顯示摘要
            print(f"\n📈 {symbol} 摘要:")
            for test_name, result in results.items():
                if "latest_ratio" in result:
                    print(f"  - {test_name}: {result['latest_ratio']}")
            
        except Exception as e:
            print(f"❌ {symbol} 測試失敗: {e}")
            all_results[symbol] = {"error": str(e)}
    
    # 生成測試報告
    print("\n" + "=" * 60)
    print("📋 測試報告摘要")
    print("=" * 60)
    
    successful_symbols = 0
    total_apis_working = 0
    total_apis_tested = 0
    
    for symbol, results in all_results.items():
        if "error" not in results:
            successful_symbols += 1
            symbol_apis_working = sum(1 for r in results.values() if "✅" in r.get("status", ""))
            total_apis_working += symbol_apis_working
            total_apis_tested += len(results)
            
            print(f"✅ {symbol}: {symbol_apis_working}/{len(results)} APIs 運作正常")
        else:
            print(f"❌ {symbol}: 測試失敗")
    
    print(f"\n🎯 總計:")
    print(f"  - 成功測試的 Symbols: {successful_symbols}/{len(test_symbols)}")
    print(f"  - 運作正常的 APIs: {total_apis_working}/{total_apis_tested}")
    print(f"  - 整體成功率: {(total_apis_working/total_apis_tested*100):.1f}%")
    
    # 保存詳細結果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"long_short_test_report_{timestamp}.json"
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "test_timestamp": datetime.now().isoformat(),
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

def test_data_structure():
    """測試資料結構和格式"""
    print("\n🔍 測試資料結構...")
    
    collector = EnhancedLongShortCollector()
    symbol = "BTC/USDT:USDT"
    
    # 測試完整資料收集
    all_data = collector.fetch_all_long_short_data(symbol, limit=3)
    
    print("📊 完整資料結構:")
    print(json.dumps(all_data, indent=2, default=str, ensure_ascii=False))
    
    # 測試摘要功能
    summary = collector.get_latest_ratio_summary(symbol)
    print("\n📈 最新摘要:")
    print(json.dumps(summary, indent=2, default=str, ensure_ascii=False))

if __name__ == "__main__":
    try:
        # 執行完整測試
        test_comprehensive_collection()
        
        # 測試資料結構  
        test_data_structure()
        
        print("\n🎉 多空比收集測試完成！")
        
    except KeyboardInterrupt:
        print("\n⏹️  測試被中斷")
    except Exception as e:
        print(f"\n❌ 測試出現意外錯誤: {e}")
        import traceback
        traceback.print_exc()