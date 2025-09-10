#!/usr/bin/env python3
"""
系統狀態監控腳本 - 檢查整個分散式系統的運行狀況
"""

import requests
import json
import sys
from datetime import datetime
from typing import Dict, List

class SystemMonitor:
    def __init__(self, master_url: str):
        self.master_url = master_url
        
    def check_master_status(self) -> Dict:
        """檢查 Master 狀態"""
        try:
            response = requests.get(f"{self.master_url}/api/status", timeout=10)
            if response.status_code == 200:
                return {"status": "online", "data": response.json()}
            else:
                return {"status": "error", "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "offline", "error": str(e)}
    
    def check_slaves_status(self) -> Dict:
        """檢查所有 Slave 狀態"""
        try:
            response = requests.get(f"{self.master_url}/api/slaves", timeout=10)
            if response.status_code == 200:
                return {"status": "online", "data": response.json()}
            else:
                return {"status": "error", "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"status": "offline", "error": str(e)}
    
    def generate_report(self) -> Dict:
        """生成系統狀態報告"""
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "master": self.check_master_status(),
            "slaves": self.check_slaves_status()
        }
        
        # 計算摘要統計
        if report["master"]["status"] == "online":
            master_data = report["master"]["data"]
            report["summary"] = {
                "total_slaves": master_data.get("total_slaves", 0),
                "online_slaves": master_data.get("online_slaves", 0),
                "total_symbols": master_data.get("total_symbols", 0),
                "avg_cpu": master_data.get("avg_cpu", 0),
                "avg_memory": master_data.get("avg_memory", 0),
                "total_errors": master_data.get("total_errors", 0)
            }
        
        return report
    
    def print_report(self, report: Dict):
        """列印格式化的報告"""
        print("=" * 60)
        print(f"🖥️  AutoTrader 分散式系統狀態報告")
        print(f"⏰ 時間: {report['timestamp']}")
        print("=" * 60)
        
        # Master 狀態
        master = report["master"]
        if master["status"] == "online":
            print("✅ Master VM: 線上")
            data = master["data"]
            print(f"   - 線上 Slaves: {data.get('online_slaves', 0)}/{data.get('total_slaves', 0)}")
            print(f"   - 總 Symbols: {data.get('total_symbols', 0)}")
            print(f"   - 平均 CPU: {data.get('avg_cpu', 0):.1f}%")
            print(f"   - 平均記憶體: {data.get('avg_memory', 0):.1f}%")
        else:
            print(f"❌ Master VM: {master['status']} - {master.get('error', 'Unknown')}")
        
        print()
        
        # Slaves 狀態  
        slaves = report["slaves"]
        if slaves["status"] == "online":
            print("📡 Slave VMs 狀態:")
            slaves_data = slaves["data"]
            
            for slave_id, slave_info in slaves_data.items():
                status_icon = "✅" if slave_info["status"] == "online" else "❌"
                print(f"   {status_icon} {slave_id}")
                print(f"      - IP: {slave_info['ip_address']}")
                print(f"      - Symbols: {len(slave_info['assigned_symbols'])}")
                print(f"      - 已處理: {slave_info['symbols_processed']}")
                print(f"      - 錯誤數: {slave_info['error_count']}")
                print(f"      - CPU: {slave_info['cpu_usage']:.1f}%")
                print(f"      - 記憶體: {slave_info['memory_usage']:.1f}%")
                print(f"      - 最後心跳: {slave_info['last_heartbeat']}")
                print()
        else:
            print(f"❌ 無法獲取 Slaves 狀態: {slaves.get('error', 'Unknown')}")
        
        print("=" * 60)

def main():
    if len(sys.argv) < 2:
        print("用法: python system_status.py <master_url>")
        print("範例: python system_status.py http://master-vm:8080")
        sys.exit(1)
    
    master_url = sys.argv[1].rstrip('/')
    
    monitor = SystemMonitor(master_url)
    report = monitor.generate_report()
    
    # 列印報告
    monitor.print_report(report)
    
    # 可選：輸出 JSON 格式
    if len(sys.argv) > 2 and sys.argv[2] == "--json":
        print("\n" + "=" * 60)
        print("📄 JSON 格式報告:")
        print(json.dumps(report, indent=2, ensure_ascii=False))
    
    # 退出碼 (可用於腳本自動化)
    if report["master"]["status"] != "online":
        sys.exit(1)
    
    if report["slaves"]["status"] == "online":
        slaves_data = report["slaves"]["data"]
        online_count = sum(1 for s in slaves_data.values() if s["status"] == "online")
        if online_count == 0:
            sys.exit(2)  # 沒有線上的 slaves

if __name__ == "__main__":
    main()