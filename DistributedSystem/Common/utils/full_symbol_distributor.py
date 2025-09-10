#!/usr/bin/env python3
"""
全量 Symbol 分配器 - 收集所有 Binance perpetual contracts
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from DataFetcher.symbol_manager import SymbolManager
from typing import Dict, List
import logging
import json

logger = logging.getLogger(__name__)

class FullSymbolDistributor:
    def __init__(self, num_slaves: int = 5):
        self.num_slaves = num_slaves
        self.symbol_manager = SymbolManager()
        
    def generate_full_distribution(self) -> Dict[str, List[str]]:
        """
        收集所有 perpetual contracts 並平均分配
        """
        logger.info(f"Generating FULL symbol distribution for {self.num_slaves} slaves")
        
        # 獲取所有 perpetual contracts (不限制數量)
        all_symbols = self.symbol_manager.fetch_all_perpetual_pairs()
        logger.info(f"Found {len(all_symbols)} total perpetual contracts")
        
        # 按交易量排序 (但不限制數量，全部收集)
        self.symbol_manager.enrich_with_volume_data()
        sorted_symbols = sorted(
            all_symbols,
            key=lambda x: self.symbol_manager.symbol_info[x].volume_24h,
            reverse=True
        )
        
        logger.info(f"Processing ALL {len(sorted_symbols)} symbols")
        
        # 平均分配給所有 slaves
        distribution_list = self.symbol_manager.distribute_symbols_across_ips(
            sorted_symbols, self.num_slaves
        )
        
        # 轉換為以 slave_id 為 key 的格式
        distribution = {}
        total_symbols = 0
        
        for i, symbols in distribution_list.items():
            slave_id = f"slave-{i+1}"
            distribution[slave_id] = symbols
            total_symbols += len(symbols)
            logger.info(f"{slave_id}: {len(symbols)} symbols (avg volume: {self._calc_avg_volume(symbols):.0f})")
        
        logger.info(f"Total symbols distributed: {total_symbols}")
        return distribution
    
    def _calc_avg_volume(self, symbols: List[str]) -> float:
        """計算平均交易量"""
        if not symbols:
            return 0
        total_volume = sum(
            self.symbol_manager.symbol_info[symbol].volume_24h 
            for symbol in symbols 
            if symbol in self.symbol_manager.symbol_info
        )
        return total_volume / len(symbols)
    
    def save_full_distribution(self, distribution: Dict[str, List[str]], output_dir: str = "Config/slaves"):
        """
        保存全量分配結果，包含 MongoDB 在 Master 的配置
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # 獲取 Master VM 的 IP (從環境變數或配置)
        master_ip = os.getenv("MASTER_VM_IP", "master-vm")
        
        for slave_id, symbols in distribution.items():
            # 生成環境變數檔案
            env_file = os.path.join(output_dir, f"{slave_id}.env")
            with open(env_file, 'w') as f:
                f.write(f"# Full Collection Configuration for {slave_id}\n")
                f.write(f"# Collecting {len(symbols)} symbols\n")
                f.write(f"SLAVE_ID={slave_id}\n")
                f.write(f"SYMBOLS={','.join(symbols)}\n")
                f.write(f"SYMBOL_COUNT={len(symbols)}\n")
                f.write(f"\n# Master VM Connection (MongoDB on Master)\n")
                f.write(f"MASTER_VM_IP={master_ip}\n")
                f.write(f"MASTER_URL=http://{master_ip}:8080\n")
                f.write(f"MONGO_URI=mongodb://{master_ip}:27017/\n")
                f.write(f"MONGO_DB_NAME=trading_data\n")
                f.write(f"MONGO_AUTH_SOURCE=admin\n")
                f.write(f"MONGO_USERNAME=trader_user\n")
                f.write(f"MONGO_PASSWORD=secure_password\n")
                f.write(f"\n# Collection Settings\n")
                f.write(f"TIMEFRAME=5m\n")
                f.write(f"FETCH_INTERVAL=60\n")
                f.write(f"BATCH_SIZE=15\n")  # 較大批次處理更多 symbols
                f.write(f"RATE_LIMIT_DELAY=0.1\n")  # 調整速率限制
                f.write(f"MAX_RETRIES=3\n")
                f.write(f"\n# Health Checker\n")
                f.write(f"HEALTH_PORT=8081\n")
                f.write(f"HEARTBEAT_INTERVAL=30\n")
                
            # 生成 JSON 配置檔案 (詳細資訊)
            json_file = os.path.join(output_dir, f"{slave_id}_config.json")
            config_data = {
                "slave_id": slave_id,
                "symbols": symbols,
                "symbol_count": len(symbols),
                "master_config": {
                    "ip": master_ip,
                    "api_url": f"http://{master_ip}:8080",
                    "mongo_uri": f"mongodb://{master_ip}:27017/"
                },
                "collection_settings": {
                    "timeframe": "5m",
                    "fetch_interval": 60,
                    "batch_size": 15,
                    "rate_limit_delay": 0.1
                },
                "symbol_details": {
                    symbol: {
                        "volume_24h": self.symbol_manager.symbol_info[symbol].volume_24h,
                        "base_asset": self.symbol_manager.symbol_info[symbol].base_asset,
                        "quote_asset": self.symbol_manager.symbol_info[symbol].quote_asset
                    }
                    for symbol in symbols[:10]  # 只保存前10個的詳細資訊
                },
                "generated_at": self.symbol_manager.symbol_info[symbols[0]].status if symbols else "unknown"
            }
            
            with open(json_file, 'w') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Saved full configuration for {slave_id}: {len(symbols)} symbols")
        
        # 生成總覽檔案
        self._save_distribution_summary(distribution, output_dir)
        
        logger.info(f"Full symbol distribution saved to {output_dir}/")
        return True
    
    def _save_distribution_summary(self, distribution: Dict[str, List[str]], output_dir: str):
        """生成分配總覽"""
        summary = {
            "distribution_type": "FULL_COLLECTION",
            "total_slaves": len(distribution),
            "total_symbols": sum(len(symbols) for symbols in distribution.values()),
            "distribution_summary": {},
            "top_symbols_by_volume": [],
            "generated_at": "2024-01-01T00:00:00Z"
        }
        
        # 各 slave 統計
        for slave_id, symbols in distribution.items():
            avg_volume = self._calc_avg_volume(symbols)
            summary["distribution_summary"][slave_id] = {
                "symbol_count": len(symbols),
                "avg_volume_24h": avg_volume,
                "sample_symbols": symbols[:5]
            }
        
        # 前20個最高交易量的 symbols
        all_symbols_with_volume = [
            (symbol, self.symbol_manager.symbol_info[symbol].volume_24h)
            for symbols in distribution.values()
            for symbol in symbols
            if symbol in self.symbol_manager.symbol_info
        ]
        all_symbols_with_volume.sort(key=lambda x: x[1], reverse=True)
        summary["top_symbols_by_volume"] = all_symbols_with_volume[:20]
        
        # 保存總覽
        summary_file = os.path.join(output_dir, "full_distribution_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        # 生成部署指令檔案
        deploy_commands_file = os.path.join(output_dir, "deploy_commands.sh")
        with open(deploy_commands_file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# 全量收集部署指令\n\n")
            f.write("echo '=== AutoTrader 全量收集部署 ==='\n")
            f.write(f"echo 'Total Symbols: {summary['total_symbols']}'\n")
            f.write(f"echo 'Slaves: {summary['total_slaves']}'\n\n")
            
            for slave_id in distribution.keys():
                f.write(f"echo 'Deploying {slave_id}...'\n")
                f.write(f"# scp {slave_id}.env slave-vm-{slave_id[-1]}:/path/to/config/\n")
                f.write(f"# ssh slave-vm-{slave_id[-1]} './deploy_slave.sh {slave_id}'\n\n")
            
            f.write("echo 'All slaves deployed!'\n")
        
        os.chmod(deploy_commands_file, 0o755)

def main():
    """
    主程序 - 生成全量收集配置
    """
    print("🚀 AutoTrader 全量 Symbol 分配器")
    print("=" * 50)
    
    # 可以從環境變數調整 slave 數量
    num_slaves = int(os.getenv("NUM_SLAVES", "5"))
    master_ip = os.getenv("MASTER_VM_IP", "master-vm")
    
    print(f"Slaves 數量: {num_slaves}")
    print(f"Master IP: {master_ip}")
    print()
    
    distributor = FullSymbolDistributor(num_slaves=num_slaves)
    distribution = distributor.generate_full_distribution()
    
    print("\n📊 分配結果:")
    print("=" * 50)
    total_symbols = 0
    for slave_id, symbols in distribution.items():
        print(f"{slave_id:>8}: {len(symbols):>3} symbols")
        total_symbols += len(symbols)
        # 顯示一些高交易量的 symbols
        top_symbols = symbols[:3]
        print(f"         Top: {', '.join(top_symbols)}")
    
    print(f"\n📈 總計: {total_symbols} symbols")
    avg_per_slave = total_symbols / num_slaves
    print(f"📊 平均每 Slave: {avg_per_slave:.1f} symbols")
    
    # 保存配置
    print(f"\n💾 保存配置到 ../../Config/slaves/")
    distributor.save_full_distribution(distribution, "../../Config/slaves")
    
    print(f"\n✅ 全量收集配置生成完成！")
    print(f"📁 配置檔案位置: ../../Config/slaves/")
    print(f"🚀 部署指令: ../../Config/slaves/deploy_commands.sh")
    
    # 估算負載
    print(f"\n⚡ 負載估算:")
    requests_per_minute = total_symbols * 5  # 每個 symbol 約 5 個 API 請求
    print(f"   - 每分鐘總請求數: ~{requests_per_minute}")
    print(f"   - 每 Slave 每分鐘: ~{requests_per_minute/num_slaves:.0f} 請求")
    print(f"   - Binance 限制: 1200 請求/分鐘/IP")
    
    if requests_per_minute/num_slaves > 1000:
        print(f"   ⚠️  接近 API 限制，建議增加 Slave 數量或調整收集頻率")
    else:
        print(f"   ✅ API 使用率安全")

if __name__ == "__main__":
    main()