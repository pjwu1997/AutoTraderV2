#!/usr/bin/env python3
"""
Symbol 分配器 - 重用現有的 SymbolManager 功能
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))

from DataFetcher.symbol_manager import SymbolManager
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class DistributedSymbolManager:
    def __init__(self, num_slaves: int = 5):
        self.num_slaves = num_slaves
        self.symbol_manager = SymbolManager()
        
    def generate_distribution(self, max_symbols_per_slave: int = 50) -> Dict[str, List[str]]:
        """
        生成 symbol 分配策略
        每個 slave 最多處理 max_symbols_per_slave 個 symbols
        """
        logger.info(f"Generating symbol distribution for {self.num_slaves} slaves")
        
        # 獲取所有 perpetual contracts
        all_symbols = self.symbol_manager.fetch_all_perpetual_pairs()
        
        # 按交易量排序，取前 num_slaves * max_symbols_per_slave 個
        total_symbols_needed = self.num_slaves * max_symbols_per_slave
        top_symbols = self.symbol_manager.get_top_symbols_by_volume(limit=total_symbols_needed)
        
        logger.info(f"Selected {len(top_symbols)} symbols for distribution")
        
        # 分配給各個 slaves
        distribution_list = self.symbol_manager.distribute_symbols_across_ips(
            top_symbols, self.num_slaves
        )
        
        # 轉換為以 slave_id 為 key 的格式
        distribution = {}
        for i, symbols in distribution_list.items():
            slave_id = f"slave-{i+1}"
            distribution[slave_id] = symbols
            logger.info(f"{slave_id}: {len(symbols)} symbols")
        
        return distribution
    
    def save_distribution(self, distribution: Dict[str, List[str]], output_dir: str = "Config/slaves"):
        """
        保存分配結果到配置檔案
        """
        os.makedirs(output_dir, exist_ok=True)
        
        for slave_id, symbols in distribution.items():
            # 生成環境變數檔案
            env_file = os.path.join(output_dir, f"{slave_id}.env")
            with open(env_file, 'w') as f:
                f.write(f"# Configuration for {slave_id}\n")
                f.write(f"SLAVE_ID={slave_id}\n")
                f.write(f"SYMBOLS={','.join(symbols)}\n")
                f.write(f"SYMBOL_COUNT={len(symbols)}\n")
                f.write(f"MONGO_URI=mongodb://shared-mongo:27017/\n")
                f.write(f"MONGO_DB_NAME=trading_data\n")
                f.write(f"TIMEFRAME=5m\n")
                f.write(f"FETCH_INTERVAL=60\n")
                f.write(f"MASTER_URL=http://master-vm:8080\n")
                
            logger.info(f"Saved configuration for {slave_id} to {env_file}")
        
        logger.info(f"Symbol distribution saved to {output_dir}/")
        return True

if __name__ == "__main__":
    # 測試分配功能
    distributor = DistributedSymbolManager(num_slaves=5)
    distribution = distributor.generate_distribution(max_symbols_per_slave=50)
    
    print("\n=== Symbol Distribution ===")
    for slave_id, symbols in distribution.items():
        print(f"{slave_id}: {len(symbols)} symbols")
        print(f"  Sample: {symbols[:3]}...")
    
    distributor.save_distribution(distribution)