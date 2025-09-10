#!/usr/bin/env python3
"""
Symbol Manager - Fetches and manages all Binance perpetual contract symbols
Supports multi-IP distribution for scaling beyond API rate limits
"""

import requests
import json
import os
import logging
from typing import List, Dict, Tuple
from datetime import datetime
import time
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SymbolInfo:
    symbol: str
    base_asset: str
    quote_asset: str
    price_precision: int
    quantity_precision: int
    status: str
    volume_24h: float = 0.0
    market_cap_rank: int = 999999

class SymbolManager:
    def __init__(self):
        self.all_symbols = []
        self.active_symbols = []
        self.symbol_info = {}
        
    def fetch_all_perpetual_pairs(self) -> List[str]:
        """
        Fetch all active Binance perpetual contract pairs
        Returns list of symbols like ['BTCUSDT', 'ETHUSDT', ...]
        """
        try:
            logger.info("Fetching all Binance perpetual contract pairs...")
            
            # Get exchange info
            url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Filter for perpetual contracts that are trading
            perpetual_symbols = []
            for symbol_data in data['symbols']:
                if (symbol_data['contractType'] == 'PERPETUAL' and 
                    symbol_data['status'] == 'TRADING'):
                    
                    symbol_info = SymbolInfo(
                        symbol=symbol_data['symbol'],
                        base_asset=symbol_data['baseAsset'],
                        quote_asset=symbol_data['quoteAsset'],
                        price_precision=symbol_data['pricePrecision'],
                        quantity_precision=symbol_data['quantityPrecision'],
                        status=symbol_data['status']
                    )
                    
                    perpetual_symbols.append(symbol_data['symbol'])
                    self.symbol_info[symbol_data['symbol']] = symbol_info
            
            self.all_symbols = perpetual_symbols
            logger.info(f"Found {len(perpetual_symbols)} active perpetual contracts")
            
            return perpetual_symbols
            
        except Exception as e:
            logger.error(f"Error fetching perpetual pairs: {e}")
            return []
    
    def enrich_with_volume_data(self) -> None:
        """
        Enrich symbols with 24h volume data for ranking
        """
        try:
            logger.info("Enriching symbols with volume data...")
            
            url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            volume_map = {item['symbol']: float(item['quoteVolume']) for item in data}
            
            for symbol in self.all_symbols:
                if symbol in self.symbol_info and symbol in volume_map:
                    self.symbol_info[symbol].volume_24h = volume_map[symbol]
                    
            logger.info("Volume data enrichment completed")
            
        except Exception as e:
            logger.error(f"Error enriching volume data: {e}")
    
    def get_top_symbols_by_volume(self, limit: int = 200) -> List[str]:
        """
        Get top N symbols by 24h volume
        """
        self.enrich_with_volume_data()
        
        # Sort by volume descending
        sorted_symbols = sorted(
            self.all_symbols,
            key=lambda x: self.symbol_info[x].volume_24h,
            reverse=True
        )
        
        top_symbols = sorted_symbols[:limit]
        logger.info(f"Selected top {len(top_symbols)} symbols by volume")
        
        return top_symbols
    
    def distribute_symbols_across_ips(self, symbols: List[str], num_ips: int) -> Dict[int, List[str]]:
        """
        Distribute symbols evenly across multiple IP addresses
        Each IP should handle roughly equal number of symbols
        """
        if num_ips <= 1:
            return {0: symbols}
        
        # Calculate symbols per IP
        symbols_per_ip = len(symbols) // num_ips
        remainder = len(symbols) % num_ips
        
        distribution = {}
        start_idx = 0
        
        for ip_idx in range(num_ips):
            # Add extra symbol to first 'remainder' IPs
            current_count = symbols_per_ip + (1 if ip_idx < remainder else 0)
            end_idx = start_idx + current_count
            
            distribution[ip_idx] = symbols[start_idx:end_idx]
            start_idx = end_idx
            
            logger.info(f"IP {ip_idx}: {len(distribution[ip_idx])} symbols")
        
        return distribution
    
    def calculate_required_ips(self, symbols: List[str], max_symbols_per_ip: int = 250) -> int:
        """
        Calculate minimum number of IPs needed based on API rate limits
        Default: 250 symbols per IP (leaves safety margin from 300 limit)
        """
        required_ips = (len(symbols) + max_symbols_per_ip - 1) // max_symbols_per_ip
        
        logger.info(f"For {len(symbols)} symbols with max {max_symbols_per_ip} per IP: need {required_ips} IPs")
        
        return required_ips
    
    def save_symbol_distribution(self, distribution: Dict[int, List[str]], output_dir: str = "."):
        """
        Save symbol distribution to configuration files for each IP
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Save master list
        master_config = {
            "total_symbols": sum(len(symbols) for symbols in distribution.values()),
            "num_ips": len(distribution),
            "generated_at": datetime.utcnow().isoformat(),
            "distribution_summary": {f"ip_{ip_idx}": len(symbols) for ip_idx, symbols in distribution.items()}
        }
        
        with open(f"{output_dir}/symbol_distribution_master.json", "w") as f:
            json.dump(master_config, f, indent=2)
        
        # Save individual IP configurations
        for ip_idx, symbols in distribution.items():
            ip_config = {
                "ip_index": ip_idx,
                "symbols": symbols,
                "symbol_count": len(symbols),
                "generated_at": datetime.utcnow().isoformat()
            }
            
            # Save as JSON
            with open(f"{output_dir}/symbols_ip_{ip_idx}.json", "w") as f:
                json.dump(ip_config, f, indent=2)
            
            # Save as .env format
            with open(f"{output_dir}/.env.ip_{ip_idx}", "w") as f:
                f.write(f"# Configuration for IP {ip_idx}\n")
                f.write(f"# {len(symbols)} symbols\n")
                f.write(f"IP_INDEX={ip_idx}\n")
                f.write(f"SYMBOLS={','.join(symbols)}\n")
                f.write(f"SYMBOL_COUNT={len(symbols)}\n")
        
        logger.info(f"Symbol distribution saved to {output_dir}/")

def main():
    """
    Main function to demonstrate usage
    """
    manager = SymbolManager()
    
    # Fetch all perpetual pairs
    all_symbols = manager.fetch_all_perpetual_pairs()
    print(f"\nTotal perpetual contracts: {len(all_symbols)}")
    
    # Get top symbols by volume for tier 1 (full data collection)
    tier1_symbols = manager.get_top_symbols_by_volume(limit=200)
    print(f"Tier 1 symbols (full data): {len(tier1_symbols)}")
    
    # Calculate required IPs for tier 1
    required_ips = manager.calculate_required_ips(tier1_symbols, max_symbols_per_ip=250)
    
    if required_ips > 1:
        print(f"\nMulti-IP setup required: {required_ips} IPs")
        
        # Distribute symbols across IPs
        distribution = manager.distribute_symbols_across_ips(tier1_symbols, required_ips)
        
        # Save configuration files
        manager.save_symbol_distribution(distribution, output_dir="./config")
        
        print("\nGenerated configuration files:")
        for ip_idx in range(required_ips):
            print(f"  - config/.env.ip_{ip_idx} ({len(distribution[ip_idx])} symbols)")
            print(f"  - config/symbols_ip_{ip_idx}.json")
    else:
        print(f"\nSingle IP sufficient for {len(tier1_symbols)} symbols")
        
        # Save single IP configuration
        distribution = {0: tier1_symbols}
        manager.save_symbol_distribution(distribution, output_dir="./config")
    
    # Display sample symbols
    print(f"\nSample symbols (top 10 by volume):")
    for i, symbol in enumerate(tier1_symbols[:10]):
        volume = manager.symbol_info[symbol].volume_24h
        print(f"  {i+1:2d}. {symbol:12s} - ${volume:,.0f}")

if __name__ == "__main__":
    main()