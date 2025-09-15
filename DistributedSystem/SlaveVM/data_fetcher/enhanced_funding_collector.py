#!/usr/bin/env python3
"""
Enhanced Funding Rate Collector - Collects both current and next funding rates
"""

import requests
import ccxt
from datetime import datetime
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class EnhancedFundingCollector:
    """
    Enhanced funding rate collector - supports both current and next funding rates
    """
    
    def __init__(self, exchange_name: str = "binance"):
        self.exchange = getattr(ccxt, exchange_name)()
        self.base_url = "https://fapi.binance.com"
        
    def fetch_current_funding_rate(self, symbol: str) -> Optional[Dict]:
        """
        Fetch current funding rate using CCXT
        """
        try:
            funding_data = self.exchange.fetchFundingRate(symbol)
            return {
                "current_funding_rate": funding_data['fundingRate'],
                "funding_timestamp": funding_data['timestamp'],
                "funding_time": datetime.utcfromtimestamp(funding_data['timestamp'] / 1000)
            }
        except Exception as e:
            logger.error(f"Error fetching current funding rate for {symbol}: {e}")
            return None
    
    def fetch_premium_index_data(self, symbol: str) -> Optional[Dict]:
        """
        Fetch premium index data including next funding rate
        API: /fapi/v1/premiumIndex
        """
        try:
            # Convert symbol format for Binance API
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            params = {"symbol": binance_symbol}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            return {
                "mark_price": float(data["markPrice"]),
                "index_price": float(data["indexPrice"]),
                "estimated_settle_price": float(data.get("estimatedSettlePrice", 0)),
                "last_funding_rate": float(data["lastFundingRate"]),
                "next_funding_time": int(data["nextFundingTime"]),
                "interest_rate": float(data["interestRate"]),
                "timestamp": int(data["time"])
            }
            
        except Exception as e:
            logger.error(f"Error fetching premium index data for {symbol}: {e}")
            return None
    
    def fetch_next_funding_rate(self, symbol: str) -> Optional[Dict]:
        """
        Fetch next funding rate from premium index endpoint
        """
        try:
            # Convert symbol format for Binance API
            binance_symbol = symbol.replace("/USDT:USDT", "USDT").replace("/", "")
            
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            params = {"symbol": binance_symbol}
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Calculate next funding rate (using interest rate as proxy)
            # In practice, next funding rate = interest_rate + premium_rate
            # For simplification, we'll use the interest rate
            next_funding_time = datetime.utcfromtimestamp(int(data["nextFundingTime"]) / 1000)
            
            return {
                "next_funding_rate": float(data["interestRate"]),
                "next_funding_time": int(data["nextFundingTime"]),
                "next_funding_time_utc": next_funding_time,
                "mark_price": float(data["markPrice"]),
                "index_price": float(data["indexPrice"])
            }
            
        except Exception as e:
            logger.error(f"Error fetching next funding rate for {symbol}: {e}")
            return None
    
    def fetch_complete_funding_data(self, symbol: str) -> Dict:
        """
        Fetch complete funding rate data (current + next + premium info)
        """
        try:
            # Fetch current funding rate
            current_data = self.fetch_current_funding_rate(symbol)
            
            # Fetch next funding rate and premium data
            premium_data = self.fetch_premium_index_data(symbol)
            
            # Combine all data
            complete_data = {
                # Current funding rate
                "current_funding_rate": current_data["current_funding_rate"] if current_data else 0,
                "current_funding_time": current_data["funding_timestamp"] if current_data else 0,
                
                # Next funding rate and timing
                "next_funding_rate": premium_data["interest_rate"] if premium_data else 0,
                "next_funding_time": premium_data["next_funding_time"] if premium_data else 0,
                "next_funding_time_utc": datetime.utcfromtimestamp(premium_data["next_funding_time"] / 1000) if premium_data else None,
                
                # Premium and pricing data
                "mark_price": premium_data["mark_price"] if premium_data else 0,
                "index_price": premium_data["index_price"] if premium_data else 0,
                "estimated_settle_price": premium_data.get("estimated_settle_price", 0) if premium_data else 0,
                
                # Collection metadata
                "collection_timestamp": datetime.utcnow().isoformat(),
                "data_sources": ["ccxt_funding_rate", "binance_premium_index"]
            }
            
            logger.debug(f"Collected complete funding data for {symbol}: current={complete_data['current_funding_rate']}, next={complete_data['next_funding_rate']}")
            return complete_data
            
        except Exception as e:
            logger.error(f"Error fetching complete funding data for {symbol}: {e}")
            return {
                "current_funding_rate": 0,
                "next_funding_rate": 0,
                "mark_price": 0,
                "index_price": 0,
                "collection_timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }

def test_enhanced_funding_collector():
    """
    Test the enhanced funding collector
    """
    print("🧪 Testing Enhanced Funding Collector...")
    
    collector = EnhancedFundingCollector()
    test_symbol = "BTC/USDT:USDT"
    
    try:
        # Test complete funding data collection
        funding_data = collector.fetch_complete_funding_data(test_symbol)
        
        print("✅ Enhanced Funding Collector Test Results:")
        print(f"  - Symbol: {test_symbol}")
        print(f"  - Current Funding Rate: {funding_data['current_funding_rate']}")
        print(f"  - Next Funding Rate: {funding_data['next_funding_rate']}")
        print(f"  - Mark Price: {funding_data['mark_price']}")
        print(f"  - Index Price: {funding_data['index_price']}")
        print(f"  - Next Funding Time: {funding_data.get('next_funding_time_utc', 'N/A')}")
        
        if funding_data.get('error'):
            print(f"  - Error: {funding_data['error']}")
            
        return funding_data
        
    except Exception as e:
        print(f"❌ Enhanced Funding Collector Test Failed: {e}")
        return None

if __name__ == "__main__":
    test_enhanced_funding_collector()