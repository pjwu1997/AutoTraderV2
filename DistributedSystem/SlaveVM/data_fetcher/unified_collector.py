#!/usr/bin/env python3
"""
Unified Data Collector - Single collector for all trading data types

This unified collector consolidates:
- Market data (OHLCV, orderbook, trades)
- Long/short ratios (global, top trader)
- Funding rates (current, next)
- Interest rates
- WebSocket real-time data
- Enhanced market metrics
"""

import sys
import os
import asyncio
import websockets
import json
import ccxt
import requests
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from dataclasses import dataclass

# Add parent directories to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../'))
from DataFetcher.data_fetcher import DataFetcher

logger = logging.getLogger(__name__)

@dataclass
class CollectorConfig:
    """Configuration for the unified collector"""
    slave_id: str
    symbols: List[str]
    mongo_uri: str
    mongo_db_name: str = "trading_data"
    timeframe: str = "1m"
    fetch_interval: int = 60
    batch_size: int = 15
    rate_limit_delay: float = 0.1
    max_retries: int = 3
    enable_websocket: bool = True
    enable_rest_api: bool = True

class UnifiedCollector:
    """
    Unified data collector that handles all types of trading data collection
    """
    
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.slave_id = config.slave_id
        
        # Initialize exchanges
        self.exchange = ccxt.binance({
            'sandbox': False,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',  # Use futures by default
            }
        })
        
        # Initialize MongoDB connection
        self.mongo_client = MongoClient(config.mongo_uri, serverSelectionTimeoutMS=5000)
        self.db = self.mongo_client[config.mongo_db_name]
        self.mongo_available = False
        
        # Test MongoDB connection and initialize collections
        try:
            self.mongo_client.admin.command('ping')
            self.mongo_available = True
            self.init_collections()
            logger.info("MongoDB connection established and collections initialized")
        except Exception as e:
            logger.warning(f"MongoDB not available: {e}")
            logger.info("Collector will continue without MongoDB storage")
        
        # Initialize base data fetcher for compatibility
        try:
            self.base_fetcher = DataFetcher(
                exchange_name="binance",
                db_uri=config.mongo_uri,
                db_name=config.mongo_db_name,
                timeframe=config.timeframe
            )
        except Exception as e:
            logger.warning(f"Could not initialize base DataFetcher: {e}")
            self.base_fetcher = None
        
        # WebSocket connection states
        self.ws_connections = {}
        self.ws_running = False
        
        logger.info(f"Unified Collector initialized for {self.slave_id} with {len(config.symbols)} symbols")

    def init_collections(self):
        """Initialize MongoDB collections with proper indexes"""
        collections = [
            'kline_data',
            'funding_rates', 
            'long_short_ratios',
            'interest_rates',
            'liquidations',
            'enhanced_market_data'
        ]
        
        for collection_name in collections:
            collection = self.db[collection_name]
            # Create indexes for better query performance
            collection.create_index([("symbol", 1), ("timestamp", -1)])
            collection.create_index([("slave_id", 1), ("timestamp", -1)])

    # =================== REST API DATA COLLECTION ===================
    
    async def fetch_market_data(self, symbol: str) -> Dict[str, Any]:
        """Fetch comprehensive market data for a symbol"""
        try:
            logger.info(f"Fetching comprehensive market data for {symbol}")
            
            # Get OHLCV data
            ohlcv = await self._fetch_ohlcv(symbol)
            
            # Get orderbook
            orderbook = await self._fetch_orderbook(symbol)
            
            # Get recent trades
            trades = await self._fetch_recent_trades(symbol)
            
            # Get funding rate
            funding_rate = await self._fetch_funding_rate(symbol)
            
            # Get long/short ratios
            long_short_data = await self._fetch_long_short_ratios(symbol)
            
            # Get 24hr ticker
            ticker = await self._fetch_ticker(symbol)
            
            # Get open interest
            open_interest = await self._fetch_open_interest(symbol)
            
            # Calculate additional metrics
            metrics = self._calculate_enhanced_metrics(ohlcv, orderbook, trades)
            
            return {
                "symbol": symbol,
                "timestamp": int(time.time() * 1000),
                "slave_id": self.slave_id,
                "ohlcv": ohlcv,
                "orderbook": orderbook,
                "trades": trades,
                "funding_rate": funding_rate,
                "long_short_ratios": long_short_data,
                "ticker_24h": ticker,
                "open_interest": open_interest,
                "enhanced_metrics": metrics,
                "collection_type": "unified_market_data"
            }
            
        except Exception as e:
            logger.error(f"Error fetching market data for {symbol}: {e}")
            return None

    async def _fetch_ohlcv(self, symbol: str, limit: int = 100) -> List[List]:
        """Fetch OHLCV candlestick data"""
        try:
            ohlcv = self.exchange.fetchOHLCV(symbol, self.config.timeframe, limit=limit)
            return ohlcv
        except Exception as e:
            logger.warning(f"Failed to fetch OHLCV for {symbol}: {e}")
            return []

    async def _fetch_orderbook(self, symbol: str, limit: int = 20) -> Dict:
        """Fetch orderbook data"""
        try:
            orderbook = self.exchange.fetchOrderBook(symbol, limit)
            return {
                "bids": orderbook['bids'][:limit],
                "asks": orderbook['asks'][:limit],
                "timestamp": orderbook['timestamp']
            }
        except Exception as e:
            logger.warning(f"Failed to fetch orderbook for {symbol}: {e}")
            return {"bids": [], "asks": [], "timestamp": None}

    async def _fetch_recent_trades(self, symbol: str, limit: int = 50) -> List[Dict]:
        """Fetch recent trades"""
        try:
            trades = self.exchange.fetchTrades(symbol, limit=limit)
            return [
                {
                    "price": trade['price'],
                    "amount": trade['amount'],
                    "side": trade['side'],
                    "timestamp": trade['timestamp']
                }
                for trade in trades
            ]
        except Exception as e:
            logger.warning(f"Failed to fetch trades for {symbol}: {e}")
            return []

    async def _fetch_funding_rate(self, symbol: str) -> Dict:
        """Fetch current and next funding rates"""
        try:
            # Current funding rate
            current_funding = self.exchange.fetchFundingRate(symbol)
            
            # Next funding rate (estimated)
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            symbol_clean = symbol.replace(':USDT', '')
            response = requests.get(url, params={"symbol": symbol_clean}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000,  # 8 hours later
                    "mark_price": float(data.get('markPrice', 0)),
                    "index_price": float(data.get('indexPrice', 0)),
                    "estimated_settle_price": float(data.get('estimatedSettlePrice', 0))
                }
            else:
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000
                }
                
        except Exception as e:
            logger.warning(f"Failed to fetch funding rate for {symbol}: {e}")
            return {}

    async def _fetch_long_short_ratios(self, symbol: str) -> Dict:
        """Fetch comprehensive long/short ratio data"""
        try:
            symbol_clean = symbol.replace(':USDT', '')
            base_url = "https://fapi.binance.com"
            ratios = {}
            
            # Global long/short account ratio
            try:
                url = f"{base_url}/futures/data/globalLongShortAccountRatio"
                response = requests.get(url, params={
                    "symbol": symbol_clean,
                    "period": "1m",
                    "limit": 1
                }, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        ratios['global_account_ratio'] = {
                            "longShortRatio": float(data[0]['longShortRatio']),
                            "longAccount": float(data[0]['longAccount']),
                            "shortAccount": float(data[0]['shortAccount']),
                            "timestamp": int(data[0]['timestamp'])
                        }
            except Exception as e:
                logger.debug(f"Failed to fetch global account ratio for {symbol}: {e}")
            
            # Top trader long/short ratio
            try:
                url = f"{base_url}/futures/data/topLongShortAccountRatio"
                response = requests.get(url, params={
                    "symbol": symbol_clean,
                    "period": "1m",
                    "limit": 1
                }, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        ratios['top_trader_ratio'] = {
                            "longShortRatio": float(data[0]['longShortRatio']),
                            "longAccount": float(data[0]['longAccount']),
                            "shortAccount": float(data[0]['shortAccount']),
                            "timestamp": int(data[0]['timestamp'])
                        }
            except Exception as e:
                logger.debug(f"Failed to fetch top trader ratio for {symbol}: {e}")
            
            # Top trader position ratio
            try:
                url = f"{base_url}/futures/data/topLongShortPositionRatio"
                response = requests.get(url, params={
                    "symbol": symbol_clean,
                    "period": "1m", 
                    "limit": 1
                }, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data:
                        ratios['top_position_ratio'] = {
                            "longShortRatio": float(data[0]['longShortRatio']),
                            "longPosition": float(data[0]['longPosition']),
                            "shortPosition": float(data[0]['shortPosition']),
                            "timestamp": int(data[0]['timestamp'])
                        }
            except Exception as e:
                logger.debug(f"Failed to fetch top position ratio for {symbol}: {e}")
                
            return ratios
            
        except Exception as e:
            logger.warning(f"Failed to fetch long/short ratios for {symbol}: {e}")
            return {}

    async def _fetch_ticker(self, symbol: str) -> Dict:
        """Fetch 24hr ticker statistics"""
        try:
            ticker = self.exchange.fetchTicker(symbol)
            return {
                "open": ticker['open'],
                "high": ticker['high'],
                "low": ticker['low'],
                "close": ticker['close'],
                "volume": ticker['baseVolume'],
                "quote_volume": ticker['quoteVolume'],
                "change": ticker['change'],
                "percentage": ticker['percentage'],
                "vwap": ticker['vwap']
            }
        except Exception as e:
            logger.warning(f"Failed to fetch ticker for {symbol}: {e}")
            return {}

    async def _fetch_open_interest(self, symbol: str) -> Dict:
        """Fetch current open interest data"""
        try:
            # Convert symbol format: BTC/USDT:USDT -> BTCUSDT
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            
            # Current open interest
            url = "https://fapi.binance.com/fapi/v1/openInterest"
            response = requests.get(url, params={"symbol": symbol_clean}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "open_interest": float(data.get('openInterest', 0)),
                    "timestamp": int(data.get('time', int(time.time() * 1000)))
                }
            else:
                logger.warning(f"Open interest API returned status {response.status_code} for symbol {symbol_clean}")
                return {}
                
        except Exception as e:
            logger.warning(f"Failed to fetch open interest for {symbol}: {e}")
            return {}

    def _calculate_enhanced_metrics(self, ohlcv: List, orderbook: Dict, trades: List) -> Dict:
        """Calculate additional market metrics"""
        try:
            metrics = {}
            
            # Calculate CVD (Cumulative Volume Delta) from trades
            if trades:
                buy_volume = sum(t['amount'] for t in trades if t['side'] == 'buy')
                sell_volume = sum(t['amount'] for t in trades if t['side'] == 'sell')
                metrics['cvd'] = buy_volume - sell_volume
                metrics['buy_sell_ratio'] = buy_volume / sell_volume if sell_volume > 0 else 0
            
            # Calculate spread from orderbook
            if orderbook.get('bids') and orderbook.get('asks'):
                best_bid = orderbook['bids'][0][0]
                best_ask = orderbook['asks'][0][0]
                metrics['spread'] = best_ask - best_bid
                metrics['spread_percentage'] = (metrics['spread'] / best_ask) * 100
            
            # Calculate volatility from OHLCV
            if len(ohlcv) > 1:
                closes = [candle[4] for candle in ohlcv[-20:]]  # Last 20 closes
                if len(closes) > 1:
                    price_changes = [abs(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
                    metrics['volatility'] = sum(price_changes) / len(price_changes)
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Failed to calculate enhanced metrics: {e}")
            return {}

    # =================== WEBSOCKET DATA COLLECTION ===================
    
    async def start_websocket_collection(self):
        """Start WebSocket connections for real-time data"""
        if not self.config.enable_websocket:
            return
            
        self.ws_running = True
        
        # Start kline WebSocket
        asyncio.create_task(self._ws_kline_handler())
        
        # Start liquidation WebSocket
        asyncio.create_task(self._ws_liquidation_handler())
        
        logger.info("WebSocket collection started")

    async def _ws_kline_handler(self):
        """Handle kline WebSocket streams"""
        while self.ws_running:
            try:
                # Create streams for all symbols
                streams = []
                for symbol in self.config.symbols:
                    symbol_clean = symbol.replace(':USDT', '').lower()
                    streams.append(f"{symbol_clean}usdt@kline_{self.config.timeframe}")
                
                stream_names = '/'.join(streams)
                ws_url = f"wss://fstream.binance.com/ws/{stream_names}"
                
                async with websockets.connect(ws_url) as websocket:
                    logger.info(f"Connected to kline WebSocket for {len(streams)} symbols")
                    
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            await self._process_kline_message(data)
                        except Exception as e:
                            logger.error(f"Error processing kline message: {e}")
                            
            except Exception as e:
                logger.error(f"Kline WebSocket error: {e}")
                if self.ws_running:
                    await asyncio.sleep(5)  # Retry after 5 seconds

    async def _ws_liquidation_handler(self):
        """Handle liquidation WebSocket stream"""
        while self.ws_running:
            try:
                ws_url = "wss://fstream.binance.com/ws/!forceOrder@arr"
                
                async with websockets.connect(ws_url) as websocket:
                    logger.info("Connected to liquidation WebSocket")
                    
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            await self._process_liquidation_message(data)
                        except Exception as e:
                            logger.error(f"Error processing liquidation message: {e}")
                            
            except Exception as e:
                logger.error(f"Liquidation WebSocket error: {e}")
                if self.ws_running:
                    await asyncio.sleep(5)  # Retry after 5 seconds

    async def _process_kline_message(self, data: Dict):
        """Process kline WebSocket message"""
        try:
            if 'k' in data:
                kline = data['k']
                symbol = kline['s']
                
                # Only process symbols we're responsible for
                symbol_formatted = f"{symbol[:len(symbol)-4]}:USDT"
                if symbol_formatted not in self.config.symbols:
                    return
                
                kline_data = {
                    "symbol": symbol_formatted,
                    "timestamp": kline['t'],
                    "open": float(kline['o']),
                    "high": float(kline['h']),
                    "low": float(kline['l']),
                    "close": float(kline['c']),
                    "volume": float(kline['v']),
                    "quote_volume": float(kline['q']),
                    "trades": int(kline['n']),
                    "is_closed": kline['x'],
                    "slave_id": self.slave_id,
                    "data_source": "websocket"
                }
                
                # Store in MongoDB
                if self.mongo_available:
                    self.db.kline_data.insert_one(kline_data)
                    logger.debug(f"Stored kline data for {symbol_formatted}")
                else:
                    logger.debug(f"Would store kline data for {symbol_formatted}")
                
        except Exception as e:
            logger.error(f"Error processing kline message: {e}")

    async def _process_liquidation_message(self, data: Dict):
        """Process liquidation WebSocket message"""
        try:
            if 'o' in data:
                order = data['o']
                symbol = order['s']
                
                # Only process symbols we're responsible for
                symbol_formatted = f"{symbol[:len(symbol)-4]}:USDT"
                if symbol_formatted not in self.config.symbols:
                    return
                
                liquidation_data = {
                    "symbol": symbol_formatted,
                    "timestamp": order['T'],
                    "side": order['S'],
                    "order_type": order['o'],
                    "time_in_force": order['f'],
                    "quantity": float(order['q']),
                    "price": float(order['p']),
                    "average_price": float(order['ap']),
                    "execution_type": order['X'],
                    "order_status": order['x'],
                    "slave_id": self.slave_id,
                    "data_source": "websocket"
                }
                
                # Store in MongoDB
                if self.mongo_available:
                    self.db.liquidations.insert_one(liquidation_data)
                    logger.debug(f"Stored liquidation data for {symbol_formatted}")
                else:
                    logger.debug(f"Would store liquidation data for {symbol_formatted}")
                
        except Exception as e:
            logger.error(f"Error processing liquidation message: {e}")

    # =================== DATA STORAGE ===================
    
    async def store_market_data(self, data: Dict):
        """Store market data to MongoDB"""
        try:
            if data and self.mongo_available:
                collection_name = f"{data['symbol'].replace(':', '_').lower()}_market_data"
                self.db[collection_name].insert_one(data)
                logger.debug(f"Stored market data for {data['symbol']}")
            elif data and not self.mongo_available:
                logger.debug(f"MongoDB not available - would store data for {data['symbol']}")
        except Exception as e:
            logger.error(f"Error storing market data: {e}")

    # =================== MAIN COLLECTION LOOP ===================
    
    async def run_collection_cycle(self):
        """Run one complete data collection cycle"""
        try:
            logger.info(f"Starting collection cycle for {len(self.config.symbols)} symbols")
            
            # Process symbols in batches
            for i in range(0, len(self.config.symbols), self.config.batch_size):
                batch = self.config.symbols[i:i + self.config.batch_size]
                
                # Collect data for batch
                tasks = []
                for symbol in batch:
                    if self.config.enable_rest_api:
                        tasks.append(self.fetch_market_data(symbol))
                
                # Execute batch
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Store results
                    for result in results:
                        if isinstance(result, dict):
                            await self.store_market_data(result)
                        elif isinstance(result, Exception):
                            logger.error(f"Collection error: {result}")
                
                # Rate limiting
                if i + self.config.batch_size < len(self.config.symbols):
                    await asyncio.sleep(self.config.rate_limit_delay)
            
            logger.info("Collection cycle completed")
            
        except Exception as e:
            logger.error(f"Error in collection cycle: {e}")

    async def start_continuous_collection(self):
        """Start continuous data collection"""
        logger.info(f"Starting continuous collection for slave {self.slave_id}")
        
        # Start WebSocket collection
        if self.config.enable_websocket:
            await self.start_websocket_collection()
        
        # Main collection loop
        while True:
            try:
                if self.config.enable_rest_api:
                    await self.run_collection_cycle()
                
                # Wait for next cycle
                await asyncio.sleep(self.config.fetch_interval)
                
            except Exception as e:
                logger.error(f"Error in continuous collection: {e}")
                await asyncio.sleep(30)  # Wait before retry

    def stop_collection(self):
        """Stop all collection activities"""
        self.ws_running = False
        logger.info("Collection stopped")

# =================== MAIN EXECUTION ===================

async def main():
    """Main execution function"""
    # Load configuration from environment
    import os
    
    config = CollectorConfig(
        slave_id=os.getenv("SLAVE_ID", "slave-1"),
        symbols=os.getenv("SYMBOLS", "BTC:USDT,ETH:USDT").split(","),
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "trading_data"),
        timeframe=os.getenv("TIMEFRAME", "1m"),
        fetch_interval=int(os.getenv("FETCH_INTERVAL", "60")),
        batch_size=int(os.getenv("BATCH_SIZE", "15")),
        enable_websocket=os.getenv("ENABLE_WEBSOCKET", "true").lower() == "true",
        enable_rest_api=os.getenv("ENABLE_REST_API", "true").lower() == "true"
    )
    
    # Initialize collector
    collector = UnifiedCollector(config)
    
    try:
        # Start collection
        await collector.start_continuous_collection()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        collector.stop_collection()
        logger.info("Unified collector stopped")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main())