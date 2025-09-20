#!/usr/bin/env python3
"""
Unified Data Collector - 1-Minute Aggregated Version

This enhanced unified collector aggregates ALL data to 1-minute intervals:
- OHLCV: 1-minute candles
- Orderbook: 1-minute aggregated depth and spread metrics
- Trades: 1-minute aggregated volume, count, VWAP
- Liquidations: 1-minute aggregated liquidation amounts
- Funding rates: Latest rate within the minute
- Long/short ratios: 1-minute period data
- Enhanced metrics: Calculated from 1-minute aggregated data
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
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, DefaultDict
from collections import defaultdict
from pymongo import MongoClient
from dataclasses import dataclass, field

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
    aggregation_interval: int = 60  # 60 seconds = 1 minute
    batch_size: int = 15
    rate_limit_delay: float = 0.1
    max_retries: int = 3
    enable_websocket: bool = True
    enable_rest_api: bool = True

@dataclass
class MinuteAggregation:
    """1-minute aggregation buffer for a symbol"""
    symbol: str
    minute_start: int
    
    # Trade aggregations
    trades: List[Dict] = field(default_factory=list)
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    total_volume: float = 0.0
    trade_count: int = 0
    vwap_sum: float = 0.0
    vwap_volume: float = 0.0
    
    # Liquidation aggregations
    liquidations: List[Dict] = field(default_factory=list)
    liquidation_buy_volume: float = 0.0
    liquidation_sell_volume: float = 0.0
    liquidation_count: int = 0
    
    # Orderbook snapshots (for aggregation)
    orderbook_snapshots: List[Dict] = field(default_factory=list)
    
    # Latest values (for rates that don't aggregate)
    latest_funding_rate: Optional[Dict] = None
    latest_long_short_ratios: Optional[Dict] = None
    latest_open_interest: Optional[Dict] = None
    latest_ticker: Optional[Dict] = None

class UnifiedCollector1M:
    """
    Enhanced unified data collector with 1-minute aggregation for all data types
    """
    
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.slave_id = config.slave_id
        
        # Initialize exchanges
        self.exchange = ccxt.binance({
            'sandbox': False,
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
            'timeout': 30000
        })
        
        # 1-minute aggregation buffers
        self.minute_buffers: Dict[str, MinuteAggregation] = {}
        
        # MongoDB connection
        try:
            self.mongo_client = MongoClient(config.mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.mongo_client[config.mongo_db_name]
            # Test connection
            self.mongo_client.admin.command('ping')
            self.mongo_available = True
            logger.info(f"Connected to MongoDB: {config.mongo_uri}")
        except Exception as e:
            logger.warning(f"MongoDB not available: {e}")
            self.mongo_available = False
            self.mongo_client = None
            self.db = None
        
        # WebSocket connections
        self.websocket_connections = {}
        self.websocket_running = False
    
    def get_current_minute_start(self) -> int:
        """Get the start timestamp of the current minute"""
        now = datetime.utcnow()
        minute_start = now.replace(second=0, microsecond=0)
        return int(minute_start.timestamp() * 1000)
    
    def get_or_create_minute_buffer(self, symbol: str) -> MinuteAggregation:
        """Get or create a minute aggregation buffer for a symbol"""
        current_minute = self.get_current_minute_start()
        
        # Check if we need a new buffer (new minute)
        if symbol not in self.minute_buffers or self.minute_buffers[symbol].minute_start != current_minute:
            self.minute_buffers[symbol] = MinuteAggregation(
                symbol=symbol,
                minute_start=current_minute
            )
        
        return self.minute_buffers[symbol]
    
    async def collect_real_time_data(self, symbol: str):
        """Continuously collect real-time data for 1-minute aggregation"""
        while True:
            try:
                buffer = self.get_or_create_minute_buffer(symbol)
                
                # Collect orderbook snapshot (every 5 seconds)
                orderbook = await self._fetch_orderbook_snapshot(symbol)
                if orderbook:
                    buffer.orderbook_snapshots.append({
                        'timestamp': int(time.time() * 1000),
                        'data': orderbook
                    })
                
                # Collect recent trades (every 5 seconds)
                trades = await self._fetch_recent_trades_for_aggregation(symbol)
                for trade in trades:
                    self._aggregate_trade(buffer, trade)
                
                # Update latest rates (every 10 seconds)
                if len(buffer.orderbook_snapshots) % 2 == 0:  # Every 10 seconds
                    buffer.latest_funding_rate = await self._fetch_funding_rate(symbol)
                    buffer.latest_long_short_ratios = await self._fetch_long_short_ratios(symbol)
                    buffer.latest_open_interest = await self._fetch_open_interest(symbol)
                    buffer.latest_ticker = await self._fetch_ticker(symbol)
                
                await asyncio.sleep(5)  # Collect every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in real-time collection for {symbol}: {e}")
                await asyncio.sleep(5)
    
    def _aggregate_trade(self, buffer: MinuteAggregation, trade: Dict):
        """Aggregate a trade into the minute buffer"""
        amount = trade.get('amount', 0)
        price = trade.get('price', 0)
        side = trade.get('side', 'unknown')
        
        buffer.trades.append(trade)
        buffer.total_volume += amount
        buffer.trade_count += 1
        
        # Calculate VWAP components
        notional = amount * price
        buffer.vwap_sum += notional
        buffer.vwap_volume += amount
        
        # Separate buy/sell volumes
        if side == 'buy':
            buffer.buy_volume += amount
        elif side == 'sell':
            buffer.sell_volume += amount
    
    def _aggregate_liquidation(self, buffer: MinuteAggregation, liquidation: Dict):
        """Aggregate a liquidation into the minute buffer"""
        quantity = liquidation.get('quantity', 0)
        side = liquidation.get('side', 'unknown')
        
        buffer.liquidations.append(liquidation)
        buffer.liquidation_count += 1
        
        if side == 'BUY':
            buffer.liquidation_buy_volume += quantity
        elif side == 'SELL':
            buffer.liquidation_sell_volume += quantity
    
    async def _fetch_orderbook_snapshot(self, symbol: str) -> Optional[Dict]:
        """Fetch orderbook snapshot for aggregation"""
        try:
            orderbook = self.exchange.fetchOrderBook(symbol, 20)
            return {
                "bids": orderbook['bids'][:20],
                "asks": orderbook['asks'][:20],
                "timestamp": orderbook.get('timestamp', int(time.time() * 1000))
            }
        except Exception as e:
            logger.warning(f"Failed to fetch orderbook for {symbol}: {e}")
            return None
    
    async def _fetch_recent_trades_for_aggregation(self, symbol: str) -> List[Dict]:
        """Fetch recent trades for aggregation"""
        try:
            trades = self.exchange.fetchTrades(symbol, limit=50)
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
        """Fetch current funding rate data"""
        try:
            current_funding = self.exchange.fetchFundingRate(symbol)
            
            # Enhanced funding data from Binance API
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            response = requests.get(url, params={"symbol": symbol_clean}, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "current_rate": current_funding['fundingRate'],
                    "current_timestamp": current_funding['fundingTimestamp'],
                    "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000,
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
        """Fetch 1-minute long/short ratio data"""
        try:
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            base_url = "https://fapi.binance.com"
            ratios = {}
            
            # Global long/short account ratio (1m period)
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
            
            # Top trader long/short ratio (1m period)
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
            
            # Top position long/short ratio (1m period)
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
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            
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
    
    def _aggregate_orderbook_metrics(self, snapshots: List[Dict]) -> Dict:
        """Aggregate orderbook snapshots into 1-minute metrics"""
        if not snapshots:
            return {}
        
        try:
            spreads = []
            bid_depths = []
            ask_depths = []
            
            for snapshot in snapshots:
                data = snapshot['data']
                if data.get('bids') and data.get('asks'):
                    best_bid = data['bids'][0][0] if data['bids'] else 0
                    best_ask = data['asks'][0][0] if data['asks'] else 0
                    
                    if best_bid and best_ask:
                        spread = best_ask - best_bid
                        spreads.append(spread)
                    
                    # Calculate depth (total volume in top 10 levels)
                    bid_depth = sum(level[1] for level in data['bids'][:10])
                    ask_depth = sum(level[1] for level in data['asks'][:10])
                    bid_depths.append(bid_depth)
                    ask_depths.append(ask_depth)
            
            # Use the last snapshot for current orderbook state
            last_snapshot = snapshots[-1]['data']
            
            return {
                "current_orderbook": {
                    "bids": last_snapshot.get('bids', [])[:10],  # Top 10 levels
                    "asks": last_snapshot.get('asks', [])[:10],  # Top 10 levels
                    "timestamp": snapshots[-1]['timestamp']
                },
                "minute_metrics": {
                    "avg_spread": statistics.mean(spreads) if spreads else 0,
                    "min_spread": min(spreads) if spreads else 0,
                    "max_spread": max(spreads) if spreads else 0,
                    "avg_bid_depth": statistics.mean(bid_depths) if bid_depths else 0,
                    "avg_ask_depth": statistics.mean(ask_depths) if ask_depths else 0,
                    "snapshot_count": len(snapshots)
                }
            }
        except Exception as e:
            logger.warning(f"Failed to aggregate orderbook metrics: {e}")
            return {}
    
    def _calculate_enhanced_metrics_1m(self, buffer: MinuteAggregation, orderbook_metrics: Dict) -> Dict:
        """Calculate enhanced metrics from 1-minute aggregated data"""
        try:
            metrics = {}
            
            # CVD from 1-minute aggregated trades
            if buffer.buy_volume or buffer.sell_volume:
                metrics['cvd'] = buffer.buy_volume - buffer.sell_volume
                metrics['buy_sell_ratio'] = buffer.buy_volume / buffer.sell_volume if buffer.sell_volume > 0 else float('inf')
            else:
                metrics['cvd'] = 0
                metrics['buy_sell_ratio'] = 1.0
            
            # VWAP for the minute
            if buffer.vwap_volume > 0:
                metrics['vwap'] = buffer.vwap_sum / buffer.vwap_volume
            else:
                metrics['vwap'] = 0
            
            # Trade metrics
            metrics['trade_count'] = buffer.trade_count
            metrics['total_volume'] = buffer.total_volume
            metrics['buy_volume'] = buffer.buy_volume
            metrics['sell_volume'] = buffer.sell_volume
            
            # Liquidation metrics
            metrics['liquidation_count'] = buffer.liquidation_count
            metrics['liquidation_buy_volume'] = buffer.liquidation_buy_volume
            metrics['liquidation_sell_volume'] = buffer.liquidation_sell_volume
            metrics['total_liquidation_volume'] = buffer.liquidation_buy_volume + buffer.liquidation_sell_volume
            
            # Orderbook metrics
            if orderbook_metrics and 'minute_metrics' in orderbook_metrics:
                om = orderbook_metrics['minute_metrics']
                metrics['avg_spread'] = om.get('avg_spread', 0)
                metrics['spread_volatility'] = om.get('max_spread', 0) - om.get('min_spread', 0)
                metrics['avg_bid_depth'] = om.get('avg_bid_depth', 0)
                metrics['avg_ask_depth'] = om.get('avg_ask_depth', 0)
                metrics['depth_imbalance'] = (om.get('avg_bid_depth', 0) - om.get('avg_ask_depth', 0)) / (om.get('avg_bid_depth', 0) + om.get('avg_ask_depth', 0)) if (om.get('avg_bid_depth', 0) + om.get('avg_ask_depth', 0)) > 0 else 0
            
            return metrics
            
        except Exception as e:
            logger.warning(f"Failed to calculate enhanced metrics: {e}")
            return {}
    
    async def generate_1m_aggregated_data(self, symbol: str) -> Optional[Dict]:
        """Generate 1-minute aggregated data for a symbol"""
        try:
            buffer = self.minute_buffers.get(symbol)
            if not buffer:
                logger.warning(f"No minute buffer found for {symbol}")
                return None
            
            # Get OHLCV for the minute (this is already 1-minute from exchange)
            ohlcv = self.exchange.fetchOHLCV(symbol, '1m', limit=1)
            current_minute_candle = ohlcv[-1] if ohlcv else None
            
            # Aggregate orderbook metrics
            orderbook_metrics = self._aggregate_orderbook_metrics(buffer.orderbook_snapshots)
            
            # Calculate enhanced metrics
            enhanced_metrics = self._calculate_enhanced_metrics_1m(buffer, orderbook_metrics)
            
            # Create aggregated document
            aggregated_data = {
                "symbol": symbol,
                "timestamp": buffer.minute_start,
                "minute_end": buffer.minute_start + 60000,  # +1 minute
                "slave_id": self.slave_id,
                "collection_type": "unified_market_data_1m",
                
                # 1-minute OHLCV candle
                "ohlcv": current_minute_candle,
                
                # Aggregated orderbook
                "orderbook": orderbook_metrics.get('current_orderbook', {}),
                "orderbook_metrics": orderbook_metrics.get('minute_metrics', {}),
                
                # Aggregated trades
                "trade_metrics": {
                    "count": buffer.trade_count,
                    "total_volume": buffer.total_volume,
                    "buy_volume": buffer.buy_volume,
                    "sell_volume": buffer.sell_volume,
                    "vwap": enhanced_metrics.get('vwap', 0),
                    "buy_sell_ratio": enhanced_metrics.get('buy_sell_ratio', 1.0)
                },
                
                # Aggregated liquidations
                "liquidation_metrics": {
                    "count": buffer.liquidation_count,
                    "buy_volume": buffer.liquidation_buy_volume,
                    "sell_volume": buffer.liquidation_sell_volume,
                    "total_volume": buffer.liquidation_buy_volume + buffer.liquidation_sell_volume
                },
                
                # Latest rates (taken from most recent in the minute)
                "funding_rate": buffer.latest_funding_rate or {},
                "long_short_ratios": buffer.latest_long_short_ratios or {},
                "ticker_24h": buffer.latest_ticker or {},
                "open_interest": buffer.latest_open_interest or {},
                
                # Enhanced metrics
                "enhanced_metrics": enhanced_metrics
            }
            
            return aggregated_data
            
        except Exception as e:
            logger.error(f"Failed to generate 1-minute aggregated data for {symbol}: {e}")
            return None
    
    async def store_aggregated_data(self, symbol: str, data: Dict):
        """Store 1-minute aggregated data to MongoDB"""
        if not self.mongo_available:
            logger.debug("MongoDB not available, skipping storage")
            return
        
        try:
            collection_name = f"{symbol.replace('/', '').replace(':USDT', '')}_1m_aggregated"
            collection = self.db[collection_name]
            
            # Insert the aggregated data
            result = collection.insert_one(data)
            logger.info(f"Stored 1-minute aggregated data for {symbol}: {result.inserted_id}")
            
        except Exception as e:
            logger.error(f"Failed to store aggregated data for {symbol}: {e}")
    
    async def run_1m_aggregation_cycle(self):
        """Run the 1-minute aggregation cycle for all symbols"""
        try:
            logger.info("Starting 1-minute aggregation cycle")
            
            for symbol in self.config.symbols:
                try:
                    # Generate aggregated data for the completed minute
                    aggregated_data = await self.generate_1m_aggregated_data(symbol)
                    
                    if aggregated_data:
                        # Store to MongoDB
                        await self.store_aggregated_data(symbol, aggregated_data)
                        
                        logger.info(f"Completed 1-minute aggregation for {symbol}")
                    
                except Exception as e:
                    logger.error(f"Error in 1-minute aggregation for {symbol}: {e}")
            
            logger.info("Completed 1-minute aggregation cycle")
            
        except Exception as e:
            logger.error(f"Error in 1-minute aggregation cycle: {e}")
    
    async def start_continuous_collection(self):
        """Start continuous data collection with 1-minute aggregation"""
        try:
            logger.info("Starting continuous 1-minute aggregated collection")
            
            # Start real-time data collection tasks for each symbol
            tasks = []
            for symbol in self.config.symbols:
                task = asyncio.create_task(self.collect_real_time_data(symbol))
                tasks.append(task)
            
            # Start the aggregation cycle (runs every minute)
            async def aggregation_scheduler():
                while True:
                    # Wait until the next minute boundary
                    now = datetime.utcnow()
                    next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                    wait_seconds = (next_minute - now).total_seconds()
                    
                    await asyncio.sleep(wait_seconds)
                    
                    # Run aggregation for the completed minute
                    await self.run_1m_aggregation_cycle()
            
            aggregation_task = asyncio.create_task(aggregation_scheduler())
            tasks.append(aggregation_task)
            
            # Run all tasks concurrently
            await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Error in continuous collection: {e}")
            raise

async def main():
    """Main function for testing"""
    # Load configuration from environment
    config = CollectorConfig(
        slave_id=os.getenv("SLAVE_ID", "test-slave"),
        symbols=os.getenv("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT").split(","),
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "trading_data"),
        timeframe=os.getenv("TIMEFRAME", "1m"),
        aggregation_interval=int(os.getenv("AGGREGATION_INTERVAL", "60"))
    )
    
    collector = UnifiedCollector1M(config)
    await collector.start_continuous_collection()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())