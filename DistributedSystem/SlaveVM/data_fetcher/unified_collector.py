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
import ccxt.async_support as ccxt
import aiohttp
import time
import logging
from logging.handlers import RotatingFileHandler
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, DefaultDict
from collections import defaultdict
from pymongo import MongoClient
from dataclasses import dataclass, field



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
    symbols_file_path: Optional[str] = None

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
        logger.info(f"UnifiedCollector1M initializing with {len(config.symbols)} symbols.")
        self.config = config
        # Defensively ensure symbols are unique and sorted
        self.config.symbols = sorted(list(set(self.config.symbols)))
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
            logger.info(f"Creating new minute buffer for {symbol} at minute {current_minute}")
            self.minute_buffers[symbol] = MinuteAggregation(
                symbol=symbol,
                minute_start=current_minute
            )
        
        return self.minute_buffers[symbol]
    
    async def collect_real_time_data(self, symbol: str):
        """Continuously collect real-time data for 1-minute aggregation"""
        logger.info(f"Real-time collection task started for {symbol}.")
        try:
            while True:
                try:
                    logger.debug(f"Attempting to collect real-time data for {symbol}")
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
                    logger.error(f"Error in real-time collection loop for {symbol}: {e}", exc_info=True)
                    logger.info(f"Continuing real-time collection for {symbol} after error.")
                    await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.warning(f"Real-time collection task for {symbol} was cancelled.")
        finally:
            logger.critical(f"CRITICAL: Real-time collection task for {symbol} has exited its while True loop.")
    
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
            orderbook = await self.exchange.fetch_order_book(symbol, 20)
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
            trades = await self.exchange.fetch_trades(symbol, limit=50)
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
            current_funding = await self.exchange.fetch_funding_rate(symbol)
            
            # Enhanced funding data from Binance API
            url = "https://fapi.binance.com/fapi/v1/premiumIndex"
            symbol_clean = symbol.replace('/', '').replace(':USDT', '')
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={"symbol": symbol_clean}, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            "current_rate": current_funding['fundingRate'],
                            "current_timestamp": current_funding['fundingTimestamp'],
                            "next_timestamp": current_funding['fundingTimestamp'] + 8 * 60 * 60 * 1000,
                            "mark_price": float(data.get('markPrice', 0)),
                            "index_price": float(data.get('indexPrice', 0)),
                            "estimated_settle_price": float(data.get('estimatedSettlePrice', 0))
                        }
                    else:
                        logger.warning(f"Failed to fetch premium index for {symbol}. Status: {response.status}, Response: {await response.text()}")
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
            
            async with aiohttp.ClientSession() as session:
                # Global long/short account ratio (5m period - 1m not supported)
                try:
                    url = f"{base_url}/futures/data/globalLongShortAccountRatio"
                    params = {"symbol": symbol_clean, "period": "5m", "limit": 1}
                    async with session.get(url, params=params, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data:
                                ratios['global_account_ratio'] = {
                                    "longShortRatio": float(data[0]['longShortRatio']),
                                    "longAccount": float(data[0]['longAccount']),
                                    "shortAccount": float(data[0]['shortAccount']),
                                    "timestamp": int(data[0]['timestamp'])
                                }
                except Exception as e:
                    logger.debug(f"Failed to fetch global account ratio for {symbol}: {e}")
                
                # Top trader long/short ratio (5m period - 1m not supported)
                try:
                    url = f"{base_url}/futures/data/topLongShortAccountRatio"
                    params = {"symbol": symbol_clean, "period": "5m", "limit": 1}
                    async with session.get(url, params=params, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data:
                                ratios['top_trader_ratio'] = {
                                    "longShortRatio": float(data[0]['longShortRatio']),
                                    "longAccount": float(data[0]['longAccount']),
                                    "shortAccount": float(data[0]['shortAccount']),
                                    "timestamp": int(data[0]['timestamp'])
                                }
                except Exception as e:
                    logger.debug(f"Failed to fetch top trader ratio for {symbol}: {e}")
                
                # Top position long/short ratio (5m period - 1m not supported)
                try:
                    url = f"{base_url}/futures/data/topLongShortPositionRatio"
                    params = {"symbol": symbol_clean, "period": "5m", "limit": 1}
                    async with session.get(url, params=params, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
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
            ticker = await self.exchange.fetch_ticker(symbol)
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
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params={"symbol": symbol_clean}, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            "open_interest": float(data.get('openInterest', 0)),
                            "timestamp": int(data.get('time', int(time.time() * 1000)))
                        }
                    else:
                        logger.warning(f"Open interest API returned status {response.status} for symbol {symbol_clean}")
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
            
            logger.debug(f"Generating 1-minute aggregated data for {symbol}")
            # Get OHLCV for the minute (this is already 1-minute from exchange)
            ohlcv = await self.exchange.fetch_ohlcv(symbol, '1m', limit=1)
            current_minute_candle = ohlcv[-1] if ohlcv else None
            
            # Aggregate orderbook metrics
            orderbook_metrics = self._aggregate_orderbook_metrics(buffer.orderbook_snapshots)
            
            # Calculate enhanced metrics
            enhanced_metrics = self._calculate_enhanced_metrics_1m(buffer, orderbook_metrics)
            
            # Create aggregated document
            aggregated_data = {
                "symbol": symbol,
                "timestamp": datetime.utcfromtimestamp(buffer.minute_start / 1000),
                "minute_end": datetime.utcfromtimestamp((buffer.minute_start + 60000) / 1000),  # +1 minute
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
        """Store 1-minute aggregated data to MongoDB using upsert"""
        if not self.mongo_available:
            logger.debug("MongoDB not available, skipping storage")
            return
        
        logger.debug(f"Attempting to store aggregated data for {symbol} to MongoDB")
        try:
            collection_name = symbol.replace('/', '').replace(':USDT', '')
            collection = self.db[collection_name]
            
            # Use update_one with upsert=True to merge data
            result = collection.update_one(
                {"timestamp": data["timestamp"], "symbol": data["symbol"]},
                {"$set": data},
                upsert=True
            )
            logger.info(f"Stored 1-minute aggregated data for {symbol}: matched={result.matched_count}, modified={result.modified_count}, upserted_id={result.upserted_id}")
            
        except Exception as e:
            logger.error(f"Failed to store aggregated data for {symbol}: {e}")
    
    async def run_1m_aggregation_cycle(self):
        """Run the 1-minute aggregation cycle for all symbols"""
        try:
            logger.info("Starting 1-minute aggregation cycle")
            logger.info(f"Processing {len(self.config.symbols)} symbols in this cycle.")
            
            for i, symbol in enumerate(self.config.symbols):
                logger.info(f"--- Aggregation cycle for symbol {i+1}/{len(self.config.symbols)}: {symbol} ---")
                try:
                    # Generate aggregated data for the completed minute
                    aggregated_data = await self.generate_1m_aggregated_data(symbol)
                    
                    if aggregated_data:
                        logger.info(f"Successfully generated aggregated data for {symbol}. Storing to MongoDB...")
                        # Store to MongoDB
                        await self.store_aggregated_data(symbol, aggregated_data)
                        
                        logger.info(f"Completed 1-minute aggregation for {symbol}")
                    else:
                        logger.warning(f"Failed to generate aggregated data for {symbol}, it was None.")
                    
                except Exception as e:
                    logger.error(f"Error in 1-minute aggregation for {symbol}: {e}", exc_info=True)
            
            logger.info("Completed 1-minute aggregation cycle")
            
        except Exception as e:
            logger.error(f"Error in 1-minute aggregation cycle: {e}", exc_info=True)
    
    async def start_continuous_collection(self):
        """Start continuous data collection with 1-minute aggregation"""
        try:
            logger.info("--- Entering start_continuous_collection ---")
            
            # Start real-time data collection tasks for each symbol
            tasks = []
            logger.info(f"Found {len(self.config.symbols)} symbols to create collection tasks for.")
            for symbol in self.config.symbols:
                task = asyncio.create_task(self.collect_real_time_data(symbol))
                tasks.append(task)
            logger.info(f"Created {len(tasks)} real-time collection tasks.")
            
            # Start the aggregation cycle (runs every minute)
            async def aggregation_scheduler():
                logger.info("Aggregation scheduler task started.")
                try:
                    while True:
                        # Wait until the next minute boundary
                        now = datetime.utcnow()
                        next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
                        wait_seconds = (next_minute - now).total_seconds()
                        
                        logger.info(f"Aggregation scheduler: waiting {wait_seconds:.2f} seconds until next minute.")
                        await asyncio.sleep(wait_seconds)
                        
                        # Run aggregation for the completed minute
                        logger.info("Aggregation scheduler: starting aggregation cycle.")
                        await self.run_1m_aggregation_cycle()
                except asyncio.CancelledError:
                    logger.warning("Aggregation scheduler task was cancelled.")
                except Exception as e:
                    logger.error(f"CRITICAL: Aggregation scheduler loop failed: {e}", exc_info=True)

            
            aggregation_task = asyncio.create_task(aggregation_scheduler())
            tasks.append(aggregation_task)
            logger.info("Created aggregation scheduler task.")
            
            logger.info(f"Passing {len(tasks)} tasks to asyncio.gather().")
            # Run all tasks concurrently
            await asyncio.gather(*tasks)
            
            logger.critical("CRITICAL: asyncio.gather() in start_continuous_collection has completed. This should not happen.")
            
        except Exception as e:
            logger.error(f"Error in continuous collection: {e}", exc_info=True)
            raise

async def main():
    """Main function for testing"""
    # Load configuration from environment
    hostname = os.getenv("HOSTNAME")
    pod_index = hostname.split('-')[-1]
    symbols_file_path = f"/config/{pod_index}/symbols.csv"
    try:
        with open(symbols_file_path, 'r') as f:
            symbols = [symbol.strip() for symbol in f.read().split(',') if symbol.strip()]
        logger.info(f"Loaded {len(symbols)} symbols from {symbols_file_path}")
    except Exception as e:
        logger.error(f"Failed to read symbols from {symbols_file_path}: {e}")
        symbols = []

    config = CollectorConfig(
        slave_id=os.getenv("SLAVE_ID", "test-slave"),
        symbols=symbols,
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017/"),
        mongo_db_name=os.getenv("MONGO_DB_NAME", "trading_data"),
        timeframe=os.getenv("TIMEFRAME", "1m"),
        aggregation_interval=int(os.getenv("AGGREGATION_INTERVAL", "60"))
    )
    
    collector = UnifiedCollector1M(config)
    await collector.start_continuous_collection()

if __name__ == "__main__":
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler('unified_collector.log', maxBytes=10*1024*1024, backupCount=5),  # Rotate logs at 10MB
        logging.StreamHandler()  # Also print to console
    ]
)
    asyncio.run(main())