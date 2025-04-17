# %%
from binance_bulk_downloader.downloader import BinanceBulkDownloader

downloader = BinanceBulkDownloader(data_type='aggTrades')
downloader.run_download()

# %%
from liqmap.mapping import HistoricalMapping

mapping = HistoricalMapping(
    start_datetime='2023-08-01 00:00:00',
    end_datetime='2023-08-01 06:00:00',
    symbol='BTCUSDT',
    exchange='binance',
)

mapping.liquidation_map_from_historical(
    mode="gross_value",
    threshold_gross_value=100000
)
# %%
from binance_liquidation_feeder import BinanceLiquidationFeeder

liq = BinanceLiquidationFeeder()
liq.ws.run_forever()
# %%
