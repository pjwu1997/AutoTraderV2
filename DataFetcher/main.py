import time
import schedule
from datetime import datetime, timedelta
from data_fetcher2 import DataCollector, fetch_binance_futures_pairs  # Ensure this module contains your DataCollector class
# from fetch_pairs import fetch_binance_futures_pairs  # Ensure this module fetches available trading pairs
from concurrent.futures import ThreadPoolExecutor, as_completed

def update_pair_data(pair, collector):
    """
    Fetch and store data for a single trading pair.
    """
    print(f"Updating data for {pair}")
    latest_timestamp = collector.get_latest_timestamp(pair)  # Implement this function
    
    if latest_timestamp:
        start_timestamp = int(time.mktime(latest_timestamp.timetuple()) * 1000)
    else:
        start_date = datetime.utcnow() - timedelta(weeks=4)
        start_timestamp = int(time.mktime(start_date.timetuple()) * 1000)
    
    end_timestamp = int(time.time() * 1000)
    collector.fetch_and_store_fixed_range(pair, start_timestamp, end_timestamp, online=True)

def update_binance_futures(timeframe='5m', max_workers=2):
    """
    Fetch and store Binance futures data using multithreading.
    """
    collector = DataCollector(
        exchange_name="binance", 
        db_uri="mongodb://pj:yolo1234@localhost:27017/", 
        db_name="TradingData", 
        timeframe=timeframe
    )
    
    trading_pairs = fetch_binance_futures_pairs()
    if not trading_pairs:
        print("No trading pairs fetched.")
        return
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_pair_data, pair, collector): pair for pair in trading_pairs}
        
        for future in as_completed(futures):
            pair = futures[future]
            try:
                future.result()
                print(f"Finished updating {pair}")
            except Exception as e:
                print(f"Error updating {pair}: {e}")

# def update_binance_futures(timeframe='5m'):
#     collector = DataCollector(
#         exchange_name="binance", 
#         db_uri="mongodb://pj:yolo1234@localhost:27017/", 
#         db_name="TradingData_20250131", 
#         timeframe=timeframe
#     )
    
#     trading_pairs = fetch_binance_futures_pairs()
#     if not trading_pairs:
#         print("No trading pairs fetched.")
#         return
    
#     for pair in trading_pairs:
#         print(f"Updating data for {pair}")
#         latest_timestamp = collector.get_latest_timestamp(pair)  # Implement this function
        
#         if latest_timestamp:
#             start_date = latest_timestamp  # Continue from last recorded timestamp
#             start_timestamp = int(time.mktime(start_date.timetuple()) * 1000)
#         else:
#             start_date = datetime.utcnow() - timedelta(weeks=4)
#             start_timestamp = int(time.mktime(start_date.timetuple()) * 1000)
        
#         end_timestamp = int(time.time() * 1000)
#         collector.fetch_and_store_fixed_range(pair, start_timestamp, end_timestamp, online=True)

if __name__ == "__main__":
    # Schedule to run every 5 minutes
    schedule.every(5).minutes.do(update_binance_futures)  

    print("Starting periodic data update...")
    
    update_binance_futures()  # Run once at start

    while True:
        schedule.run_pending()
        time.sleep(60)  # Sleep for 60 seconds to prevent excessive CPU usage
