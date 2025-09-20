#!/usr/bin/env python3
"""
Test script to create per-symbol collections directly
"""

from pymongo import MongoClient
from datetime import datetime

def test_per_symbol_collections():
    # Connect to MongoDB
    client = MongoClient("mongodb://20.2.20.242:27017/")
    db = client["trading_data"]
    
    print("Testing per-symbol collection creation...")
    
    # Test symbols
    test_symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]
    
    for symbol in test_symbols:
        collection_name = f"{symbol}_1m"
        collection = db[collection_name]
        
        # Insert a test document
        test_doc = {
            "symbol": symbol,
            "exchange": "binance",
            "timestamp": datetime.utcnow(),
            "test_data": True,
            "collector_id": "test-script"
        }
        
        collection.insert_one(test_doc)
        print(f"✅ Created collection {collection_name}")
    
    # List all collections
    print("\n=== All Collections ===")
    collections = db.list_collection_names()
    for collection_name in sorted(collections):
        count = db[collection_name].count_documents({})
        print(f"{collection_name}: {count} documents")

if __name__ == "__main__":
    test_per_symbol_collections()