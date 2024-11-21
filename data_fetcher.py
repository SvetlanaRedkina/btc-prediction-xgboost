import requests
from datetime import datetime, timedelta
import time
import fire
import os
import pandas as pd

class CoinbaseDataFetcher:
    def __init__(self):
        self.base_url = "https://api.exchange.coinbase.com"
        self.granularity = 3600  # 1-hour candles
        self.data_dir = os.path.join('btc_price_prediction', 'data')
        
    def fetch_data(self, days: int = 30):
        """Fetch historical candle data from Coinbase"""
        os.makedirs(self.data_dir, exist_ok=True)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        print(f"Fetching {days} days of data")
        
        candles = []
        current_start = start_date
        chunk_days = 12
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=chunk_days), end_date)
            
            params = {
                "start": current_start.isoformat(),
                "end": current_end.isoformat(),
                "granularity": self.granularity
            }
            
            response = requests.get(
                f"{self.base_url}/products/BTC-USD/candles",
                params=params
            )
            
            if response.status_code != 200:
                raise Exception(f"API request failed: {response.text}")
                
            chunk_data = response.json()
            candles.extend(chunk_data)
            print(f"Got {len(chunk_data)} candles")
            
            current_start = current_end
            time.sleep(0.5)
            
        # Create DataFrame without displaying it
        df = pd.DataFrame(
            candles,
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        
        # Process and save
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        filename = os.path.join(self.data_dir, f"btc_historical_{days}d.csv")
        df.to_csv(filename)
        print(f"Saved {len(candles)} candles to: {filename}")

if __name__ == "__main__":
    fire.Fire(CoinbaseDataFetcher)