import pandas as pd
import numpy as np
from typing import Tuple
import fire
import os

class DataPreprocessor:
    def __init__(self, window_size: int = 24):
        self.window_size = window_size
        self.data_dir = os.path.join('btc_price_prediction', 'data')
        
    def load_and_sort(self, filename: str) -> pd.DataFrame:
        """Load CSV and sort by timestamp"""
        filepath = os.path.join(self.data_dir, filename)
        df = pd.read_csv(filepath)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df.sort_values('timestamp')
    
    def create_sequences(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, pd.DatetimeIndex]:
        """Create sequences of 24h -> 1h prediction"""
        volumes = df['volume'].values
        timestamps = df['timestamp'].values
        X, y = [], []
        
        for i in range(len(volumes) - self.window_size):
            X.append(volumes[i:(i + self.window_size)])
            y.append(volumes[i + self.window_size])
            
        target_timestamps = timestamps[self.window_size:]
        return np.array(X), np.array(y), pd.DatetimeIndex(target_timestamps)
    
    def process(self, filename: str, train_size: float = 0.8):
        """Process data end-to-end"""
        # Load and prepare data
        df = self.load_and_sort(filename)
        X, y, timestamps = self.create_sequences(df)
        
        # Split into train/test
        split_idx = int(len(X) * train_size)
        train_data = {
            'X': X[:split_idx],
            'y': y[:split_idx],
            'timestamps': timestamps[:split_idx]
        }
        test_data = {
            'X': X[split_idx:],
            'y': y[split_idx:],
            'timestamps': timestamps[split_idx:]
        }
        
        # Print minimal info with clear time ranges
        print(f"\nData processed:")
        print(f"Training sequences: {len(train_data['X'])}")
        print(f"Training period: {train_data['timestamps'][0]} to {train_data['timestamps'][-1]}")
        print(f"Test sequences: {len(test_data['X'])}")
        print(f"Test period: {test_data['timestamps'][0]} to {test_data['timestamps'][-1]}")
        
        return "Data processing complete. Use in Python to access the arrays."

if __name__ == "__main__":
    fire.Fire(DataPreprocessor)