import pandas as pd
from pathlib import Path
import os

def load_data_from_file():
    """
    Load data from the Parquet files
    """
    # Get the absolute path to the data files
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    project_root = current_dir.parent
    jan_data_path = project_root / 'data' / 'raw' / 'yellow_tripdata_2024-01.parquet'
    feb_data_path = project_root / 'data' / 'raw' / 'yellow_tripdata_2024-02.parquet'
    
    # Validate files exist
    if not jan_data_path.exists() or not feb_data_path.exists():
        raise FileNotFoundError(f"Data files not found at {jan_data_path} or {feb_data_path}")
    
    # Read and combine the Parquet files
    df_jan = pd.read_parquet(jan_data_path)
    df_feb = pd.read_parquet(feb_data_path)
    df = pd.concat([df_jan, df_feb], ignore_index=True)
    
    # Rename columns to match our pipeline
    column_mapping = {
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'PULocationID': 'pickup_location_id',
        'DOLocationID': 'dropoff_location_id',
        'payment_type': 'payment_type',
        'fare_amount': 'fare_amount',
        'tip_amount': 'tip_amount',
        'total_amount': 'total_amount'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Select and reorder columns we need
    required_columns = [
        'pickup_datetime',
        'dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'pickup_location_id',
        'dropoff_location_id',
        'payment_type',
        'fare_amount',
        'tip_amount',
        'total_amount'
    ]
    
    # Validate all required columns are present
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return df[required_columns]

if __name__ == "__main__":
    # Load and display sample of the data
    df = load_data_from_file()
    print("\nDataset Shape:", df.shape)
    print("\nSample Data:")
    print(df.head())
    print("\nColumn Info:")
    print(df.info())
