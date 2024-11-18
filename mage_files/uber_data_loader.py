from mage_ai.io.file import FileIO
from pandas import DataFrame
import pandas as pd
from pathlib import Path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
    from mage_ai.data_preparation.utils import get_repo_path

@data_loader
def load_data_from_file(*args, **kwargs) -> DataFrame:
    """
    Load data from the raw CSV file
    """
    # Get the absolute path to the data file
    data_path = Path(get_repo_path()) / 'data' / 'raw' / 'uber_data.csv'
    
    # Validate file exists
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found at {data_path}")
    
    # Read the CSV file
    df = pd.read_csv(data_path)
    
    # Basic validation
    required_columns = [
        'pickup_datetime',
        'dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'pickup_longitude',
        'pickup_latitude',
        'dropoff_longitude',
        'dropoff_latitude',
        'payment_type',
        'fare_amount',
        'tip_amount',
        'total_amount'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return df
