from mage_ai.data_preparation.decorators import transformer
from mage_ai.data_preparation.utils import get_repo_path
from pandas import DataFrame
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import duckdb

@transformer
def transform_data(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Transform the raw Uber data into dimensional model format
    """
    # Create DuckDB connection
    db_path = Path(get_repo_path()) / 'uber_rides.db'
    conn = duckdb.connect(str(db_path))
    
    try:
        # 1. Clean and prepare datetime data
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds()
        
        # 2. Create dimension tables
        
        # dim_datetime
        dim_datetime = pd.DataFrame({
            'datetime_id': range(len(df)),
            'pickup_datetime': df['pickup_datetime'],
            'pickup_hour': df['pickup_datetime'].dt.hour,
            'pickup_day': df['pickup_datetime'].dt.day,
            'pickup_month': df['pickup_datetime'].dt.month,
            'pickup_year': df['pickup_datetime'].dt.year,
            'pickup_weekday': df['pickup_datetime'].dt.weekday
        })
        
        # dim_location
        dim_location = pd.DataFrame({
            'location_id': range(len(df)),
            'pickup_latitude': df['pickup_latitude'],
            'pickup_longitude': df['pickup_longitude'],
            'dropoff_latitude': df['dropoff_latitude'],
            'dropoff_longitude': df['dropoff_longitude']
        })
        
        # dim_payment
        unique_payments = df['payment_type'].unique()
        dim_payment = pd.DataFrame({
            'payment_id': range(len(unique_payments)),
            'payment_name': unique_payments
        })
        
        # dim_passenger
        unique_passenger_counts = df['passenger_count'].unique()
        dim_passenger = pd.DataFrame({
            'passenger_id': range(len(unique_passenger_counts)),
            'passenger_count': unique_passenger_counts
        })
        
        # 3. Create fact table
        fact_trips = pd.DataFrame({
            'trip_id': range(len(df)),
            'datetime_id': dim_datetime['datetime_id'],
            'location_id': dim_location['location_id'],
            'payment_id': df['payment_type'].map(dict(zip(dim_payment['payment_name'], dim_payment['payment_id']))),
            'passenger_id': df['passenger_count'].map(dict(zip(dim_passenger['passenger_count'], dim_passenger['passenger_id']))),
            'trip_distance': df['trip_distance'],
            'trip_duration': df['trip_duration'],
            'fare_amount': df['fare_amount'],
            'tip_amount': df['tip_amount'],
            'total_amount': df['total_amount']
        })
        
        # 4. Load tables into DuckDB
        conn.execute('DROP TABLE IF EXISTS dim_datetime')
        conn.execute('DROP TABLE IF EXISTS dim_location')
        conn.execute('DROP TABLE IF EXISTS dim_payment')
        conn.execute('DROP TABLE IF EXISTS dim_passenger')
        conn.execute('DROP TABLE IF EXISTS fact_trips')
        
        conn.execute('CREATE TABLE dim_datetime AS SELECT * FROM dim_datetime')
        conn.execute('CREATE TABLE dim_location AS SELECT * FROM dim_location')
        conn.execute('CREATE TABLE dim_payment AS SELECT * FROM dim_payment')
        conn.execute('CREATE TABLE dim_passenger AS SELECT * FROM dim_passenger')
        conn.execute('CREATE TABLE fact_trips AS SELECT * FROM fact_trips')
        
        # 5. Create indexes for better query performance
        conn.execute('CREATE INDEX idx_datetime_id ON dim_datetime(datetime_id)')
        conn.execute('CREATE INDEX idx_location_id ON dim_location(location_id)')
        conn.execute('CREATE INDEX idx_payment_id ON dim_payment(payment_id)')
        conn.execute('CREATE INDEX idx_passenger_id ON dim_passenger(passenger_id)')
        conn.execute('CREATE INDEX idx_trip_id ON fact_trips(trip_id)')
        
        return fact_trips
        
    finally:
        conn.close()

@transformer
def validate_transformed_data(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Validate the transformed data
    """
    # Validate no null values in key fields
    key_fields = ['trip_id', 'datetime_id', 'location_id', 'payment_id', 'passenger_id']
    null_counts = df[key_fields].isnull().sum()
    if null_counts.any():
        raise ValueError(f"Found null values in key fields: {null_counts[null_counts > 0].to_dict()}")
    
    # Validate numeric ranges
    if (df['trip_distance'] < 0).any():
        raise ValueError("Found negative trip distances")
    if (df['trip_duration'] < 0).any():
        raise ValueError("Found negative trip durations")
    if (df['fare_amount'] < 0).any():
        raise ValueError("Found negative fare amounts")
    
    return df
