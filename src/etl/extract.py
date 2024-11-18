import duckdb
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_duckdb():
    """Create or connect to DuckDB database"""
    return duckdb.connect('uber_rides.db')

def initialize_database(conn):
    """Initialize database with schema"""
    try:
        with open('src/models/dimensional_models.sql', 'r') as f:
            sql_commands = f.read()
            # Split commands by semicolon and execute each
            for command in sql_commands.split(';'):
                if command.strip():
                    conn.execute(command)
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def load_csv_to_duckdb(file_path: str, table_name: str):
    """Load CSV file into DuckDB staging table"""
    conn = connect_to_duckdb()
    
    try:
        # Create staging table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS staging_{table_name} AS 
            SELECT * FROM read_csv_auto('{file_path}')
        """)
        
        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM staging_{table_name}").fetchone()[0]
        logger.info(f"Successfully loaded {row_count} rows into staging_{table_name}")
        
        # Initialize schema
        initialize_database(conn)
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        conn.close()

def validate_data(file_path: Path):
    """Validate the input data file"""
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found at {file_path}")
    
    try:
        # Read first few rows to validate structure
        df = pd.read_csv(file_path, nrows=5)
        required_columns = [
            'pickup_datetime', 'dropoff_datetime',
            'pickup_latitude', 'pickup_longitude',
            'dropoff_latitude', 'dropoff_longitude',
            'passenger_count', 'trip_distance',
            'fare_amount', 'tip_amount', 'total_amount',
            'payment_type'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        logger.info("Data validation successful")
        
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise

def main():
    """Execute the extraction process"""
    try:
        # Path to your data file
        data_path = Path("data/raw/uber_data.csv")
        
        # Validate data
        validate_data(data_path)
        
        # Load data into staging table
        load_csv_to_duckdb(str(data_path), "uber_rides")
        
        logger.info("Extraction process completed successfully")
        
    except Exception as e:
        logger.error(f"Extraction process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()