import duckdb
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_duckdb():
    """Create or connect to DuckDB database"""
    return duckdb.connect('uber_rides.db')

def transform_datetime_dimension():
    """Transform and load datetime dimension"""
    conn = connect_to_duckdb()
    
    try:
        conn.execute("""
            INSERT INTO dim_datetime (
                datetime_id,
                pickup_datetime,
                pickup_hour,
                pickup_day,
                pickup_month,
                pickup_year,
                pickup_weekday,
                dropoff_datetime,
                dropoff_hour,
                dropoff_day,
                dropoff_month,
                dropoff_year,
                dropoff_weekday
            )
            SELECT 
                ROW_NUMBER() OVER () as datetime_id,
                pickup_datetime,
                EXTRACT(HOUR FROM pickup_datetime) as pickup_hour,
                EXTRACT(DAY FROM pickup_datetime) as pickup_day,
                EXTRACT(MONTH FROM pickup_datetime) as pickup_month,
                EXTRACT(YEAR FROM pickup_datetime) as pickup_year,
                EXTRACT(DOW FROM pickup_datetime) as pickup_weekday,
                dropoff_datetime,
                EXTRACT(HOUR FROM dropoff_datetime) as dropoff_hour,
                EXTRACT(DAY FROM dropoff_datetime) as dropoff_day,
                EXTRACT(MONTH FROM dropoff_datetime) as dropoff_month,
                EXTRACT(YEAR FROM dropoff_datetime) as dropoff_year,
                EXTRACT(DOW FROM dropoff_datetime) as dropoff_weekday
            FROM staging_uber_rides
        """)
        logger.info("Successfully transformed datetime dimension")
    except Exception as e:
        logger.error(f"Error transforming datetime dimension: {str(e)}")
        raise
    finally:
        conn.close()

def transform_location_dimension():
    """Transform and load location dimension"""
    conn = connect_to_duckdb()
    
    try:
        conn.execute("""
            INSERT INTO dim_location (
                location_id,
                pickup_latitude,
                pickup_longitude,
                dropoff_latitude,
                dropoff_longitude,
                pickup_location_name,
                dropoff_location_name
            )
            SELECT 
                ROW_NUMBER() OVER () as location_id,
                pickup_latitude,
                pickup_longitude,
                dropoff_latitude,
                dropoff_longitude,
                'NYC Area' as pickup_location_name,  -- Could be enhanced with geocoding
                'NYC Area' as dropoff_location_name  -- Could be enhanced with geocoding
            FROM staging_uber_rides
            GROUP BY 
                pickup_latitude,
                pickup_longitude,
                dropoff_latitude,
                dropoff_longitude
        """)
        logger.info("Successfully transformed location dimension")
    except Exception as e:
        logger.error(f"Error transforming location dimension: {str(e)}")
        raise
    finally:
        conn.close()

def transform_payment_dimension():
    """Transform and load payment dimension"""
    conn = connect_to_duckdb()
    
    try:
        conn.execute("""
            INSERT INTO dim_payment (
                payment_id,
                payment_type,
                payment_name,
                payment_description
            )
            SELECT 
                ROW_NUMBER() OVER () as payment_id,
                payment_type,
                CASE payment_type
                    WHEN 1 THEN 'Credit Card'
                    WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No Charge'
                    WHEN 4 THEN 'Dispute'
                    ELSE 'Unknown'
                END as payment_name,
                CASE payment_type
                    WHEN 1 THEN 'Payment by credit card'
                    WHEN 2 THEN 'Cash payment'
                    WHEN 3 THEN 'Free ride'
                    WHEN 4 THEN 'Disputed charge'
                    ELSE 'Unknown payment type'
                END as payment_description
            FROM staging_uber_rides
            GROUP BY payment_type
        """)
        logger.info("Successfully transformed payment dimension")
    except Exception as e:
        logger.error(f"Error transforming payment dimension: {str(e)}")
        raise
    finally:
        conn.close()

def transform_passenger_dimension():
    """Transform and load passenger dimension"""
    conn = connect_to_duckdb()
    
    try:
        conn.execute("""
            INSERT INTO dim_passenger (
                passenger_id,
                passenger_count
            )
            SELECT 
                ROW_NUMBER() OVER () as passenger_id,
                passenger_count
            FROM staging_uber_rides
            GROUP BY passenger_count
        """)
        logger.info("Successfully transformed passenger dimension")
    except Exception as e:
        logger.error(f"Error transforming passenger dimension: {str(e)}")
        raise
    finally:
        conn.close()

def transform_fact_table():
    """Transform and load fact table"""
    conn = connect_to_duckdb()
    
    try:
        conn.execute("""
            INSERT INTO fact_trips (
                trip_id,
                datetime_id,
                location_id,
                payment_id,
                passenger_id,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount,
                trip_duration
            )
            SELECT 
                ROW_NUMBER() OVER () as trip_id,
                d.datetime_id,
                l.location_id,
                p.payment_id,
                ps.passenger_id,
                s.trip_distance,
                s.fare_amount,
                s.tip_amount,
                s.total_amount,
                EXTRACT(EPOCH FROM (s.dropoff_datetime - s.pickup_datetime)) as trip_duration
            FROM staging_uber_rides s
            JOIN dim_datetime d ON 
                d.pickup_datetime = s.pickup_datetime AND
                d.dropoff_datetime = s.dropoff_datetime
            JOIN dim_location l ON 
                l.pickup_latitude = s.pickup_latitude AND
                l.pickup_longitude = s.pickup_longitude AND
                l.dropoff_latitude = s.dropoff_latitude AND
                l.dropoff_longitude = s.dropoff_longitude
            JOIN dim_payment p ON p.payment_type = s.payment_type
            JOIN dim_passenger ps ON ps.passenger_count = s.passenger_count
        """)
        logger.info("Successfully transformed fact table")
    except Exception as e:
        logger.error(f"Error transforming fact table: {str(e)}")
        raise
    finally:
        conn.close()

def validate_transformations():
    """Validate the transformed data"""
    conn = connect_to_duckdb()
    
    try:
        # Check for orphaned records
        orphaned_check = """
            SELECT 'fact_trips' as table_name, COUNT(*) as orphaned_records
            FROM fact_trips f
            LEFT JOIN dim_datetime d ON f.datetime_id = d.datetime_id
            WHERE d.datetime_id IS NULL
            UNION ALL
            SELECT 'fact_trips', COUNT(*)
            FROM fact_trips f
            LEFT JOIN dim_location l ON f.location_id = l.location_id
            WHERE l.location_id IS NULL
        """
        
        results = conn.execute(orphaned_check).fetchall()
        for table, count in results:
            if count > 0:
                raise ValueError(f"Found {count} orphaned records in {table}")
        
        logger.info("Data validation successful")
        
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    """Execute all transformation steps"""
    try:
        transform_datetime_dimension()
        transform_location_dimension()
        transform_payment_dimension()
        transform_passenger_dimension()
        transform_fact_table()
        validate_transformations()
        logger.info("All transformations completed successfully")
    except Exception as e:
        logger.error(f"Transformation process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()