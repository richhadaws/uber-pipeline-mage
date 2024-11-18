import duckdb
import pandas as pd
from pathlib import Path
import logging
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_to_duckdb():
    """Create or connect to DuckDB database"""
    return duckdb.connect('uber_rides.db')

def create_analytics_views():
    """Create views for common analytics queries"""
    conn = connect_to_duckdb()
    
    try:
        # Average fare by hour of day
        conn.execute("""
            CREATE OR REPLACE VIEW vw_hourly_fares AS
            SELECT 
                d.pickup_hour,
                AVG(f.fare_amount) as avg_fare,
                COUNT(*) as num_trips,
                AVG(f.tip_amount) as avg_tip,
                AVG(f.total_amount) as avg_total
            FROM fact_trips f
            JOIN dim_datetime d ON f.datetime_id = d.datetime_id
            GROUP BY d.pickup_hour
            ORDER BY d.pickup_hour
        """)
        logger.info("Created hourly fares view")

        # Popular pickup locations
        conn.execute("""
            CREATE OR REPLACE VIEW vw_popular_locations AS
            SELECT 
                l.pickup_latitude,
                l.pickup_longitude,
                COUNT(*) as num_pickups,
                AVG(f.fare_amount) as avg_fare,
                AVG(f.trip_distance) as avg_distance,
                AVG(f.trip_duration) as avg_duration
            FROM fact_trips f
            JOIN dim_location l ON f.location_id = l.location_id
            GROUP BY l.pickup_latitude, l.pickup_longitude
            ORDER BY num_pickups DESC
        """)
        logger.info("Created popular locations view")

        # Payment type analysis
        conn.execute("""
            CREATE OR REPLACE VIEW vw_payment_analysis AS
            SELECT 
                p.payment_name,
                COUNT(*) as num_trips,
                AVG(f.fare_amount) as avg_fare,
                AVG(f.tip_amount) as avg_tip,
                AVG(f.total_amount) as avg_total,
                AVG(f.trip_distance) as avg_distance
            FROM fact_trips f
            JOIN dim_payment p ON f.payment_id = p.payment_id
            GROUP BY p.payment_name
        """)
        logger.info("Created payment analysis view")

        # Daily statistics
        conn.execute("""
            CREATE OR REPLACE VIEW vw_daily_stats AS
            SELECT 
                d.pickup_year,
                d.pickup_month,
                d.pickup_day,
                COUNT(*) as num_trips,
                AVG(f.fare_amount) as avg_fare,
                SUM(f.total_amount) as total_revenue,
                AVG(f.trip_distance) as avg_distance,
                AVG(f.trip_duration) as avg_duration
            FROM fact_trips f
            JOIN dim_datetime d ON f.datetime_id = d.datetime_id
            GROUP BY d.pickup_year, d.pickup_month, d.pickup_day
            ORDER BY d.pickup_year, d.pickup_month, d.pickup_day
        """)
        logger.info("Created daily statistics view")

    except Exception as e:
        logger.error(f"Error creating analytics views: {str(e)}")
        raise

def export_analytics(output_dir: str = "data/processed"):
    """Export analytics views to CSV files"""
    conn = connect_to_duckdb()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # List of views to export
        views = [
            'vw_hourly_fares',
            'vw_popular_locations',
            'vw_payment_analysis',
            'vw_daily_stats'
        ]
        
        # Export each view
        for view in views:
            df = conn.execute(f"SELECT * FROM {view}").fetchdf()
            csv_path = output_path / f"{view}.csv"
            df.to_csv(csv_path, index=False)
            logger.info(f"Exported {view} to {csv_path}")
            
            # Generate basic statistics
            stats = {
                'row_count': len(df),
                'columns': list(df.columns),
                'null_counts': df.isnull().sum().to_dict(),
                'numeric_columns': {
                    col: {
                        'min': float(df[col].min()),
                        'max': float(df[col].max()),
                        'mean': float(df[col].mean()),
                        'median': float(df[col].median())
                    }
                    for col in df.select_dtypes(include=['number']).columns
                }
            }
            
            # Save statistics
            stats_path = output_path / f"{view}_stats.json"
            with open(stats_path, 'w') as f:
                json.dump(stats, f, indent=2, default=str)
            logger.info(f"Exported statistics for {view} to {stats_path}")
    
    except Exception as e:
        logger.error(f"Error exporting analytics: {str(e)}")
        raise
    finally:
        conn.close()

def generate_summary_report(output_dir: str = "data/processed"):
    """Generate a summary report of the data"""
    conn = connect_to_duckdb()
    output_path = Path(output_dir)
    
    try:
        summary = {}
        
        # Total trips and revenue
        result = conn.execute("""
            SELECT 
                COUNT(*) as total_trips,
                SUM(total_amount) as total_revenue,
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration)/60 as avg_duration_minutes
            FROM fact_trips
        """).fetchone()
        
        summary['overall_stats'] = {
            'total_trips': result[0],
            'total_revenue': round(float(result[1]), 2),
            'avg_distance': round(float(result[2]), 2),
            'avg_duration_minutes': round(float(result[3]), 2)
        }
        
        # Payment type distribution
        summary['payment_distribution'] = conn.execute("""
            SELECT 
                p.payment_name,
                COUNT(*) as trip_count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_trips), 2) as percentage
            FROM fact_trips f
            JOIN dim_payment p ON f.payment_id = p.payment_id
            GROUP BY p.payment_name
            ORDER BY trip_count DESC
        """).fetchdf().to_dict('records')
        
        # Save summary report
        summary_path = output_path / "summary_report.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Generated summary report at {summary_path}")
        
    except Exception as e:
        logger.error(f"Error generating summary report: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    """Execute all loading steps"""
    try:
        create_analytics_views()
        export_analytics()
        generate_summary_report()
        logger.info("All loading steps completed successfully")
    except Exception as e:
        logger.error(f"Loading process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()