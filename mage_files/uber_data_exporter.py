from mage_ai.data_preparation.decorators import data_exporter
from mage_ai.data_preparation.utils import get_repo_path
from pandas import DataFrame
from pathlib import Path
import duckdb
import json

@data_exporter
def export_data_to_files(df: DataFrame, *args, **kwargs):
    """
    Export processed data and analytics views to files
    """
    # Setup paths
    repo_path = Path(get_repo_path())
    output_dir = repo_path / 'data' / 'processed'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB
    conn = duckdb.connect(str(repo_path / 'uber_rides.db'))
    
    try:
        # Export analytics views
        views = [
            'vw_hourly_fares',
            'vw_popular_locations',
            'vw_payment_analysis',
            'vw_daily_stats'
        ]
        
        analytics_data = {}
        
        for view in views:
            try:
                # Export view to CSV
                df = conn.execute(f"SELECT * FROM {view}").fetchdf()
                csv_path = output_dir / f"{view}.csv"
                df.to_csv(csv_path, index=False)
                
                # Generate statistics
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
                
                analytics_data[view] = stats
                
            except Exception as e:
                print(f"Error exporting {view}: {str(e)}")
        
        # Generate summary report
        summary = {}
        
        # Overall statistics
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
        
        # Payment distribution
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
        
        # Save analytics data
        with open(output_dir / 'analytics_stats.json', 'w') as f:
            json.dump(analytics_data, f, indent=2)
        
        # Save summary report
        with open(output_dir / 'summary_report.json', 'w') as f:
            json.dump(summary, f, indent=2)
            
    finally:
        conn.close()
