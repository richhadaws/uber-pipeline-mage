# Uber Data Pipeline Project

## Overview
This project implements a data pipeline for processing Uber trip data using DuckDB for storage and Mage.ai for orchestration. The pipeline transforms raw data into a dimensional model for efficient analytics.

## Architecture
- **Storage**: DuckDB (lightweight, analytical database)
- **Orchestration**: Mage.ai
- **Data Model**: Star Schema with Fact and Dimension tables

## Data Model
### Fact Table
- fact_trips
  - Trip metrics (distance, duration, fare)
  - Foreign keys to dimension tables

### Dimension Tables
- dim_datetime
  - Date and time attributes
- dim_location
  - Pickup and dropoff locations
- dim_payment
  - Payment type and details
- dim_passenger
  - Passenger-related information

## Project Structure
```
uber-data-pipeline/
├── data/
│   ├── raw/         # Raw CSV files
│   └── processed/   # Processed data
├── src/
│   ├── etl/         # ETL scripts
│   └── models/      # SQL models
└── mage_files/      # Mage pipeline configs
```

## Setup
1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Place your Uber data CSV file in the `data/raw` directory as `uber_data.csv`

3. Run the pipeline:
```bash
# Initialize the database and load data
python src/etl/extract.py

# Transform data into dimensional model
python src/etl/transform.py

# Create analytics views and export data
python src/etl/load.py
```

## Analytics Views
The pipeline creates several analytics views:
1. **Hourly Fares** (vw_hourly_fares)
   - Average fare by hour of day
   - Trip count by hour

2. **Popular Locations** (vw_popular_locations)
   - Popular pickup locations
   - Average fares by location

3. **Payment Analysis** (vw_payment_analysis)
   - Trip counts by payment type
   - Average fares and tips by payment method

## Data Flow
1. Raw CSV data is loaded into DuckDB staging table
2. Data is transformed into dimension tables:
   - DateTime dimension with pickup/dropoff time attributes
   - Location dimension with pickup/dropoff coordinates
   - Payment dimension with payment types
   - Passenger dimension with passenger counts
3. Fact table is populated with metrics and foreign keys
4. Analytics views are created for common queries
5. Processed data is exported to CSV files

## Future Enhancements
1. Add data quality checks
2. Implement incremental loading
3. Add geographic enrichment for locations
4. Create visualization dashboard