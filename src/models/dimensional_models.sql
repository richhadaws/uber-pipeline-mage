
-- Create dimension tables
CREATE TABLE IF NOT EXISTS dim_datetime (
    datetime_id INTEGER PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    pickup_hour INTEGER,
    pickup_day INTEGER,
    pickup_month INTEGER,
    pickup_year INTEGER,
    pickup_weekday INTEGER,
    dropoff_datetime TIMESTAMP,
    dropoff_hour INTEGER,
    dropoff_day INTEGER,
    dropoff_month INTEGER,
    dropoff_year INTEGER,
    dropoff_weekday INTEGER
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY,
    pickup_latitude DOUBLE,
    pickup_longitude DOUBLE,
    dropoff_latitude DOUBLE,
    dropoff_longitude DOUBLE,
    pickup_location_name VARCHAR,
    dropoff_location_name VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_payment (
    payment_id INTEGER PRIMARY KEY,
    payment_type VARCHAR,
    payment_name VARCHAR,
    payment_description VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_passenger (
    passenger_id INTEGER PRIMARY KEY,
    passenger_count INTEGER
);

-- Create fact table
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_id INTEGER PRIMARY KEY,
    datetime_id INTEGER REFERENCES dim_datetime(datetime_id),
    location_id INTEGER REFERENCES dim_location(location_id),
    payment_id INTEGER REFERENCES dim_payment(payment_id),
    passenger_id INTEGER REFERENCES dim_passenger(passenger_id),
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    tip_amount DOUBLE,
    total_amount DOUBLE,
    trip_duration INTEGER,
    FOREIGN KEY (datetime_id) REFERENCES dim_datetime(datetime_id),
    FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
    FOREIGN KEY (payment_id) REFERENCES dim_payment(payment_id),
    FOREIGN KEY (passenger_id) REFERENCES dim_passenger(passenger_id)
);

-- Create analytics views
CREATE VIEW IF NOT EXISTS vw_hourly_fares AS
SELECT 
    d.pickup_hour,
    AVG(f.fare_amount) as avg_fare,
    COUNT(*) as num_trips
FROM fact_trips f
JOIN dim_datetime d ON f.datetime_id = d.datetime_id
GROUP BY d.pickup_hour
ORDER BY d.pickup_hour;

CREATE VIEW IF NOT EXISTS vw_popular_locations AS
SELECT 
    l.pickup_latitude,
    l.pickup_longitude,
    COUNT(*) as num_pickups,
    AVG(f.fare_amount) as avg_fare
FROM fact_trips f
JOIN dim_location l ON f.location_id = l.location_id
GROUP BY l.pickup_latitude, l.pickup_longitude
ORDER BY num_pickups DESC;

CREATE VIEW IF NOT EXISTS vw_payment_analysis AS
SELECT 
    p.payment_name,
    COUNT(*) as num_trips,
    AVG(f.fare_amount) as avg_fare,
    AVG(f.tip_amount) as avg_tip
FROM fact_trips f
JOIN dim_payment p ON f.payment_id = p.payment_id
GROUP BY p.payment_name;