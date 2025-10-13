CREATE TABLE trips (
    id SERIAL PRIMARY KEY,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    avg_speed_kmh FLOAT,
    fare_per_km FLOAT
);

