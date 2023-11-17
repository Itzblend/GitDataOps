CREATE DATABASE taxi;

\c taxi

CREATE TABLE taxi_trips (
    VendorID TEXT,
    tpep_pickup_datetime TEXT,
    tpep_dropoff_datetime TEXT,
    passenger_count TEXT,
    trip_distance TEXT,
    RatecodeID TEXT,
    store_and_fwd_flag TEXT,
    PULocationID TEXT,
    DOLocationID TEXT,
    payment_type TEXT,
    fare_amount TEXT,
    extra TEXT,
    mta_tax TEXT,
    tip_amount TEXT,
    tolls_amount TEXT,
    improvement_surcharge TEXT,
    total_amount TEXT,
    congestion_surcharge TEXT,
    airport_fee TEXT,
    PRIMARY KEY (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, total_amount)
);

COPY taxi_trips FROM '/data/yellow_tripdata_sample.csv' WITH (FORMAT CSV, HEADER TRUE);
