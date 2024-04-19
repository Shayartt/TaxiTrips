
CREATE TABLE traffic_db.streaming_record (
    user_id int,
    DOLocationID int,
    PULocationID int,
    RatecodeID int,
    VendorID int,
    congestion_surcharge float,
    extra float,
    fare_amount float,
    generated_id text,
    improvement_surcharge float,
    mta_tax float,
    passenger_count int,
    payment_type int,
    platform text,
    room text,
    stage text,
    store_and_fwd_flag text,
    tip_amount float,
    tolls_amount float,
    total_amount float,
    tpep_dropoff_datetime timestamp,
    tpep_pickup_datetime timestamp,
    trip_distance float,
    PULocationName text,
    DOLocationName text,
    trip_duration float,
    PRIMARY KEY (user_id, DOLocationID, PULocationID)
);