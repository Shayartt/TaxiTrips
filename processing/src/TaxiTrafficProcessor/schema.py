from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, LongType, DateType

# Define schema for the incoming JSON data
schema = StructType() \
    .add("DOLocationID", LongType()) \
    .add("PULocationID", LongType()) \
    .add("RatecodeID", LongType()) \
    .add("VendorID", LongType()) \
    .add("congestion_surcharge", FloatType()) \
    .add("extra", FloatType()) \
    .add("fare_amount", FloatType()) \
    .add("generated_id", StringType()) \
    .add("improvement_surcharge", FloatType()) \
    .add("mta_tax", FloatType()) \
    .add("passenger_count", LongType()) \
    .add("payment_type", LongType()) \
    .add("platform", StringType()) \
    .add("room", StringType()) \
    .add("stage", StringType()) \
    .add("store_and_fwd_flag", StringType()) \
    .add("tip_amount", FloatType()) \
    .add("tolls_amount", FloatType()) \
    .add("total_amount", FloatType()) \
    .add("tpep_dropoff_datetime", StringType()) \
    .add("tpep_pickup_datetime", StringType()) \
    .add("trip_distance", FloatType())