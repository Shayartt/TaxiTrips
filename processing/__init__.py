# First Level import :
import os 
import json
from dotenv import load_dotenv
import uuid 
import time 

# Second Level import :
from src.CassandraHandler import CassandraWriter
from src.TaxiTrafficProcessor import TaxiTrafficProcessor
from src.TaxiTrafficProcessor.schema import schema as source_input_schema

# Third Level import :
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.broadcast import Broadcast
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import pandas as pd 


load_dotenv()

def process_stream(spark_df: SparkDataFrame, cd_writer: CassandraWriter, zone_brodcast: SparkDataFrame) -> None : 
    """
    This function will be responsible for processing the data of the taxi traffic received on the streaming server.
    """
    # Capture the start time
    start_time = time.perf_counter()
    
    # Init TaxiTrafficProcessor :
    taxi_traffic_processor = TaxiTrafficProcessor(spark_df, zone_brodcast)
    
    # Insert Data into Cassandra :
    cd_writer.write_to_cassandra(taxi_traffic_processor.get_data())
    
    # Capture the end time
    end_time = time.perf_counter()
    
    # Calculate the duration
    duration = end_time - start_time
    print(f"Batch execution time: {duration} seconds")
    pass
    

if __name__ == "__main__":
    # Create a local SparkContext with two working threads and a batch interval of 1 second
    sc = SparkContext("local[2]", "TrafficStreamingApp")
    
    spark = SparkSession.builder.appName("TrafficStreamingApp")\
            .getOrCreate()
            
    # Set the logging level to ERROR (or any other desired level)
    spark.sparkContext.setLogLevel("ERROR")
    
    input_directory = "file://" + os.environ['STREAMING_FILES_PATH'] 
    
    # Generated streaming session id : 
    streaming_process_id = str(uuid.uuid4())
    
    # Load Zone lookup dictionary :
    zone_naming = spark.read.csv("file://" + os.environ['ZONE_LOOKUP_PATH'], header=True, inferSchema=True)
    zone_naming = zone_naming.select("LocationID", "Zone") # We don't care about rest of columns.
    
    # Init Database Writer:
    cassandra_writer = CassandraWriter()
        
    # Create a TextStream
    df_stream = spark \
        .readStream \
        .schema(source_input_schema) \
        .option("latestFirst", "true") \
        .format("json") \
        .load(f"{input_directory}/*.json")
    
    query = df_stream \
        .writeStream \
        .foreachBatch(lambda df, epoch_id: process_stream(df, cassandra_writer, zone_naming)) \
        .start()

    query.awaitTermination()
