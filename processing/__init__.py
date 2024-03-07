# First Level import :
import os 
import json
from dotenv import load_dotenv
import uuid 
import time 

# Second Level import :
from src.CassandraHandler import CassandraWriter
from src.TaxiTrafficProcessor import TaxiTrafficProcessor

# Third Level import :
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import pandas as pd 


load_dotenv()

def process_stream(rdd: RDD, cd_writer: CassandraWriter, zones_df: pd.DataFrame) -> None : 
    """
    This function will be responsible for processing the data of the taxi traffic received on the streaming server.
    """
    # Capture the start time
    start_time = time.perf_counter()
    
    # Init TaxiTrafficProcessor :
    taxi_traffic_processor = TaxiTrafficProcessor(rdd, zones_df)
    
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
    ssc = StreamingContext(sc, 1)
    
    spark = SparkSession.builder.appName("TrafficStreamingApp")\
            .getOrCreate()
            
    # Set the logging level to ERROR (or any other desired level)
    spark.sparkContext.setLogLevel("ERROR")
    
    input_directory = "file://" + os.environ['STREAMING_FILES_PATH'] 
    
    # Generated streaming session id : 
    streaming_process_id = str(uuid.uuid4())
    
    # Load Zone lookup dict, note : I used pandas here, because the whole file is small enough and never changed then the best option is to use pandas DF
    df_zones = pd.read_csv(os.environ['ZONE_LOOKUP_PATH'])
    
    # Init Database Writer:
    cassandra_writer = CassandraWriter()
    
    # Create a TextStream
    lines = ssc.textFileStream(input_directory)
    
    # Process each RDD : 
    lines.foreachRDD(lambda rdd: process_stream(rdd, cassandra_writer, df_zones))
    
    # Start the streaming computation
    ssc.start()

    # Wait for the streaming computation to finish
    ssc.awaitTermination()