# First Level import :
from dataclasses import dataclass
import json 

# Second Level import :


# Third Level import :
from pyspark.rdd import RDD
from pyspark.broadcast import Broadcast
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import *
import pandas as pd 

"""

TaxiTrafficProcessor

"""

# Note : Here we won't be using any abc classes because the whole project is about taxi traffic processing,
# if we want to scale into different inputs sources, we may want to implement an interface of processing and then 
# enherit from it to implement the different processing classes. and this would be an example of the open/closed principle.

@dataclass
class TaxiTrafficProcessor:
    """
    This Class will be responsible for processing the data of the taxi traffic received on the streaming server.
    """
    _data_df: SparkDataFrame
    zone_brodcast: Broadcast

    def __post_init__(self):
        """
        Post init method to initialize the class. This will automatically process the data and broadcast the zone matrix
        after the class is initialized.
        """
        self.process_data()
        self.zone_brodcast = broadcast(self.zone_brodcast)

    def get_data(self):
        """
        Method to get the data.
        """
        return self._data_df

    def pipeline_01(self) -> bool:
        """
        Method to apply the first pipeline of the data.

        Apply Pipeline 01 over _data_df and return the status of the operation.

        1- Get Pickup/Drop Zone names from id.
        2- Compute trip duration.

        """
        # Alias for Pickup Zone Name
        pu_zone_df = self.zone_brodcast.withColumnRenamed("LocationID", "PULocationID") \
                                        .withColumnRenamed("Zone", "PULocationName")

        # Alias for Drop Zone Name
        do_zone_df = self.zone_brodcast.withColumnRenamed("LocationID", "DOLocationID") \
                                        .withColumnRenamed("Zone", "DOLocationName")

        # Join with Pickup Zone Name
        self._data_df = self._data_df.join(pu_zone_df, self._data_df["PULocationID"] == pu_zone_df["PULocationID"], "left_outer")

        # Join with Drop Zone Name
        self._data_df = self._data_df.join(do_zone_df, self._data_df["DOLocationID"] == do_zone_df["DOLocationID"], "left_outer")

        # Fix the data type of the columns :
        self._data_df = self._data_df.withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        self._data_df = self._data_df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
        
        # Compute trip duration:
        self._data_df = self._data_df.withColumn("trip_duration", (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60)
        
        return True
    
    def process_data(self):
        """
        Method to process the data.
        """
        # Apply pipeline 01 :
        self.pipeline_01()
        
            
        pass
            
            
        