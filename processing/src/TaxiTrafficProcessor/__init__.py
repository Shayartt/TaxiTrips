# First Level import :
from dataclasses import dataclass
import json 

# Second Level import :


# Third Level import :
from pyspark.rdd import RDD
from pyspark.sql.functions import *
import pandas as pd 

"""

TaxiTrafficProcessor

"""

# Note : Here we won't be using any abc classes because the whole project is about taxi traffic processing,
# if we want to scale into different inputs sources, we may want to implement an interface of processing and then 
# enherit from it to implement the different processing classes. and this would be an example of the open/closed principle.

@dataclass
class TaxiTrafficProcessor :
    """
    This Class will be responsible for processing the data of the taxi traffic received on the streaming server.
    """
    _data_rdd : RDD
    zone_df: pd.DataFrame
    _data : List[dict] = None
    
    def __post_init__(self):
        """
        Post init method to initialize the class. this will automatically process the data 
        after the class is initialized.
        """
        self.process_data()
    
    def get_data(self):
        """
        Method to get the data.
        """
        return self._data
    
    def pipeline_01(self, obj_formatted: dict) -> dict:
        """
        Method to apply the first pipeline of the data.
        
        :param obj_formatted: The formatted object to apply the pipeline on.
        :return: The formatted object after applying the pipeline.
        
        1- Get Pickup/Drop Zone names from id.\n
        2- Compute trip duration.
        
        """
        # TODO, we can use a map here to avoid the for loop.
        
        # 1- Pickup Zone :
        obj_formatted["PULocationID"] = self.zone_df[self.zone_df['LocationID'] == obj_formatted['PULocationID']].to_dict('records')[0]
        obj_formatted["DOLocationID"]  = self.zone_df[self.zone_df['LocationID'] == obj_formatted['DOLocationID']].to_dict('records')[0]

        # 2- Compute trip duration :
        obj_formatted["trip_duration"] = obj_formatted["tpep_dropoff_datetime"] - obj_formatted["tpep_pickup_datetime"]
        
        return obj_formatted
    
    def process_data(self):
        """
        Method to process the data.
        """
        # TODO : We can use a map here to avoid the for loop.
        for json_obj in self._data_rdd.collect(): # Should be always 1 element in the RDD, unless received more than 1 message at once.
            obj_formatted = json.loads(json_obj)
            print(obj_formatted)
            
            # Call Pipelines :
            obj_formatted = self.pipeline_01(obj_formatted)
            
            # Append to the data :
            self._data.append(obj_formatted)
            
            
        