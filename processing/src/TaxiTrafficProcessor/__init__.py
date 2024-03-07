# First Level import :
from dataclasses import dataclass
import json 

# Second Level import :


# Third Level import :
from pyspark.rdd import RDD
from pyspark.sql.functions import *
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
    
    def __post_init__(self):
        """
        Post init method to initialize the class.
        """
        #TODO apply data preparation methods here.
        pass
    
    def get_data(self):
        """
        Method to get the data.
        """
        return self._data_rdd
    
    def process_data(self):
        """
        Method to process the data.
        """
        for json_obj in self._data_rdd.collect(): # Should be always 1 element in the RDD, unless received more than 1 message at once.
            obj_formatted = json.loads(json_obj)
            print(obj_formatted)
        pass