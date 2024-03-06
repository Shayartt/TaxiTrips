# First level imports
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
# Second level imports
from .basic_generator import Generator

# Third Party imports
import pandas as pd

@dataclass
class DummyGenerator(Generator):
    """
    Dummy data generator class.
    
    Attributes
    ----------
    id : int
        The id of the instance.
    is_ready : bool
        The readiness of the data.
    
    """
    
    __generator_type: str = "dummy"
    
    def __str__(self) -> str:
        """
        String representation of the class.
        
        :return: The string representation.
        :rtype: str
        """
        return f"DummyGenerator(id={self._id}, is_ready={self.is_ready})"
    
    def generate(self, source_data: pd.DataFrame) -> dict:
        """
        Generate method.
        
        :return: The generated data.
        :rtype: dict
        """
        # Generate a random number between 1 and 22 and used it as trip perdiod in minutes.
        random_number = random.randint(1, 22)
        tmp = {"tpep_dropoff_datetime" : datetime.now(), "tpep_pickup_datetime" : datetime.now() - timedelta(minutes=random_number)}
        
        # For each column in our source_data except : tpep_dropoff_datetime, we will generate a random value based on values in our source_data : 
        for col in source_data.columns :
            if col not in ("tpep_dropoff_datetime","tpep_pickup_datetime") :
                tmp[col] = random.choice(source_data[col].values)
        
        # Set the readiness to True
        self.is_ready = True
        
        self.data = tmp # Save the generated data
        return tmp
