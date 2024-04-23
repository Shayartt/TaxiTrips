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
        user_id = random.randint(1, 100)
        tmp = {"user_id" : user_id,"generated_id": self._id, "tpep_dropoff_datetime" : datetime.now().isoformat(), "tpep_pickup_datetime" : (datetime.now() - timedelta(minutes=random_number)).isoformat() }
        
        # For each column in our source_data except : tpep_dropoff_datetime, we will generate a random value based on values in our source_data : 
        for col in source_data.columns :
            if col not in ("tpep_dropoff_datetime","tpep_pickup_datetime") :
                random_value = random.choice(source_data[col].values)
                
                # Convert int64 & float64 to int and float
                if source_data[col].dtype == "int64" :
                    random_value = int(random_value)
                elif source_data[col].dtype == "float64" :
                    random_value = float(random_value)
                    
                # Append to our tmp dict
                tmp[col] = random_value
        
        # Set the readiness to True
        self.is_ready = True
        
        # tmp['total_amount'] = 989742 # To test alerts
        self.data = tmp # Save the generated data
        return tmp
