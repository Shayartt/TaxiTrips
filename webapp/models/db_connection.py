from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import os
import sys
from typing import Dict, List


# Second Level import :
current_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(current_dir, "..", "..")) # Coudln't figure out a better way ... unless I publish the package.
sys.path.append(parent_dir)

from tools.CassandraHandler import CassandraHandler

class CassandraReader(CassandraHandler):
    """
    This class will be responsible for writing the data into Cassandra.
    """
    
    def __init__(self) -> None: 
        """
        Constructor will start the connection to Cassandra using signv4 plugins.
        
        """
        super().__init__()
    
    def read_from_cassandra(self, query: str) -> List[Dict[str, str]]:
        """
        Read data from Cassandra.
        
        Parameters:
        query : str : the query to be executed.
        
        Returns:
        data : List[Dict[str, str]] : The data read from Cassandra.
        
        """
        # Read data from Keyspaces system table.  This table   
        r = self.session.execute(query)
        
        return r
    