from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import os
import sys
from typing import Dict, List
from opensearchpy import OpenSearch, helpers
import pandas as pd 

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
    
class OpenSearchReader():
    """
    This class will be responsible for writing the data into OpenSearch.
    """
    
    def __init__(self) -> None: 
        """
        Constructor will start the connection to OpenSearch.
        
        """
        opensearch_host = os.environ.get("OPENSEARCH_HOST", "localhost")
        opensearch_username = os.environ.get("OPENSEARCH_USER", "root")
        opensearch_password = os.environ.get("OPENSEARCH_PW", "passport")

        # Create an OpenSearch client
        self.opensearch_client = OpenSearch(opensearch_host, http_auth=(opensearch_username, opensearch_password))
    
    def read_from_opensearch(self, index_name: str, query: str) -> List[Dict[str, str]]:
        """
        Read data from OpenSearch.
        
        Parameters:
        query : str : the query to be executed.
        
        Returns:
        data : List[Dict[str, str]] : The data read from OpenSearch.
        
        """
        # Read data from OpenSearch
        response = self.opensearch_client.search(
            body = query,
            index = index_name,
            size = 10000 # In a production project, you would use .scroll instead to load all variable, for us we only need top 10k it's totally fine.
        )

        # Get the hits from the response, and format them into a list of dictionaries
        hits = response['hits']['hits']
        result = [ 
                        {
                            **hit['_source']
                        } for hit in hits]

        df = pd.DataFrame(result)
        return df