from typing import Dict, List
from pyspark.sql import DataFrame as SparkDataFrame

# Second Level import :
from tools.CassandraHandler.sigv4.KeyspacesRetryPolicy import KeyspacesRetryPolicy

# Libraries to connect into AWS Keyspace : 
import os
import boto3
import ssl
import sys
from boto3 import Session
from cassandra_sigv4.auth import AuthProvider, Authenticator, SigV4AuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

# We won't be using ABC here because sub-classes won't need to use all the methods.
class CassandraHandler():
    """
    This class will be responsible for managing connection with Cassandra.
    """
    
    def __init__(self) -> None: 
        """
        Constructor will start the connection to Cassandra using signv4 plugins.
        
        
        """
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        cert_path = os.path.join(os.path.dirname(__file__), 'resources/sf-class2-root.crt')
        ssl_context.load_verify_locations(cert_path)
        ssl_context.verify_mode = CERT_REQUIRED
        
        # this will automatically pull the credentials from either the
        # ~/.aws/credentials file
        # ~/.aws/config 
        # or from the boto environment variables.
        boto_session = boto3.Session()
        
        # verify that the session is set correctly
        credentials = boto_session.get_credentials()
        if not credentials or not credentials.access_key:
            sys.exit("No access key found, please setup credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) according to https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")
    
        region = boto_session.region_name

        if not region:  
            sys.exit("You do not have a region set.  Set environment variable AWS_REGION or provide a configuration see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html#cli-configure-quickstart-precedence\n")
            

        auth_provider = SigV4AuthProvider(boto_session)
        contact_point = "cassandra.{}.amazonaws.com".format(region)

        profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            retry_policy=KeyspacesRetryPolicy(RETRY_MAX_ATTEMPTS=5))

        cluster = Cluster([contact_point], 
                         ssl_context=ssl_context, 
                         auth_provider=auth_provider,
                         port=9142,
                         execution_profiles={EXEC_PROFILE_DEFAULT: profile})

        self.session = cluster.connect()
    
    def write_to_cassandra(self, data: SparkDataFrame) -> bool:
        """
        Insert the data into Cassandra.
        
        Parameters:
        data : SparkDataFrame : The data to be inserted into Cassandra.
        
        Returns:
        status : bool : The status of the operation.
        
        """
        pass
    
    def read_from_cassandra(self, query: str) -> List[Dict[str, str]]:
        """
        Read data from Cassandra.
        
        Parameters:
        query : str : the query to be executed.
        
        Returns:
        data : List[Dict[str, str]] : The data read from Cassandra.
        
        """
        pass