from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

# Second Level import :
from plugins.sigv4.KeyspacesRetryPolicy import KeyspacesRetryPolicy
from plugins.NotifcationReporter import NotificationReporter
from plugins.ErrorHandler import ErrorHandler
from plugins.ErrorHandler.enums import report_status_enum

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

MY_ERROR_HANDLER = ErrorHandler()

class CassandraWriter:
    """
    This class will be responsible for writing the data into Cassandra.
    """
    _track_reporter: NotificationReporter
    
    def __init__(self) : 
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
    
    @MY_ERROR_HANDLER.handle_error
    def write_to_cassandra(self, data: SparkDataFrame) -> bool:
        """
        Insert the data into Cassandra.
        
        Parameters:
        data : SparkDataFrame : The data to be inserted into Cassandra.
        
        Returns:
        status : bool : The status of the operation.
        
        """
        # Convert the SparkDataFrame to a list of rows, check readme to understand why we are doing this.
        data_to_insert = data.collect()
        
        column_names = '"'  +  '", "'.join(data.columns) + '"'
        
        num_columns = len(data.columns)
        placeholders = ','.join(['%s'] * num_columns)
        
        for row in data_to_insert : 
            # Write data to Keyspace
            self.session.execute(f'INSERT INTO traffic_db.streaming_record ({column_names.lower()}) values ({placeholders})', row)
        
        # Logging the operation :
        reporting_tracker_message = {
            "message" : f"Data inserted into the database successfully.",
            "status" : report_status_enum.SUCCESS.value, 
        }
        self._track_reporter.publish_to_sqs( reporting_tracker_message)
        
        return True