from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
import os
import sys

# Second Level import :
current_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(current_dir, "..", "..", "..")) # Coudln't figure out a better way ... unless I publish the package.
sys.path.append(parent_dir)

from tools.CassandraHandler import CassandraHandler
from plugins.NotifcationReporter import NotificationReporter
from plugins.ErrorHandler import ErrorHandler
from plugins.ErrorHandler.enums import report_status_enum

MY_ERROR_HANDLER = ErrorHandler()

class CassandraWriter(CassandraHandler):
    """
    This class will be responsible for writing the data into Cassandra.
    """
    
    def __init__(self, track_reporter: NotificationReporter) -> None: 
        """
        Constructor will start the connection to Cassandra using signv4 plugins.
        
        :param track_reporter : NotificationReporter : The notification reporter object.
        
        """
        super().__init__()
        self._track_reporter = track_reporter
    
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