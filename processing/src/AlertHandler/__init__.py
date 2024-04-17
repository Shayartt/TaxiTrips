from dataclasses import dataclass
import json 
import time 
from datetime import datetime

# Second Level import :
from plugins.NotifcationReporter import NotificationReporter
from plugins.ErrorHandler import ErrorHandler
from plugins.ErrorHandler.enums import report_status_enum

# Third Level import :
from pyspark.rdd import RDD
from pyspark.broadcast import Broadcast
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pandas as pd 


MY_ERROR_HANDLER = ErrorHandler()

"""

AlertHandler

"""

@dataclass
class AlertHandler:
    """
    This Class will be responsible for monitoring a few alerts and report them to the logging system and send email if needed.
    """
    __spark_session: SparkSession
    track_reporter : NotificationReporter
    LOCATION_BLACKLIST = ["Seaport"]
    
    def __post_init__(self):
        """
        Post init method will create the spark dataframe and save it as temporary view in memory to be used during the execution.
        """
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("details", StringType(), True),
            StructField("datetime", TimestampType(), True) 
        ])
        self.__alerts_df = self.__spark_session.createDataFrame([], schema)
        self.__start_time = time.perf_counter()
        
    @MY_ERROR_HANDLER.handle_error
    def apply_alerts(self, data: SparkDataFrame) -> None:
        """
        Method to apply alerts on the data.
        """
        # Save in cache the data : 
        data.persist()  
        
        # Applying the rules, to scale add more rules here.
        self.rule_1(data)
        self.rule_2(data)
        self.rule_3(data)
        
        # Report the alerts
        self.report_alerts()
        
        # KPIs : 
        # Capture the end time
        end_time = time.perf_counter()
        
        # Calculate the duration
        duration = end_time - self.__start_time
        reporting_tracker_message = {
            "message" : f"Batch execution time: {duration} seconds",
            "duration" : duration, 
            "length" : data.count(),
            "alerts" : self.__alerts_df.count()
        }
        self.track_reporter.publish_to_sqs( reporting_tracker_message)
            
    
    @MY_ERROR_HANDLER.handle_error
    def rule_1(self, data: SparkDataFrame) -> None:
        """
        Method to apply the first rule of the alerts, we will check if the total_amount > 1000 and the trip_distance < 100.
        """
        # We'll use SQL to apply the rule.
        df_check = data.filter((data.total_amount > 1000) & (data.trip_distance < 100))
        
        # Check if the dataframe is not empty
        if df_check.count() > 0:
            # Add new record to self.__alerts_df with id = 1, status = "ALERT", details = "Total amount > 1000 and trip distance < 100"
            self.__alerts_df = self.__alerts_df.union(self.__spark_session.createDataFrame([(1, "ALERT", "Total amount > 1000 and trip distance < 100", datetime.now())], ["id", "status", "details", "datetime"]))
            
    @MY_ERROR_HANDLER.handle_error
    def rule_2(self, data: SparkDataFrame) -> None:
        """
        Method to apply the second rule of the alerts, we will check if the dolocation is in the LOCATION_BLACKLIST.
        """
        # We'll use SQL to apply the rule.
        df_check = data.filter(data.DOLocationName.isin(self.LOCATION_BLACKLIST))
        
        # Check if the dataframe is not empty
        if df_check.count() > 0:
            # Add new record to self._data_df with id = 2, status = "ALERT", details = "Dropoff location is in the LOCATION_BLACKLIST"
            self.__alerts_df = self.__alerts_df.union(self.__spark_session.createDataFrame([(1, "ALERT", "Dropoff location is in the LOCATION_BLACKLIST", datetime.now())], ["id", "status", "details", "datetime"]))
            
    @MY_ERROR_HANDLER.handle_error
    def rule_3(self, data: SparkDataFrame) -> None:
        """
        Method to apply the third rule of the alerts, we will check if pickup datetime is higher than 9pm night.
        """
        # We'll use SQL to apply the rule.
        df_check = data.filter(hour(data.tpep_pickup_datetime) > 21)
        
        # Check if the dataframe is not empty
        if df_check.count() > 0:
            # Add new record to self._data_df with id = 3, status = "ALERT", details = "Pickup datetime is higher than 9pm night"
            self.__alerts_df = self.__alerts_df.union(self.__spark_session.createDataFrame([(1, "ALERT", "Pickup datetime is higher than 9pm night", datetime.now())], ["id", "status", "details", "datetime"]))
            
    @MY_ERROR_HANDLER.handle_error
    def report_alerts(self) -> None:
        """
        Method will be used after applying rules to report triggered rules.
        """
        if self.__alerts_df.count() > 0:
            # For all records in our self._data_df, we'll publish them to the logging system.
            for record in self.__alerts_df.collect():
                reporting_tracker_message = {
                    "message" : f"Alert triggered : {record['details']}",
                    "status" : report_status_enum.TRIGGERED.value, 
                }
                self.track_reporter.publish_to_sqs( reporting_tracker_message)
                
            # Email Notification : 
            self.track_reporter.send_email("Alerts triggered, please check the logging system for more details.")
        else : 
            print("No alerts triggered.")
        
