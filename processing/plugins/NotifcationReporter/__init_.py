import os
from datetime import datetime
import json 
import boto3 


"""

NotificationReporter 

-------------------

This module is used to report updates / informations and any other important tracing to inform other microServices and logging system.

"""

class NotificationReporter(object):
    """
    NotificationReporter is used to report into SQS queue to publish into socketIO microservices to be consumed by the front-end and insert into OS index to vizualise in Kibana.
    """
    def __init__(self, index_name = "data_processing_tracker") : 
        """
        Constructor of NotificationReporter class that will be the mother function to init resources needed to keep the application in-track and publish anything to logging system and socket.
        
        :param index_name: Name of the index to publish into OS.
        :type index_name: str
        :return: NotificationReporter object.
        """
        
        self.index_name = index_name
        # Init boto3 client to be used to publish into SQS
        self.sqs_client = boto3.client("sqs", os.environ["AWS_REGION"], aws_access_key_id= os.environ["AWS_ACCESS_KEY_ID"], aws_secret_access_key= os.environ["AWS_SECRET_ACCESS_KEY"])
        self.SQS_NOTIFICATION = os.environ["SQS_NOTIFICATION"]
        
    def publish_to_sqs(self, message) : 
        """
        Publish message to SQS queue.
        
        :param message: Message to publish.
        :type message: str
        :return: True if success else False.
        """
        try : 
            message['received_at'] = str(datetime.now()) # Add received_at field to the message
            
            # Prepare format : 
            input_body = {
                "document_content" : message
            }
            self.sqs_client.send_message(QueueUrl=self.SQS_QUEUE_TRACKING_URL, MessageBody=json.dumps(message)) # This is by default an Async call, no need to implement anything specific.
            return True
        except Exception as e : 
            print("Error while publishing to SQS : " + str(e))
            return False
