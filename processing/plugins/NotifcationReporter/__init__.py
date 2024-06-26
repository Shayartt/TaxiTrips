import os
from datetime import datetime
import json 
import boto3 
from dotenv import load_dotenv

load_dotenv()

"""

NotificationReporter 

-------------------

This module is used to report updates / informations and any other important tracing to the logging system.

"""

class NotificationReporter:
    """
    NotificationReporter is used to report into SQS queue to publish into socketIO microservices to be consumed by the front-end and insert into OS index to vizualise in Kibana.
    """
    def __init__(self,stream_id, index_name = "traffic_processing_tracker") : 
        """
        Constructor of NotificationReporter class that will be the mother function to init resources needed to keep the application in-track and publish anything to logging system and socket.
        
        :param index_name: Name of the index to publish into OS.
        :type index_name: str
        :return: NotificationReporter object.
        """
        self.stream_id = stream_id
        self._index_name = index_name # TODO use in input later to make this reporter dynamic and used from the whole project.
        # Init boto3 client to be used to publish into SQS
        self.__sqs_client = boto3.client("sqs", os.environ["AWS_REGION"], aws_access_key_id= os.environ["AWS_ACCESS_KEY_ID"], aws_secret_access_key= os.environ["AWS_SECRET_ACCESS_KEY"])
        self.__SQS_NOTIFICATION = os.environ["SQS_NOTIFICATION"]
        self.__SQS_EMAIL = os.environ["SQS_EMAIL"]
        
    def publish_to_sqs(self, message: dict, function_name: str = None) : 
        """
        Publish message to SQS queue.
        
        :param message: Message to publish.
        :type message: dict
        :param function_name: Name of the function that is publishing the message.
        :type function_name: str
        :return: True if success else False.
        """
        try : 
            message['received_at'] = str(datetime.now()) # Add received_at field to the message
            message['stream_id'] = self.stream_id # Add stream_id field to the message
            message['function_name'] = function_name # Add function_name field to the message
            message['index_name'] = self._index_name # Add index_name field to the message to specify the index to publish into OS.
            # Prepare format : 
            input_body = {
                "document_content" : message
            }
            self.__sqs_client.send_message(QueueUrl=self.__SQS_NOTIFICATION, MessageBody=json.dumps(input_body)) # This is by default an Async call, no need to implement anything specific.
            return True
        except Exception as e : 
            print("Error while publishing to SQS : " + str(e))
            return False

    def send_email(self, message, to= str(os.environ['EMAILS_TO']).split(",")) : 
        """
        Send email to the specified email addresses.
        
        :param message: Message to send.
        :type message: str
        :param to: List of email addresses to send the email to.
        :type to: list
        :return: True if success else False.
        """
        try : 
            
            vars = {
                        "subject" : "sample_message",
                        "to" : to,
                        "variables": {
                            "msg": message
                            },
                    }

            self.__sqs_client.send_message(QueueUrl=self.__SQS_EMAIL, MessageBody=json.dumps(vars)) 

        except Exception as e : 
            print("Error while sending email : " + str(e))
            return False