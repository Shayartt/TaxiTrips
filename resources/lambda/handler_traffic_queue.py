try : 
    import unzip_requirements
except ImportError:
    pass

from datetime import datetime
import json
import boto3 
import os 


def traffic_event_handler(event,context):
    #The date var : 
    my_date = datetime.now()
    
    # Read input : 
    message = json.loads(event['Records'][0]['body'])
    print(f"Event received : {message}" )
    
    # Const variables : 
    STREAMING_PATH = "/efs/traffic_streaming"
    AWS_REGION = "ca-central-1"
    #Reading Env :
    aws_acceskeyId = os.environ['aws_acceskeyId']
    aws_secretKey = os.environ['aws_secretKey']
    LOGGING_QUEUE_URL = os.environ['LOGGING_QUEUE_URL']
    
    # Logging Step : 
    message['stage'] = "traffic_event_handler"
    message['platform'] = "lambda"
    
    input_logging  = {
        "index_name" : "traffic_processing_tracker",
        "event_chanel" : "summary",
        "document_content" : message,
    }
    
    # Publish TO SQS : 
    sqs = boto3.client('sqs', aws_access_key_id=aws_acceskeyId, aws_secret_access_key=aws_secretKey, region_name=AWS_REGION)
    response = sqs.send_message(
        QueueUrl=LOGGING_QUEUE_URL,
        MessageBody=json.dumps(input_logging)
    )
    print(f"Message Loggedd to queue : {response}")

    # Generate filename based on datetime : 
    filename = my_date.strftime("%Y-%m-%d_%H-%M-%S") + ".json"
    
    # If filename exists, we will create a new one :
    while os.path.exists(STREAMING_PATH + "/" + filename):
        my_date = datetime.now()
        filename = my_date.strftime("%Y-%m-%d_%H-%M-%S") + ".json"
    
    # Publish message to our streaming as json file in our streaming_path :
    with open(STREAMING_PATH + filename, "w") as file:
        json.dump(message, file) 
        
    # Return success :
    body = {
        "Status":"Success, failed placed correctly in streaming folder",
        "filename" : filename
    }  
    return {"statusCode": 200, "body": json.dumps(body)}


def process_tracking_message(event, context):
    message = json.loads(event['Records'][0]['body'])
        
    # Reading input vars
    print("Reading Inputs")
    print("event : " + str(message))
    document_content = message['document_content'] # Json Object containig the tracking information's message in a specific format.
    event_chanel = message['event_chanel']
    index_name = message['index_name']
    
    # Init status variables :
    failed = None 
    success = None 
    
    # Read env :
    print("Reading Env..")
    OS_HOST = os.environ['OS_HOST']
    OS_USER = os.environ['OS_USER']
    OS_PASSWORD = os.environ['OS_PASSWORD']
    
    my_os_client = load_opensearch_connection(OS_HOST, OS_USER, OS_PASSWORD)
    
    # Publish to socket first :
    try : 
        rooms = [document_content['generated_id']]
        send_to_socket(document_content, rooms, event_chanel)
    except Exception as e:
        print(f'Error in sending to socket : {e}')
    
    # Insert into OpenSearch :
    print("Inserting into OpenSearch..")
    success, failed = insert_into_opensearch_doc_many([document_content], my_os_client, index_name)
    
    # Check Status of insertion
    if failed:
        print(f"Failed to insert {len(failed)} documents")
        return False
    
    # Refresh Index :
    print("Refreshing Index..")
    refresh_os_index(my_os_client, index_name)
    result_os = "Success"
    
    # Return Success :
    body = {
        "input": event,
        "failed_message": failed,
        "success_message": success,
        "result_os": result_os,
    }
    return {"statusCode": 200, "body": json.dumps(body)}