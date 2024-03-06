# First Level import :
from datetime import datetime
import time 
import json 
import boto3
import os 
from dotenv import load_dotenv

# Second level import 
from src.DataGenerator.dummy_generator import DummyGenerator

# Third Party imports
import pandas as pd

# Get the path of the parent directory
parent_dir = os.path.dirname(os.path.abspath(__file__))

# Load variables from .env.local
dotenv_path = os.path.join(parent_dir, '..', '.env.local')
load_dotenv(dotenv_path)

def main() : 
    print("Hi, I'm the main function of the simulation module, I will generate dummy data and send them to our processing system.")
    
    print("First, I will need you to provide with some parameters to generate the data : ")
    
    num_of_rows = int(input("Number of rows to generate : "))
    frequence = int(input("Frequence of generation (in seconds) : "))
    
    # Read the source data to be used in the generation: 
    source_data = pd.read_csv("C:/Users/MohamedMallouk/Documents/Micro-Services/Learning/data/yellow_tripdata_2019-01.csv")
    
    # FILLNA for the source data
    source_data.fillna(0, inplace=True)
    
    # Create BOTO3 SQS Client : 
    client_sqs = boto3.client('sqs','eu-central-1', aws_access_key_id= os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key= os.environ['AWS_SECRET_ACCESS_KEY'])
    
    # Create a dummy generator list and generate data for each one of them
    list_generators = [DummyGenerator() for _ in range(num_of_rows)]
    
    for indx, generator in enumerate(list_generators) :
        generator.generate(source_data)
        
        if num_of_rows <= 100 or indx % 100 == 0 : # To avoid spam CMD
            print(f"Data generated : {generator}, Iteration : {indx} / {num_of_rows}")
            
            # Sleep for the frequence
            print(f"Sleeping for {frequence} seconds")
            
        # Frequence Rest 
        time.sleep(frequence)
        
        # Send the data to the processing system
        print("Publishing Data into the Queue ...")
        response = client_sqs.send_message(
        QueueUrl= os.environ['SQS_TRAFFIC_QUEUE'],
        MessageBody= json.dumps(generator.data),
        )
        
        
    print("All data generated and sent to the processing system, exiting ...")
    
    
if __name__ == "__main__" :
    main()