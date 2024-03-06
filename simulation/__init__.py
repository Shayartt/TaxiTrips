# First Level import :
from datetime import datetime
import time 

# Second level import 
from src.DataGenerator.dummy_generator import DummyGenerator

# Third Party imports
import pandas as pd

def main() : 
    print("Hi, I'm the main function of the simulation module, I will generate dummy data and send them to our processing system.")
    
    print("First, I will need you to provide with some parameters to generate the data : ")
    
    num_of_rows = int(input("Number of rows to generate : "))
    frequence = int(input("Frequence of generation (in seconds) : "))
    
    # Read the source data to be used in the generation: 
    source_data = pd.read_csv("C:/Users/MohamedMallouk/Documents/Micro-Services/Learning/data/yellow_tripdata_2019-01.csv")
    
    # Create a dummy generator list and generate data for each one of them
    list_generators = [DummyGenerator() for _ in range(num_of_rows)]
    
    for generator in list_generators :
        generator.generate(source_data)
        print(f"Data generated : {generator}")
        
        # Sleep for the frequence
        print(f"Sleeping for {frequence} seconds")
        time.sleep(frequence)
        
        # Send the data to the processing system
        print("Publishing Data into the Queue ...")
        queue_input = {
            "data" : generator.data,
        }
        
        # TODO send to SQS now : 
        
        
    print("All data generated and sent to the processing system, exiting ...")
    
    
if __name__ == "__main__" :
    main()