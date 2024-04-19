# TaxiTrips : Big Data Processing & Monitoring 

# Current Version: 1.2.0

This project aims to handle real-time taxi traffic data, perform ETL processes, implement data processing, and monitor alerting mechanisms. Additionally, it includes an admin dashboard platform.

For more information or any questions, please contact malloukmoe@gmail.com.

## High-Level Diagram

![High-Level Diagram](processing/doc/diagrams/Tec-Diagram-Flow.png?raw=true "High-Level")

## Start Project : 

#### Streaming:
Once the code is deployed into SPARK/EMR cluster, navigate into /processing and execute : spark-submit __init__.py

#### Data Simulation:
Once streaming is started, open a new cmd and navigate into /simulation, execute : python __init__.py, the console will be asking you a few questions regarding data generation parameters and feel free to enter the numbers you want and/or rerun the script again and again...

#### Web Application:
To start the web-application locally, navigate into /webapp and run python app.py, if you're deploying the webapp into a different server, please make sure to specify the host/port in the .env to sync with the spark application.


## Credits:

Cassandra (AWS KeySpace) connector plugin : [SOURCE CODE](https://github.com/aws-samples/amazon-keyspaces-examples/tree/main)
WebApp template design from : AppSeed.


## Conclusion:

Unfortunately, using AWS Keyspace with EMR wasn't the best option. Despite both belonging to the same cloud provider (AWS), the integration isn't straightforward. The TokenFactory isn't implemented in the driver yet, making it very complex to write/read using the Spark Driver. As a workaround to avoid delaying progress, I'm going to use a direct Signv4 connection to write data. the bottleneck here is that we'll perform a .collect() we could also use UDF or push it into a queue system which can result into more optimized results.

Perhaps OpenSearch or PostgreSQL would have been a better option, but since this is a non-profitable project, I've decided to continue and challenge myself to find a solution.

Personally I love using SQL filtering over my dataframe instead of dataframe APIs, in the alert handler, I tried to use them first but I had an issue with saving the dataframe into a temporary view, even passing the main spark session to my class I was getting the error "The table or view cannot be found", I'll try to work on that part later or maybe in another project to figure out how can I use the temporary views when loading data from my streaming, please feel free to commit a new branch using sql queries instead of dataframe APIs in the alert handler, it's would be very helpfull.

The CassandraHandler was built to serve the spark project only, the dependencies there makes it very complex to use it somewhere else, this is a mistake that I've realised later when I started working on the webApp and found out that I needed to use it, to fix that following the SOLID coding norms, I'll need to either make it a mother class  and create sub-classes that enherite from it, you can check previous commits code to compare the changes, after this commit it's will be fixed, however the python library import is tricky here and I had to use the sys add path to specify my /tools for the main Cassandra handler, maybe you can help me by importing it in a simpler way?



More details coming soon..

