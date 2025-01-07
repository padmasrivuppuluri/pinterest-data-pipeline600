# Pinterest Data Pipeline

## Table of contents:
- [Project Overview](#Project-overview) 
- [Data Pipeline](#Data Pipeline)
- [Dependencies](#Dependencies)
- [How to Run the Project](#How to run the project)
- [Usage](#Usage)

## Project Overview:
### Description:
The "Pinterest Data Pipeline" project replicates Pinterest's data pipeline on the AWS Cloud. Pinterest processes large amounts of data daily to enhance user engagement and deliver personalized content. This project serves as a practical exercise to build a scalable data pipeline using AWS services. It helped me learn about managing large datasets, configuring cloud infrastructure, and integrating pipeline components, along with data cleaning and analysis.

### Objectives:
- Develop a scalable and high-performing data processing pipeline.
- Ingest, process, and analyze large datasets.
- Explore and gain expertise in various AWS services.
- Establish a data streaming pipeline.

## Data Pipeline

  Amazon RDS

## How to run the project:
1. Clone the repository using:
git clone https://github.com/padmasrivuppuluri/pinterest-data-pipeline600
2. Run user_posting_emulation.py to extract the data from Amazon RDS and formats it
3. Run batch_emulation.py ->Data will send to the kafka topics
4. Run user_posting_emulation_streaming.py to extract the data from RDS and send to the Kinesis streams

## Project structure:
### Milestone 1:
Set up the environment.

### Milestone 2: 
Sign into AWS to build the pipeline

## Project structure:
### Milestone 1:
Set up the environment.

### Milestone 3: 
#### Configure the EC2 Kafka Client:
- Generate a .pem file for EC2 instance access through Parameter Store and store it with the key pair name.
- Install Kafka and the IAM MSK authentication package on the EC2 instance. Ensure that the required permissions are set up for MSK cluster authentication.
- Modify the client.properties file located in the kafka_folder/bin directory to enable AWS IAM authentication for the cluster.
- Use the Bootstrap servers and Apache Zookeeper strings from the MSK Management Console to create specific Kafka topics.
- The goal of configuring the EC2 Kafka client was to enable secure access to the MSK cluster through IAM-based authentication.

### Milestone 4:
#### Connecting an MSK Cluster to an S3 Bucket:
- Create or locate the target S3 bucket in the S3 console.
- Download the Confluent.io Amazon S3 Connector to the EC2 client and upload it to the identified S3 bucket.
- In the MSK Connect console, create a custom plugin.
- During the connector setup, select the IAM role for MSK authentication to ensure secure access to MSK and MSK Connect.

### Milestone 5:
#### API Gateway and Kafka REST Proxy Setup
##### API Gateway Configuration:
- Utilize the provided API by configuring a PROXY integration.
- Set up an HTTP ANY method for the resource, ensuring the correct EC2 PublicDNS from your EC2 machine is used.
- Deploy the API to generate the Invoke URL, which will be used for communication.

##### Kafka REST Proxy Configuration:
- Install the Confluent Kafka REST Proxy package on your EC2 client machine.
- Modify the kafka-rest.properties file to configure IAM authentication for connecting to the MSK cluster.
- Start the Kafka REST Proxy to enable seamless data transmission between your API and the MSK Cluster, using the previously configured plugin-connector pair.

### Milestone 6: Databricks
- Mount s3 bucket to Databricks
- Create three distinct Data Frames (df_pin, df_geo, df_user) for processing Pinterest post data, geolocation data, and user data.

### Milestone 7: spark on Databricks:
#### Data Cleaning for df_pin DataFrame:
- Replace empty or irrelevant values in the df_pin DataFrame.
- Ensure that all data is correctly represented with numeric data types where applicable.
- Reorganize the columns to ensure they follow the desired order for analysis.

#### Data Cleaning for df_geo DataFrame:
- Create a new array that stores the coordinates for geographic data in the df_geo DataFrame.
- Convert data types where necessary to ensure consistency.
- Reorder the columns to match the required format for further processing.

#### Data Cleaning for df_user DataFrame:
- Concatenate the first_name and last_name fields to create a user_name column in the df_user DataFrame.
- Adjust data types to ensure proper consistency across the DataFrame.
- Reorder the columns to improve readability and maintain consistency.

#### Performing Spark DataFrame Queries:
- Use Spark's DataFrame operations in Databricks to query the cleaned data and extract meaningful insights.
- Implement various queries to answer relevant questions based on the structured data.

### Milestone 8: AWS MWAA
#### Set Up Airflow DAG in MWAA Environment:
- Leverage the provided Databricks-Airflow-env MWAA environment and mwaa-dags-bucket to configure an Airflow Directed Acyclic Graph (DAG).
- Set up the DAG to trigger a Databricks Notebook on a specified schedule for automated processing.

#### Manual Triggering for Immediate Execution:
- Ensure that the DAG can also be manually triggered from the AWS MWAA interface for on-demand execution, allowing for flexible processing when required.

#### Efficient Batch Processing:
- By using AWS MWAA, this setup facilitates seamless batch processing, automating the scheduling of Databricks Notebooks, ensuring efficient data processing and analysis without requiring manual intervention.

### Milestone 9: Stream Processing- AWS Kinesis
- Configure an API with kinesis
- Extract data from RDS and send to Kinesis streams
- Read Kinesis Streaming data into databricks
- Clean the data
- Write the cleaned data into delta table
