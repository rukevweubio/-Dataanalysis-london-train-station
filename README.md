## Project Overview:
This project  focuses on automating an ETL pipeline using Snowflake to streamline data processing, transformation, and loading.
It addresses the challenge of manually handling large datasets, ensuring timely and accurate data updates.
By utilizing Snowflake's Streams, Tasks, Pipes, and SnowSQL, the pipeline will automate real-time data ingestion, transformation, and loading, significantly
improving operational efficiency and data accuracy. Currently, the data processing workflows are manual, time-consuming, and prone to errors,
highlighting the need for an automated system that ensures real-time data updates and transformation with minimal intervention. The solution 
automates the entire ETL process, from data extraction to transformation and loading, using Snowflake’s
native capabilities. This ensures seamless, error-free data flow, increases operational efficiency, and provides stakeholders with up-to-date insights.
### Project objective
The project aims to automate data extraction, transformation, and loading processes to improve data accuracy, ensure real-time updates, and minimize manual
intervention. It seeks to enhance scalability to handle growing data volumes efficiently. The expected outcomes include a
fully automated ETL pipeline in Snowflake, real-time data availability, and improved decision-making from timely and accurate data. Success will be
measured by seamless data integration into Snowflake, automated transformation tasks, accurate data loading, and real-time monitoring and error handling using Snowflake's native features.
### Project features
The project features core functionalities such as real-time data ingestion using Snowflake Pipes for continuous loading,
change data capture with Streams to detect and update changes in source tables, and automated data transformation through 
Snowflake Tasks, reducing manual SQL executions. It also includes efficient querying, transformation, and reporting via SnowSQL.
Innovative aspects include the integration of Streams and Tasks, which ensures real-time updates and automated transformations,
maintaining an up-to-date pipeline without manual intervention. The pipeline is designed for scalability and flexibility,
leveraging Snowflake’s cloud-native architecture to handle increasing data volumes. Additionally, built-in error handling 
and monitoring features ensure reliability by automatically managing failures and retries.
### Project Tech Stack
- Snowflake
- snowflake stream : Real-time change data capture (CDC) for tracking changes in source tables.
- Snowflake Tasks:  Automation of SQL-based transformation jobs based on defined schedules or events
- Snowflake Pipes: Continuous data loading into Snowflake from external sources.
- SnowSQL:Command-line client for executing queries, automating transformation processes, and managing Snowflake resources.
- SQL (Snowflake SQL):Used for writing and executing SQL queries for data manipulation and transformation
### How to Set Up the Tech Stack
- Snowflake Setup:create a Snowflake account if not already done and Set up a Snowflake warehouse, database, and schema to manage the ETL pipeline
```
CREATE WAREHOUSE my_warehouse;
CREATE DATABASE my_database;
CREATE SCHEMA my_schema;
````
- Install SnowSQL:Download and install SnowSQL from Snowflake’s official site [snowflake.com](snowflake.com)
- Configure SnowSQL Connection:Configure SnowSQL to connect to your Snowflake account
```
snowsql -a <account_identifier> -u <username> -r <role> -w <warehouse> -d <database> -s <schema>
```
- Set Up Snowflake Pipes for Data Ingestion:Define the external stage and pipe
``` CREATE STAGE my_stage
 FILE_FORMAT = (TYPE = 'CSV');
```
```
CREATE PIPE my_pipe
 AUTO_INGEST = TRUE AS
 COPY INTO my_table FROM @my_stage;
```
- Set Up Snowflake Streams for Change Data Capture (CDC):Create a stream to track changes in a source table

 ``` 
  CREATE OR REPLACE STREAM my_stream ON TABLE my_source_table;
```
-  Snowflake Tasks for Automation:Create a task to automate transformation
```
CREATE TASK my_transformation_task
WAREHOUSE = my_warehouse
SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
INSERT INTO my_target_table
SELECT * FROM my_source_table WHERE change_detected = TRUE;
```

