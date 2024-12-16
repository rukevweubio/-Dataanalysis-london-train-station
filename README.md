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
CREATE OR REPLACE PROCEDURE stream_on_table3()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Use double quotes around the case-sensitive table name
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Date_ID_Departure" ';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_date"  ';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" ';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys" ';
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_Station" ';
    RETURN 'Stream created successfully';
END;
$$;
;
```
- create store procedure to automate the process of the pipeline
  ```// create task for DIm_data
CREATE OR REPLACE TASK my_store_procedure_dim_date
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '2 MINUTE'  
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_DATE_STREAM_2') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_date" AS Dim_date
    USING (
        SELECT 
            "Date_id",
            "Date_of_Purchase",
            "DAY_NAME",
            "MONTH_NAME",
            "Year",
            "Quarter",
            "Hour_of_Purchase",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_DATE_STREAM_2
    ) AS stream
    ON Dim_date."Date_id" = stream."Date_id"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_date."Date_of_Purchase" = stream."Date_of_Purchase",
            Dim_date."DAY_NAME" = stream."DAY_NAME",
            Dim_date."MONTH_NAME" = stream."MONTH_NAME",
            Dim_date."Year" = stream."Year",
            Dim_date."Quarter" = stream."Quarter",
            Dim_date."Hour_of_Purchase" = stream."Hour_of_Purchase"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Date_id", "Date_of_Purchase", "DAY_NAME", "MONTH_NAME", "Year", "Quarter", "Hour_of_Purchase")
        VALUES (
            stream."Date_id", 
            stream."Date_of_Purchase", 
            stream."DAY_NAME", 
            stream."MONTH_NAME", 
            stream."Year", 
            stream."Quarter", 
            stream."Hour_of_Purchase"
        );
```
- create table in the raw stage and using the copy command to load data into table
```// create a table  and load the data from the stage 
 create or replace table uk_railway_company
as 
select 
    $1 as "Transaction ID",
    $2 as "Date of Purchase",
    $3 as "Time of Purchase",
    $4 as "Purchase Type",
    $5 as "Payment Method",
    $6 as "Railcard",
    $7 as "Ticket Class",
    $8 as "Ticket Type",
    $9 as "Price",
    $10 as "Departure Station",
    $11 as "Arrival Destination",
    $12 as "Date of Journey",
    $13 as "Departure Time",
    $14 as "Arrival Time",
    $15 as "Actual Arrival Time",
    $16 as "Journey Status",
    $17 as "Reason for Delay",
    $18 as "Refund Request"
from @uk_internal_stage;
// check if the table is created 
select * from uk_railway_company
```
### Data Sources
- Structured Data: The primary data used in the pipeline is structured data, such as transactional data, sales records, customer information, and financial data.
- Flat Files:the Data type flat files (CSV, json)
- Format:csv.
### Data Flow
The data flow through this ETL pipeline is designed to automate the process of ingesting, transforming, and loading data into Snowflake.
The process involves loading a CSV file into Snowflake using SnowSQL, where the file is initially staged. With the help of a Snowflake Pipe,
you automate the process of loading the data from the stage into the target table. A task is used to trigger the pipe and automate the loading 
process. The loading operation is written as a stored procedure, which is wrapped in the task. Each time data is loaded into the stage,
the task calls the pipe to load the data into the target table, ensuring an automated, continuous data flow.
- stream:
After loading the file into the table, a Snowflake Stream is created on the table to capture any changes, including inserts, updates, and deletes. The Stream tracks changes in the table that occur between ETL operations, enabling real-time detection of data changes for efficient monitoring and processing.


