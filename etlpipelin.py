#!/usr/bin/env python
# coding: utf-8

# Install required packages
!pip install snowflake-connector-python
!pip install load_dotenv
!pip install python-dotenv
get_ipython().system('pip install snowflake-connector-python snowflake-sqlalchemy pandas')

# In[9]:
get_ipython().system('pip install SQLAlchemy')

# In[ ]:
from dotenv import load_dotenv
from sqlalchemy import create_engine
import snowflake.connector
import os
load_dotenv(override=True)

# In[ ]:
def load_data_from_env():
    load_dotenv(override=True)
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    return user, account, password, warehouse, database, schema

user, account, password, warehouse, database, schema = load_data_from_env()
print(user, account, password, warehouse, database, schema)

# In[ ]:
def connection_snowflake_database():
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    conn = conn.cursor()
    conn.execute('select current_database()').fetchone()
    print('Database is connected')
    return conn

# In[ ]:
def execute_multi_query(conn):
    """Executes multiple SQL queries."""
    try:
        queries = [
            "use database ubio_demo;",
            "use schema ubio_schema;"
        ]
        for query in queries:
            conn.execute(query)
        print("Queries executed successfully.")
    except Exception as e:
        print(f"Error executing queries: {e}")
    # Ensure connection is closed in case of errors

# Assuming 'connection_snowflake_database' function returns an active connection
conn = connection_snowflake_database()
execute_multi_query(conn)

# In[ ]:
# Test database connection
conn.execute("select current_database(), current_schema()").fetchone()

# In[ ]:
def create_database_and_schema(conn):
    """Create a new database and schema where we can load the data"""
    try:
        query = [
            "create database if not exists uk_demo;",
            "use database ubio_demo;",
            "create schema if not exists uk_schema;",
            "use schema ubio_schema;"
        ]
        for q in query:
            conn.execute(q)
        print("Database and schema created successfully")
    except Exception as e:
        print(f"Error executing queries: {e}")

conn = connection_snowflake_database()
create_database_and_schema(conn)

# In[ ]:
def create_file_format_stage(conn):
    """
    Create a file format and stage in Snowflake, and upload a file to the stage.
    """
    try:
        # Define the queries to create file format, stage, and upload the file
        file_path = "/content/railway.csv"
        with open(file_path, 'rb') as file:
            file_path = file.read()

        queries = [
            """
            CREATE OR REPLACE FILE FORMAT my_csv_file_format
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            SKIP_HEADER = 2;
            """,
            """
            CREATE OR REPLACE STAGE my_csv_stage
            FILE_FORMAT = my_csv_file_format;
            """,
            """
            PUT file:///content/railway.csv @my_csv_stage;
            """
        ]

        # Execute each query in the list
        for query in queries:
            conn.execute(query)
            print(f"Executed: {query.strip().splitlines()[0]}...")

        print("File format and stage created successfully.")
        print("File loaded into the stage.")

    except Exception as e:
        print(f"Error executing queries: {e}")

conn = connection_snowflake_database()
create_file_format_stage(conn)

# In[ ]:
import pandas as pd

def check_if_the_file_is_loaded(conn):
    """
    Check the Snowflake stage to verify if the file is loaded successfully.
    """
    try:
        # Query to list files in the stage
        query = "LIST @my_csv_stage;"

        conn.execute(query)
        result = conn.fetchall()

        columns = [desc[0] for desc in conn.description]

        df = pd.DataFrame(result, columns=columns)
        print("Stage Contents:")
        print(df)
        print("File is loaded successfully into the stage.")

    except Exception as e:
        print(f"Error executing query: {e}")

conn = connection_snowflake_database()
check_if_the_file_is_loaded(conn)

# In[ ]:
def load_data_to_table(conn):
    """
    Load data from the Snowflake stage into a table.
    """
    try:
        # List of queries to execute
        queries = [
            """
            CREATE OR REPLACE TABLE uk_train_table AS
            SELECT
                CAST($1 AS STRING) AS Transaction_ID,
                CAST($2 AS DATE) AS Date_of_Purchase,
                CAST($3 AS TIME) AS Time_of_Purchase,
                CAST($4 AS VARCHAR) AS Purchase_Type,
                CAST($5 AS VARCHAR) AS Payment_Method,
                CAST($6 AS VARCHAR) AS Railcard,
                CAST($7 AS VARCHAR) AS Ticket_Class,
                CAST($8 AS VARCHAR) AS Ticket_Type,
                CAST($9 AS DECIMAL(10,2)) AS Price,
                CAST($10 AS VARCHAR) AS Departure_Station,
                CAST($11 AS VARCHAR) AS Arrival_Destination,
                CAST($12 AS DATE) AS Date_of_Journey,
                CAST($13 AS TIME) AS Departure_Time,
                CAST($14 AS TIME) AS Arrival_Time,
                CAST($15 AS TIME) AS Actual_Arrival_Time,
                CAST($16 AS VARCHAR) AS Journey_Status,
                CAST($17 AS VARCHAR) AS Reason_for_Delay,
                CAST($18 AS VARCHAR) AS Refund_Request
            FROM @my_csv_stage
            """,
            """
            SELECT * FROM uk_train_table;
            """
        ]

        # Execute each query
        for query in queries:
            conn.execute(query)

            if query.strip().lower().startswith("select"):
                result = conn.fetchall()
                columns = [desc[0] for desc in conn.description]
                df = pd.DataFrame(result, columns=columns)
                print("Data loaded into DataFrame:")
                print(df.head())

    except Exception as e:
        print(f"Error executing query: {e}")
    return df

conn = connection_snowflake_database()
df = load_data_to_table(conn)

# In[ ]:
def transformation_of_data(df):
    """
    Perform basic data transformations and return insights about the DataFrame.
    Args:
        df (pd.DataFrame): The input DataFrame.
    Returns:
        dict: A dictionary containing column names, null counts, and data types.
    """
    try:
        result = {
            "columns": df.columns.tolist(),
            "null_counts": df.isnull().sum().to_dict(),
            "data_types": df.dtypes.to_dict(),
            'change_data_type': df['PRICE'].astype('int').to_dict()
        }
        return result
    except Exception as e:
        print(f"Error during transformation: {e}")
        return None

transformation_of_data(df)

# In[ ]:
def create_dimensional_table(df):
    """Create the dimensional table from the dataset."""
    Dim_railcard = df['RAILCARD'].rename({'RAILCARD': 'railcard'})
    Dim_railcard.index.name = 'railcard_id'
    Dim_railcard = Dim_railcard.reset_index()
    Dim_railcard.to_sql('Dim_railcard', con=engine, if_exists='replace', index=False)
    return Dim_railcard

create_dimensional_table(df)

# In[ ]:
# Save DataFrame to SQL table
df.index.name = 'Ticket_id'
df = df.reset_index()
df.to_sql('uk_train_table', con=engine, if_exists='replace', index=False)
df.to_csv('fact_table', index=False)
DF = pd.read_csv('fact_table')
DF.head()

# In[ ]:
Dim_railcard = df['RAILCARD'].rename({'RAILCARD': 'railcard'})
Dim_railcard.index.name = 'railcard_id'
Dim_railcard = Dim_railcard.reset_index()
Dim_railcard.to_sql('Dim_railcard', con=engine, if_exists='replace', index=False)

# In[ ]:
def create_table_ticket_id(df):
    """Create a table for ticket id."""
    Dim_Tickets = df[['TICKET_CLASS', 'TICKET_TYPE']]
    Dim_Tickets.index.name = 'Ticket_id'
    Dim_Tickets = Dim_Tickets.reset_index()
    Dim_Tickets.to_sql('Dim_Tickets', con=engine, if_exists='replace', index=False)
    return Dim_Tickets

create_table_ticket_id(df)

# In[ ]:
Dim_Tickets = df[['TICKET_CLASS', 'TICKET_TYPE']]
Dim_Tickets.index.name = 'Ticket_id'
Dim_Tickets = Dim_Tickets.reset_index()
Dim_Tickets.to_csv('Dim_Tickets.csv', index=False)

# In[ ]:
"""Create the Dim_station table from the dataset."""
Dim_Station = df[["DEPARTURE_STATION", "ARRIVAL_DESTINATION"]]
Dim_Station.index.name = 'Station_id'
Dim_Station = Dim_Station.reset_index()
Dim_Station = Dim_Station.rename(columns={'DEPARTURE_STATION': 'Departure_Station', 'ARRIVAL_DESTINATION': 'Arrival_Destination'})
Dim_Station.to_sql('Dim_Station', con=engine, if_exists='replace', index
