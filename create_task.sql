create or replace schema curated_schema;

// show the number of table 
show tables;

    

CREATE OR REPLACE PROCEDURE stream_on_table()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Use double quotes around the case-sensitive table name
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_railcard" ';
    RETURN 'Stream created successfully';
END;
$$;

CALL stream_on_table()

CREATE OR REPLACE PROCEDURE stream_on_table2()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    -- Use double quotes around the case-sensitive table name
    EXECUTE IMMEDIATE 'CREATE OR REPLACE STREAM dim_railcard_stream ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_time"  ';
    RETURN 'Stream created successfully';
END;
$$;

CALL stream_on_table2()



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

UBIO_DEMO.UBIO_SCHEMA.STREAM_ON_TABLE3UBIO_DEMO.UBIO_SCHEMA.STREAM_ON_TABLE3


// create a task to  call the store procdure to activate the stream on the table 
-- Create the task
CREATE OR REPLACE TASK my_store_procedure_task2
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'  -- Runs every minute
  AS
  BEGIN
      CALL stream_on_table3();
      CALL stream_on_table();
      CALL stream_on_table2();
  END;

    



CREATE OR REPLACE PROCEDURE my_update()
RETURNS text
LANGUAGE SQL
AS
$$
DECLARE
    sql_statement text;
BEGIN
    -- Prepare the dynamic SQL for merge statement
    sql_statement := '
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_railcard" AS Dim_railcard
    USING (
        SELECT 
            RAILCARD_ID,
            RAILCARD,
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_RAILCARD_STREAM
    ) AS stream
    ON Dim_railcard."RAILCARD_ID" = stream."RAILCARD_ID"
    
    -- Update when matched and action is insert and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = ''INSERT'' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_railcard."RAILCARD" = stream."RAILCARD"
    
    -- Delete when matched and action is delete
    WHEN MATCHED AND stream.METADATA$ACTION = ''DELETE'' THEN
        DELETE
    
    -- Insert when not matched and action is insert
    WHEN NOT MATCHED AND stream.METADATA$ACTION = ''INSERT'' THEN
        INSERT ("RAILCARD_ID", "RAILCARD")
        VALUES (stream."RAILCARD_ID", stream."RAILCARD");
    ';

    -- Execute the dynamic SQL
    EXECUTE IMMEDIATE :sql_statement;

    -- Return success message
    RETURN 'Procedure executed successfully';
END;
$$;

call my_update()


// CRETE TASK FOR THE DIM_RAILCARD_STREAM ;

CREATE OR REPLACE TASK my_store_procedure_DIM_RAILCARD
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'  -- Runs every minute
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_RAILCARD_STREAM') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_railcard" AS Dim_railcard
    USING (
        SELECT 
            "RAILCARD_ID",
            "RAILCARD",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_RAILCARD_STREAM
    ) AS stream
    ON Dim_railcard."RAILCARD_ID" = stream."RAILCARD_ID"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_railcard."RAILCARD" = stream."RAILCARD"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("RAILCARD_ID", "RAILCARD")
        VALUES (stream."RAILCARD_ID", stream."RAILCARD");


// CREATE STREAM ON EACH TABLE 
CREATE OR REPLACE STREAM dim_railcard_stream_1 ON TABLE UBIO_DEMO.UBIO_SCHEMA."Date_ID_Departure";
CREATE OR REPLACE STREAM dim_railcard_stream_2 ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_date";
CREATE OR REPLACE STREAM dim_railcard_stream_3 ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets";
CREATE OR REPLACE STREAM dim_railcard_stream_4 ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys";
CREATE OR REPLACE STREAM dim_railcard_stream_5 ON TABLE UBIO_DEMO.UBIO_SCHEMA."Dim_Station";
create or replace stream dim_time on table UBIO_DEMO.UBIO_SCHEMA."Dim_time";

// DROP THE STTEAM FOR ME 
DROP STREAM IF EXISTS dim_railcard_stream_1;
DROP STREAM IF EXISTS dim_railcard_stream_2;
DROP STREAM IF EXISTS dim_railcard_stream_3;
DROP STREAM IF EXISTS dim_railcard_stream_4;
DROP STREAM IF EXISTS dim_railcard_stream_5;
//



// create task for DIm_data
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



// create a store procedure for dim_ticket
CREATE OR REPLACE TASK my_store_procedure_dim_tickets
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '3 MINUTE'  
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_TICKETS_STREAM_3') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" AS Dim_ticket
    USING (
        SELECT 
            "Ticket_id",
            "TICKET_CLASS",
            "TICKET_TYPE",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_TICKETS_STREAM_3
    ) AS stream
    ON Dim_ticket."Ticket_id" = stream."Ticket_id"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_ticket."TICKET_CLASS" = stream."TICKET_CLASS",
            Dim_ticket."TICKET_TYPE" = stream."TICKET_TYPE"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Ticket_id", "TICKET_CLASS", "TICKET_TYPE")
        VALUES (
            stream."Ticket_id", 
            stream."TICKET_CLASS", 
            stream."TICKET_TYPE"
        );


// create taskUBIO_DEMO.UBIO_SCHEMA."Dim_time"
// create a store procedure for dim_ticket
CREATE OR REPLACE TASK my_store_procedure_dim_time
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '4 MINUTE'  
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_TIME') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_time" AS Dim_time
    USING (
        SELECT 
            "Time_id",
            "TIME_OF_PURCHASE",
            "Hour_of_Purchase",
            "Minute_of_Purchase",
            "Second_of_Purchase",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_TIME
    ) AS stream
    ON Dim_time."Time_id" = stream."Time_id"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_time."TIME_OF_PURCHASE" = stream."TIME_OF_PURCHASE",
            Dim_time."Hour_of_Purchase" = stream."Hour_of_Purchase",
            Dim_time."Minute_of_Purchase" = stream."Minute_of_Purchase",
            Dim_time."Second_of_Purchase" = stream."Second_of_Purchase"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Time_id", "TIME_OF_PURCHASE", "Hour_of_Purchase", "Minute_of_Purchase", "Second_of_Purchase")
        VALUES (
            stream."Time_id", 
            stream."TIME_OF_PURCHASE", 
            stream."Hour_of_Purchase", 
            stream."Minute_of_Purchase", 
            stream."Second_of_Purchase"
        );


// create task for UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys"
// create taskUBIO_DEMO.UBIO_SCHEMA."Dim_time"
// create a store procedure for dim_ticket
CREATE OR REPLACE TASK my_store_procedure_dim_journey
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'  
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_JOURNEYS_STREAM_4') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys" AS Dim_journey
    USING (
        SELECT 
            "Journey_id",
            "REASON_FOR_DELAY",
            "Departure_Time",
            "Arrival_Time",
            "Actual_Arrival_Time",
            "JOURNEY_STATUS",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_JOURNEYS_STREAM_4
    ) AS stream
    ON Dim_journey."Journey_id" = stream."Journey_id"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_journey."REASON_FOR_DELAY" = stream."REASON_FOR_DELAY",
            Dim_journey."Departure_Time" = stream."Departure_Time",
            Dim_journey."Arrival_Time" = stream."Arrival_Time",
            Dim_journey."Actual_Arrival_Time" = stream."Actual_Arrival_Time",
            Dim_journey."JOURNEY_STATUS" = stream."JOURNEY_STATUS"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Journey_id", "REASON_FOR_DELAY", "Departure_Time", "Arrival_Time", "Actual_Arrival_Time", "JOURNEY_STATUS")
        VALUES (
            stream."Journey_id", 
            stream."REASON_FOR_DELAY", 
            stream."Departure_Time", 
            stream."Arrival_Time", 
            stream."Actual_Arrival_Time", 
            stream."JOURNEY_STATUS"
        );
// create task for UBIO_DEMO.UBIO_SCHEMA."Date_ID_Departure"
CREATE OR REPLACE TASK my_store_procedure_Date_ID_Departure
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '6 MINUTE'  
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DATE_ID_DEPARTURE_STREAM_1') 
  AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Date_ID_Departure" AS Dim_Departure
    USING (
        SELECT 
            "Date_ID_Departure",
            "Date_of_Journey",
            "DAY_NAME_JOURNEY",
            "MONTH_NAME_JOURNEY",
            "Year_journey",
            "Quarter",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DATE_ID_DEPARTURE_STREAM_1
    ) AS stream
    ON Dim_Departure."Date_ID_Departure" = stream."Date_ID_Departure"

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_Departure."Date_of_Journey" = stream."Date_of_Journey",
            Dim_Departure."DAY_NAME_JOURNEY" = stream."DAY_NAME_JOURNEY",
            Dim_Departure."MONTH_NAME_JOURNEY" = stream."MONTH_NAME_JOURNEY",
            Dim_Departure."Year_journey" = stream."Year_journey",
            Dim_Departure."Quarter" = stream."Quarter"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Date_ID_Departure", "Date_of_Journey", "DAY_NAME_JOURNEY", "MONTH_NAME_JOURNEY", "Year_journey", "Quarter")
        VALUES (
            stream."Date_ID_Departure", 
            stream."Date_of_Journey", 
            stream."DAY_NAME_JOURNEY", 
            stream."MONTH_NAME_JOURNEY", 
            stream."Year_journey", 
            stream."Quarter"
        );

// create atask for my_store_procedure_Dim_Station        
CREATE OR REPLACE TASK my_store_procedure_Dim_Station
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '2 MINUTE'
  
  WHEN SYSTEM$STREAM_HAS_DATA('UBIO_DEMO.UBIO_SCHEMA.DIM_STATION_STREAM_5') 
AS
    MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Dim_Station" AS Dim_Station
    USING (
        SELECT 
            "Station_id",
            "Departure_Station",
            "Arrival_Destination",
            "DAY_NAME_JOURNEY",
            METADATA$ACTION,
            METADATA$ISUPDATE
        FROM UBIO_DEMO.UBIO_SCHEMA.DIM_STATION_STREAM_5
    ) AS stream
    ON Dim_Station."Station_id" = stream."Station_id" -- Adjust this to the correct matching column

    -- Update when matched and action is 'INSERT' and isupdate is false
    WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = FALSE THEN
        UPDATE SET 
            Dim_Station."Departure_Station" = stream."Departure_Station",
            Dim_Station."Arrival_Destination" = stream."Arrival_Destination",
            Dim_Station."DAY_NAME_JOURNEY" = stream."DAY_NAME_JOURNEY"

    -- Insert when not matched and action is 'INSERT'
    WHEN NOT MATCHED AND stream.METADATA$ACTION = 'INSERT' THEN
        INSERT ("Station_id", "Departure_Station", "Arrival_Destination", "DAY_NAME_JOURNEY")
        VALUES (
            stream."Station_id", 
            stream."Departure_Station", 
            stream."Arrival_Destination", 
            stream."DAY_NAME_JOURNEY"
        );

// show all task create  for tha fact table 
CREATE OR REPLACE TASK my_fact_table_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '7 MINUTE'  
AS
  MERGE INTO UBIO_DEMO.UBIO_SCHEMA."Fact_Table" AS fact_table
  USING (
    SELECT
      uk."Ticket_id" AS Fact_id,
      uk.price::int AS price,
      RC."RAILCARD_ID",
      dm_t."Ticket_id" AS ticket_id,
      st_t."Station_id",
      din_j."Journey_id",
      dim_date."Date_id",
      dim_t."Time_id",
      date_id."Date_ID_Departure",
      payment_table.PAYMENT_ID
    FROM 
      UBIO_DEMO.UBIO_SCHEMA.UK_TRAIN_TABLE uk
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Dim_railcard" rc
      ON uk."Ticket_id" = rc."RAILCARD_ID"
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" dm_t
      ON uk."Ticket_id" = dm_t."Ticket_id"
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Dim_Station" st_t
      ON uk."Ticket_id" = st_t."Station_id"
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys" din_j
      ON uk."Ticket_id" = din_j."Journey_id"
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Dim_time" dim_t
      ON uk."Ticket_id" = dim_t."Time_id"
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Dim_date" dim_date
      ON uk."Ticket_id" = dim_date."Date_id"
    INNER JOIN
      UBIO_DEMO.UBIO_SCHEMA."Date_ID_Departure" date_id
      ON uk."Ticket_id" = date_id."Date_ID_Departure"
    inner join UBIO_DEMO.UBIO_SCHEMA.PAYMENT_TYPE payment_table
     on uk."Ticket_id"=payment_table.PAYMENT_ID
  ) AS source
  ON fact_table."Fact_id" = source."Fact_id"

  -- Update when matched and action is 'INSERT' and isupdate is false
  WHEN MATCHED AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = FALSE THEN
    UPDATE SET 
      fact_table."price" = source."price",
      fact_table."RAILCARD_ID" = source."RAILCARD_ID",
      fact_table."ticket_id" = source."ticket_id",
      fact_table."Station_id" = source."Station_id",
      fact_table."Journey_id" = source."Journey_id",
      fact_table."Date_id" = source."Date_id",
      fact_table."Time_id" = source."Time_id",
      fact_table."Date_ID_Departure" = source."Date_ID_Departure",
      fact_table.PAYMENT_ID=source.PAYMENT_ID

  -- Insert when not matched and action is 'INSERT'
  WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
    INSERT (
      "Fact_id", "price", "RAILCARD_ID", "ticket_id", 
      "Station_id", "Journey_id", "Date_id", "Time_id", "Date_ID_Departure"
    )https://co40859.eu-west-3.aws.snowflakecomputing.com
    VALUES (
      source."Fact_id", 
      source."price", 
      source."RAILCARD_ID",  
      source."ticket_id", 
      source."Station_id", 
      source."Journey_id", 
      source."Date_id", 
      source."Time_id", 
      source."Date_ID_Departure",
      source.PAYMENT_ID
      
    );


