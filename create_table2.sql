create schema uk_schema_curated;
create or replace sequence my_sq start 1 increment 1
;

// create the dim table from the main table 
create or replace table Dim_railcard as
select 
    my_sq.nextval as Railcard_ID,
   "Railcard" as Railcard_type
from UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

// check if data is loaded 
select * from Dim_railcard

// create the ticket table 
create or replace table Dim_Tickets
as 
select 
    my_sq.nextval as Ticket_ID,
    "Ticket Class" as Ticket_Class,
    "Ticket Type" as Ticket_Type
from 
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;


// create  station tabl
create or replace table Dim_Station
as
select 
    my_sq.nextval as Station_ID,
    "Departure Station" as Dp_Station,
    "Arrival Destination" as Arr_station
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;
select * from  Dim_Station

// create  the journey table 
create or replace table   Dim_Journeys
as
SELECT
    my_sq.nextval as Journey_ID,
"Date of Journey" as Date_of_Journey,
"Departure Time" as Departure_Time,
"Arrival Time" as Arrival_Time,
"Actual Arrival Time" as Actual_Arrival_Time,
"Journey Status" as Journey_Status
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

//  create the refund table 
create or replace table Dim_Refunds
as 
SELECT 
    my_sq.nextval as Refund_ID,
    "Reason for Delay" as Reason_for_Delay,
    "Refund Request" as Refund_Request
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

//create  table Dim_Date
create or replace table Dim_Date
as
 select 
    my_sq.nextval as Date_ID,
    "Date of Purchase" as Date_of_Purchase,
    dayname(to_date("Date of Purchase")) as day_name ,
    monthname(to_date("Date of Purchase")) as month_name,
     year(to_date("Date of Purchase")) as Year,
     QUARTER(to_date("Date of Purchase")) as Quanter,
    HOUR("Time of Purchase"::time) as Hour_of_Purchase
   
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

 select * from Dim_Date

 // create the time table 
 create or replace table  Dim_Time
 as
select 
    my_sq.nextval as  Time_ID,
    HOUR("Time of Purchase"::time) as Hour_of_Purchase,
    Minute("Time of Purchase"::time) as minute_of_Purchase,
    second("Time of Purchase"::time) as second_of_Purchase
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;

//create the fact table 
create or replace table Dim_Date_Departure
as
 select 
    my_sq.nextval as Date_ID_Departure,
    "Date of Purchase" as Date_of_Purchase,
    dayname(to_date("Date of Journey")) as day_name_journey ,
    monthname(to_date("Date of Journey")) as month_name_journey,
     year(to_date("Date of Journey")) as Year_journey,
     QUARTER(to_date("Date of Journey")) as Quanter,
    HOUR("Departure Time"::time) as Departure_Time
   
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY;
    
ALTER WAREHOUSE compute_wh SET WAREHOUSE_SIZE = 'SMALL';

// create the fact table
 select
    MY_sq.nextval as id, 
    *
     
from    
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY uk
left join
    UK_DEMO.UK_SCHEMA_curated.Dim_railcard Dim_railcard
    on  uk."Railcard"=Dim_railcard.RAILCARD_TYPE

left join
    UK_DEMO.UK_SCHEMA_curated.Dim_Tickets
    on  uk."Ticket Class"=Dim_Tickets.Ticket_Class
    AND  uk."Ticket Type"=Dim_Tickets.Ticket_Type
    
left join
    select
    *
    from UK_DEMO.UK_SCHEMA_curated.Dim_Station
    
left join
    select
    *
    from UK_DEMO.UK_SCHEMA_curated.Dim_Journeys
left join
    select
    *
    from UK_DEMO.UK_SCHEMA_curated.Dim_Refunds

left join
    select
    *
    from UK_DEMO.UK_SCHEMA_curated.Dim_Date

left join
    select
    *
    from UK_DEMO.UK_SCHEMA_curated.Dim_Date
    




create or replace TABLE  fact_dimse
as
select
my_sq.nextval as fact_id,
"Price" as Ticket_price
from
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY uk
left join
 UK_DEMO.UK_SCHEMA_curated.Dim_railcard Dim_railcard
   on  uk."Railcard"=Dim_railcard.Railcard_type

select
UUID_STRING()as fact_id,
"Price" as Ticket_price
from
    UK_DEMO.UK_SCHEMA.UK_RAILWAY_COMPANY
gen_random_uuid()

select * from UBIO_DEMO.UBIO_SCHEMA."UK_TRAIN_TABLE";
create or replace table  fact_table 
as 
select
uk."Ticket_id" as Fact_id,
uk.price:: int as price,
RC.RAILCARD_ID,
dm_t."Ticket_id",
st_t."Station_id",
din_j."Journey_id",
dim_date."Date_id",
dim_t."Time_id",
date_id."Date_ID_Departure",
payment_table.PAYMENT_ID
from 
    UBIO_DEMO.UBIO_SCHEMA.UK_TRAIN_TABLE uk
INNER join
    UBIO_DEMO.UBIO_SCHEMA."Dim_railcard" rc
    on uk."Ticket_id"=rc."RAILCARD_ID"
inner join
    UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" dm_t
    on uk."Ticket_id"=dm_t."Ticket_id"
 join UBIO_DEMO.UBIO_SCHEMA."Dim_Station" st_t
    on  uk."Ticket_id"=st_t."Station_id"
join UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys" din_j
 on uk."Ticket_id"=din_j."Journey_id"
 join UBIO_DEMO.UBIO_SCHEMA."Dim_time" dim_t
 on uk."Ticket_id"=dim_t."Time_id"
 join UBIO_DEMO.UBIO_SCHEMA."Dim_date" dim_date
 on uk."Ticket_id"=dim_date."Date_id"
 join UBIO_DEMO.UBIO_SCHEMA."Date_ID_Departure" date_id
 on uk."Ticket_id"=date_id."Date_ID_Departure"
 join UBIO_DEMO.UBIO_SCHEMA.PAYMENT_TYPE payment_table
 on uk."Ticket_id"=payment_table.PAYMENT_ID
    


 select 
 count(cast(fact_table.railcard_id as int)),
avg(cast(fact_table.price as int) ) as price,
"Dim_railcard".RAILCARD
 
 from  fact_table
 join "Dim_railcard"
 on fact_table.railcard_id="Dim_railcard".railcard_id
 group by "Dim_railcard".RAILCARD

// create a stream of the various table to track change
select * from UBIO_DEMO.UBIO_SCHEMA.UK_TRAIN_TABLE
create or replace sequence my_sq start 0 increment =1 

 create or replace table payment_type
 as 
  select 
  my_sq.nextval as payment_id,
    PURCHASE_TYPE,
    PAYMENT_METHOD
from UBIO_DEMO.UBIO_SCHEMA.UK_TRAIN_TABLE
    
