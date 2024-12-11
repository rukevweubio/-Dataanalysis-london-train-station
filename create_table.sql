create database uk_demo;
create schema uk_schema;

use database uk_demo;
use schema uk_schema;
// create file  format for the file 
create or replace file format my_uk_file_csv
type ='csv'
field_delimiter =','
skip_header=2;

// create internal stage for the file 
create or replace stage uk_internal_stage
file_format=(format_name=my_uk_file_csv);


// create a variant table
create or replace table new_table(
data variant
)

//// load data into stage from the snowsql cli
list @uk_internal_stage;


// create the staging table
copy into new_table
from @uk_internal_stage
file_format = (format_name = my_uk_file_csv)
pattern = '.*railway.csv.gz.*'
on_error = 'continue';

// create a sequqnce to  form the primary key of the table 
create or replace sequence my_sequence 
    start = 1 
    increment = 1;
// create a table  and load the data from the stage 
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