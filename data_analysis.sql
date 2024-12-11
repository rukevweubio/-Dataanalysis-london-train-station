use database ubio_demo;

use schema ubio_schema;

//Calculate the Average Ticket Price for Each Ticket
select round( avg(price),2) as average_price,
t."TICKET_TYPE"
from 
    fact_table f
join UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" as t
on
    f.fact_id=t."Ticket_id"
group by "TICKET_TYPE"
order by average_price desc

 
// Count of Refund Requests by Reason for Delay
WITH reason_for_delay AS (
    SELECT 
        COUNT(*) AS total_number_of_failure, 
        t."REASON_FOR_DELAY"
    FROM 
        fact_table f
    JOIN UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys" t
        ON f.fact_id = t."Journey_id"
    GROUP BY t."REASON_FOR_DELAY"
    ORDER BY total_number_of_failure DESC
)
SELECT 
    reason_for_delay.total_number_of_failure,
    CASE 
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Weather' THEN 'Weather'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Technical Issue' THEN 'Technical Issue'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Signal Failure' THEN 'Signal Failure'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Signal failure' THEN 'Signal failure'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Staffing' THEN 'Staffing'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Staff Shortage' THEN 'Staff Shortage'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Weather Conditions' THEN 'Weather Conditions'
        WHEN reason_for_delay."REASON_FOR_DELAY" = 'Traffic' THEN 'Traffic'
        ELSE 'no reason'
    END AS reason_for_delay
FROM 
    reason_for_delay;
//Total Revenue and Average Ticket Price by Payment Method
 select round( avg(price),2)as average_price, sum(price)  as total_revenue ,
t."TICKET_TYPE",
p.payment_method
from 
    fact_table f
join UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" as t
on
    f.fact_id=t."Ticket_id"
join payment_type p
on f.fact_id=p.payment_id
group by "TICKET_TYPE",payment_method
order by average_price desc


select * from UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys"

// Percentage of Delayed Journeys by Departure Station

select
    count(*) as total_amount_travel,
    sum(case when JOURNEY_STATUS = 'Delayed' then 1 else 0 end) as delay,
    sum(case when JOURNEY_STATUS = 'On Time' then 1 else 0 end) as on_time,
    sum(case when JOURNEY_STATUS not in ('Delayed', 'On Time') then 1 else 0 end) as unknown,
    delay/total_amount_travel*100 as  percentage_of_delay
from UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys";

//Total Number of Journeys and Delays by Ticket Class
select 
    count(*) as total_number_journey,
    t."TICKET_TYPE",
    sum(case when dim_j.JOURNEY_STATUS = 'Delay' then 1 else 0 end) as total_number_of_delay
from 
    fact_table f
join UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" t
    on f.fact_id = t."Ticket_id"
join UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys" dim_j
    on f.fact_id = dim_j."Journey_id"
group by t."TICKET_TYPE";

//Calculate the Average Delay Time for Each Journey
 select
    ---"Journey_id",
    datediff(hour,"Actual_Arrival_Time","Arrival_Time") as average_delay_time
from 
    UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys"
    where "JOURNEY_STATUS"='Delay';
    


// test the stream of the table 
// select from stream 
select * from UBIO_DEMO.UBIO_SCHEMA.DIM_JOURNEYS_STREAM_4;
 select * from UBIO_DEMO.UBIO_SCHEMA.DIM_STATION_STREAM_5;
select * from  UBIO_DEMO.UBIO_SCHEMA.DIM_TICKETS_STREAM_3;

 
// alter the task to load the data 
alter task UBIO_DEMO.UBIO_SCHEMA.MY_STORE_PROCEDURE_DIM_JOURNEY resume;
alter task UBIO_DEMO.UBIO_SCHEMA.MY_STORE_PROCEDURE_DIM_STATION resume;
alter task UBIO_DEMO.UBIO_SCHEMA.MY_STORE_PROCEDURE_DIM_TICKETS resume;
 
// suspend the stream on each table 
alter task UBIO_DEMO.UBIO_SCHEMA.MY_STORE_PROCEDURE_DIM_JOURNEY suspend;
alter task UBIO_DEMO.UBIO_SCHEMA.MY_STORE_PROCEDURE_DIM_STATION suspend;
alter task UBIO_DEMO.UBIO_SCHEMA.MY_STORE_PROCEDURE_DIM_TICKETS suspend;


 // select  from table 
  select * from UBIO_DEMO.UBIO_SCHEMA."Dim_Journeys";
  select * from UBIO_DEMO.UBIO_SCHEMA."Dim_Station";
  select * from UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets"

// show stream and table 
 show tasks


insert into ubio_schema."Dim_Journeys" ("Journey_id", "REASON_FOR_DELAY", "Departure_Time", "Arrival_Time", "Actual_Arrival_Time", "JOURNEY_STATUS")
values 
(3, 'Signal Failure', '09:45:00', '11:35:00', '11:40:00', 'Delayed'),
(4, '', '18:15:00', '18:45:00', '18:45:00', 'On Time'),
(6, '', '21:30:00', '22:30:00', '22:30:00', 'On Time'),
(9, '', '16:45:00', '19:00:00', '19:00:00', 'On Time');

// insert statement for  UBIO_DEMO.UBIO_SCHEMA."Dim_Station" table 
insert into UBIO_DEMO.UBIO_SCHEMA."Dim_Station" ("Station_id", "Departure_Station", "Arrival_Destination")
values 
(1, 'London Kings Cross', 'York'),
(2, 'Liverpool Lime Street', 'Manchester Piccadilly'),
(3, 'London Paddington', 'Reading'),
(4, 'London Kings Cross', 'York'),
(5, 'Liverpool Lime Street', 'London Euston');

// insert into table UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets"
insert into UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets" ("Ticket_id", "TICKET_CLASS", "TICKET_TYPE")
values 
(1, 'Standard', 'Advance'),
(2, 'First Class', 'Advance'),
(3, 'Standard', 'Anytime'),
(4, 'First Class', 'Anytime'),
(5, 'Standard', 'Off-Peak');


;

show columns in table UBIO_DEMO.UBIO_SCHEMA."Dim_Tickets"






 

       
