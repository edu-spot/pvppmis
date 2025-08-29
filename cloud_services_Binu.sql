select (date_trunc('Year',current_date) -184)
select last_day(add_months(current_date,-1))

select max(usage_date), min(usage_date) from snowflake.organization_usage.usage_in_currency_daily

--drop table DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES

select * from SNOWFLAKE.ORGANIZATION_USAGE.RATE_SHEET_DAILY
where service_type = 'STORAGE'
AND DATE = '2025-08-18'
where date = current_date order by account_name

---------------------------------------


create or replace table DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES
AS
(
select account_name,Service_type, date_trunc(Month,usage_date) UsageMonth,round(sum(usage_in_currency),3) Billable_Usage
from snowflake.organization_usage.usage_in_currency_daily
where usage_date >= (date_trunc('Year',current_date) -184) and usage_date <=  last_day(add_months(current_date,-1))
and account_name not in ('DMKTP_AWS_US_EAST_1','UAT_DMKTP_AWS_US_EAST_1','WUDNA_AWS_US_WEST_2')
GROUP BY 1,2,3
order by 1,2,3
);

select max(UsageMonth), min(UsageMonth) from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES

select * from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES

select * from snowflake.organization_usage.usage_in_currency_daily limit 10
where service_type = 'WAREHOUSE_METERING' and usage_date in ('2025-04-20', '2025-07-01')

create or replace table DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_1 as
(
select account_name,service_type, year(Usagemonth)||'-'||monthname(Usagemonth) as us_month, 
case when sum(billable_usage) is null then 0 else sum(billable_usage) end as bill_usage
from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES
group by account_name,service_type,us_month
);

create or replace table DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_1_5 as
(
select account_name,service_type, usagemonth,
case when sum(billable_usage) is null then 0 else sum(billable_usage) end as bill_usage
from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES
group by account_name,service_type,usagemonth
);

select * from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_1_5 
//month view 

select * from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_1

alter table DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_1 drop column account_name 


create or replace table DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_2 as
(
select 
service_type,
case when jul_24 is null then 0 else jul_24 end jul_24,
case when aug_24 is null then 0 else aug_24 end aug_24,
case when sep_24 is null then 0 else sep_24 end sep_24,
case when oct_24 is null then 0 else oct_24 end oct_24,
case when nov_24 is null then 0 else nov_24 end nov_24,
case when dec_24 is null then 0 else dec_24 end dec_24,
case when jan_25 is null then 0 else jan_25 end jan_25,
case when feb_25 is null then 0 else feb_25 end feb_25,
case when mar_25 is null then 0 else mar_25 end mar_25,
case when apr_25 is null then 0 else apr_25 end apr_25,
case when may_25 is null then 0 else may_25 end may_25,
case when jun_25 is null then 0 else jun_25 end jun_25,
case when jul_25 is null then 0 else jul_25 end jul_25
from 
(
select service_type,jul_24,aug_24,sep_24,Oct_24,nov_24,dec_24,jan_25,feb_25,mar_25,apr_25,may_25,jun_25,jul_25 from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_1
pivot(SUM(bill_Usage) for us_month IN ('2024-Jul','2024-Aug','2024-Sep','2024-Oct','2024-Nov','2024-Dec','2025-Jan','2025-Feb','2025-Mar','2025-Apr','2025-May','2025-Jun','2025-Jul')) as P
(service_type,jul_24,Aug_24,Sep_24,Oct_24,Nov_24,Dec_24,Jan_25,Feb_25,Mar_25,Apr_25,May_25,Jun_25,Jul_25)
)
);

select * from DBA_DB.PUBLIC.WH_SNOWFLAKE_SERVICES_2


===================================================

-- Query to find the total storage, fail safe cost to update in Ashwini's sheet 
-- change date next month start date 
-- download the file and add the FS cost and DB storage cost 


set timezone = 'UTC';

with daily as (											
select account_name,date_trunc(month, usage_date) as USAGE_MONTH,database_name, max(database_id) as object_id,	
max(AVERAGE_DATABASE_BYTES + AVERAGE_HYBRID_TABLE_STORAGE_BYTES) as database_storage_bytes,										max(AVERAGE_FAILSAFE_BYTES) as failsafe_storage_bytes,											
from snowflake.organization_usage.database_storage_usage_history 											
where usage_date >= TO_TIMESTAMP_LTZ('2024-06-01T00:00:00Z', 'auto') and usage_date < TO_TIMESTAMP_LTZ('2025-08-01T00:00:00Z', 'auto') 
--and account_name in ('WUDATA')	
--and database_name in ('USR_FINANCE')
and account_name not in ('DMKTP_AWS_US_EAST_1','UAT_DMKTP_AWS_US_EAST_1','WUDNA_AWS_US_WEST_2')			
group by 1,2,3											
)
, com_db as (
select account_name,USAGE_MONTH,
sum(database_storage_bytes) as db_storage_bytes,
sum(failsafe_storage_bytes) as fs_storage_bytes,
from daily
group by 1,2
), tot_sum_db as (
select 
account_name, 											
USAGE_MONTH, 											
round(avg(db_storage_bytes) / power(2, 40), 1) as db_storage_tb,											
round(avg(fs_storage_bytes) / power(2, 40), 1) as fs_storage_tb,
18.40 * db_storage_tb as database_storage_cost,										
18.40 * fs_storage_tb as failsafe_storage_cost						
from com_db 
--where usage_month in ('2025-07-01')
group by 1,2) 
select usage_month,
sum(db_storage_tb) as tot_db_storage_tb,
sum(fs_storage_tb) as tot_fs_storage_tb,
sum(database_storage_cost) as tot_database_storage_cost,
sum(failsafe_storage_cost) as tot_failsafe_storage_cost
from tot_sum_db
group by 1


==================================================================

