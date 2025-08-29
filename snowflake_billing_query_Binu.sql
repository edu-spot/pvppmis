use role accountadmin;
use WAREHOUSE ADMIN_LARGE_WH;
use DATABASE SNOWFLAKE;
use SCHEMA ACCOUNT_USAGE;
set compute_price = 2.40;
set storage_price = 23;
set report_months = 1;
--COMPUTE
select current_account() account_name, WAREHOUSE_NAME AS WH_DB_NAME, 'COMPUTE' as SERVICE_TYPE, date_trunc(month, start_time) as USAGE_MONTH,
sum(coalesce(CREDITS_USED_COMPUTE, 0.00)) as TOTAL_CREDITS,
sum($compute_price * coalesce(CREDITS_USED_COMPUTE, 0.00)) as BILLABLE_USAGE
from snowflake.account_usage.warehouse_metering_history
where start_time >= date_trunc(month, dateadd(month, -$report_months, current_timestamp)) and start_time < date_trunc(month, current_timestamp)
-- WHERE  date_trunc(month, start_time) = '2021-01-01 00:00:00.000 -0500'
group by 1,2,3,4
having BILLABLE_USAGE > 0
UNION ALL
--CLOUD SERVICES
select current_account() account_name, name AS WH_DB_NAME,case when Service_Type='WAREHOUSE_METERING' THEN 'CLOUD SERVICES' ELSE SERVICE_TYPE END as SERVICE_TYPE, date_trunc(month, start_time) as USAGE_MONTH, 
sum(case when Service_Type='WAREHOUSE_METERING' THEN CREDITS_USED_CLOUD_SERVICES ELSE CREDITS_USED_COMPUTE END ) as TOTAL_CREDITS,
sum($compute_price * coalesce(case when Service_Type='WAREHOUSE_METERING' THEN CREDITS_USED_CLOUD_SERVICES ELSE CREDITS_USED_COMPUTE END, 0.00)) as BILLABLE_USAGE
FROM ACCOUNT_USAGE.METERING_HISTORY --METERING_DAILY_HISTORY 
where start_time >= date_trunc(month, dateadd(month, -$report_months, current_timestamp)) and start_time < date_trunc(month, current_timestamp) 
-- WHERE  date_trunc(month, start_time) = '2021-01-01 00:00:00.000 -0500'
and Service_Type NOT IN ('SEARCH_OPTIMIZATION', 'AUTO_CLUSTERING','MATERIALIZED_VIEW')
group by 1,2,3,4
having BILLABLE_USAGE > 0
UNION ALL
--SEARCH_OPTIMIZATION
select current_account() account_name, B.TABLE_CATALOG AS WH_DB_NAME,SERVICE_TYPE, date_trunc(month, start_time) as USAGE_MONTH, 
sum(CREDITS_USED_COMPUTE) as TOTAL_CREDITS,
sum($compute_price * coalesce(CREDITS_USED_COMPUTE, 0.00)) as BILLABLE_USAGE
FROM ACCOUNT_USAGE.METERING_HISTORY A --METERING_DAILY_HISTORY 
JOIN (SELECT DISTINCT TABLE_ID, TABLE_CATALOG FROM ACCOUNT_USAGE.TABLES) B ON SUBSTR(A.NAME,33,10) = B.TABLE_ID
where start_time >= date_trunc(month, dateadd(month, -$report_months, current_timestamp)) and start_time < date_trunc(month, current_timestamp) 
--WHERE  date_trunc(month, start_time) = '2021-01-01 00:00:00.000 -0500'
and Service_Type IN ('SEARCH_OPTIMIZATION')
group by 1,2,3,4
having BILLABLE_USAGE > 0
UNION ALL
--AUTO_CLUSTERING
select current_account() account_name, B.TABLE_CATALOG AS WH_DB_NAME,SERVICE_TYPE, date_trunc(month, start_time) as USAGE_MONTH, 
sum(CREDITS_USED_COMPUTE) as TOTAL_CREDITS,
sum($compute_price * coalesce(CREDITS_USED_COMPUTE, 0.00)) as BILLABLE_USAGE
FROM ACCOUNT_USAGE.METERING_HISTORY A --METERING_DAILY_HISTORY 
JOIN (SELECT DISTINCT TABLE_ID, TABLE_CATALOG FROM ACCOUNT_USAGE.TABLES) B ON A.ENTITY_ID = B.TABLE_ID
where start_time >= date_trunc(month, dateadd(month, -$report_months, current_timestamp)) and start_time < date_trunc(month, current_timestamp) 
--WHERE  date_trunc(month, start_time) = '2021-01-01 00:00:00.000 -0500'
and Service_Type IN ('AUTO_CLUSTERING')
group by 1,2,3,4
having BILLABLE_USAGE > 0
UNION ALL
--MATERIALIZED_VIEW
select current_account() account_name, DATABASE_NAME AS WH_DB_NAME,'MATERIALIZED_VIEW' as SERVICE_TYPE, date_trunc(month, start_time) as USAGE_MONTH, 
sum(CREDITS_USED) as TOTAL_CREDITS,
sum($compute_price * coalesce(CREDITS_USED, 0.00)) as BILLABLE_USAGE
FROM ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
where start_time >= date_trunc(month, dateadd(month, -$report_months, current_timestamp)) and start_time < date_trunc(month, current_timestamp) 
--WHERE  date_trunc(month, start_time) = '2021-01-01 00:00:00.000 -0500'
and Service_Type IN ('MATERIALIZED_VIEW')
group by 1,2,3,4
having BILLABLE_USAGE > 0
UNION ALL
--STORAGE
select current_account() account_name, DATABASE_NAME  AS WH_DB_NAME, 'STORAGE' AS SERVICE_TYPE, date_trunc(month, usage_date) as USAGE_MONTH, 
round(avg(AVERAGE_DATABASE_BYTES + AVERAGE_FAILSAFE_BYTES)/power(1024, 4), 3) as TOTAL_CREDITS,
$storage_price * round(avg(AVERAGE_DATABASE_BYTES + AVERAGE_FAILSAFE_BYTES)/power(1024, 4), 3) as BILLABLE_USAGE
from snowflake.account_usage.DATABASE_storage_usage_HISTORY
where usage_date >= date_trunc(month, dateadd(month, -$report_months, current_timestamp)) and usage_date < date_trunc(month, current_timestamp)
--WHERE  date_trunc(month, start_time) = '2021-01-01 00:00:00.000 -0500'
group by 1,2,3,4
having BILLABLE_USAGE > 0
ORDER BY 4,1,2,3;
