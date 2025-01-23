--set compute_price = 2.32;
--set storage_price = 16.86;

show warehouses in account;
with usage_data as(
select 
WAREHOUSE_NAME,
DATE_TRUNC('week',START_TIME) as week_start,
SUM(CREDITS_USED) as total_credits
from 
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE
WAREHOUSE_NAME IN (select "name" from table(result_scan(last_query_id())))
AND START_TIME >= DATEADD('week',-4,DATE_TRUNC('week',CURRENT_DATE))
GROUP BY
WAREHOUSE_NAME,
DATE_TRUNC('week',START_TIME)
),
pivot_data as (
select
    WAREHOUSE_NAME,
    CASE 
    WHEN week_start = DATE_TRUNC('week',current_date) then 'current_week'
    WHEN week_start = DATEADD(week,-1,DATE_TRUNC('week',CURRENT_DATE)) then 'week-1'
    WHEN week_start = DATEADD(week,-2,DATE_TRUNC('week',CURRENT_DATE)) then 'week-2'
    WHEN week_start = DATEADD(week,-3,DATE_TRUNC('week',CURRENT_DATE)) then 'week-3'
    WHEN week_start = DATEADD(week,-4,DATE_TRUNC('week',CURRENT_DATE)) then 'week-4'
    END as week_bucket,
    total_credits
FROM
    usage_data
WHERE
    week_start >= DATEADD(week,-4,DATE_TRUNC('week',CURRENT_DATE))
),
aggregated_data as (
SELECT
    WAREHOUSE_NAME,
    MAX(CASE WHEN week_bucket = 'week-4' THEN total_credits ELSE 0 END) as week_4,
    MAX(CASE WHEN week_bucket = 'week-3' THEN total_credits ELSE 0 END) as week_3,
    MAX(CASE WHEN week_bucket = 'week-2' THEN total_credits ELSE 0 END) as week_2,
    MAX(CASE WHEN week_bucket = 'week-1' THEN total_credits ELSE 0 END) as last_week,
FROM
pivot_data
GROUP BY
WAREHOUSE_NAME
)
select
    WAREHOUSE_NAME,
    week_4,
    week_3,
    week_2,
    last_week,
    CASE WHEN week_2 > 0 THEN ROUND(((last_week - week_2)/week_2)*100,2)
    ELSE NULL END as percent_change
FROM
aggregated_data order by percent_change desc


------------++++++++++++++  STORAGE  ++++++++++++++++++

with storage_data as (
select 
    ID,
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    ROUND(sum(active_bytes)/1099511627776,10) as TOTAL_ACTIVE_BYTES_IN_TB,
    ROUND(sum(active_bytes)/1099511627776,10) * 16.86 as TOTAL_ACTIVE_COST_PER_MONTH,
    ROUND(sum(FAILSAFE_BYTES)/1099511627776,10) as TOTAL_FAILSAFE_BYTES_IN_TB,
    ROUND(sum(FAILSAFE_BYTES)/1099511627776,10) * 16.86 as TOTAL_FAILSAFE_COST_PER_MONTH,
    ROUND(sum(TIME_TRAVEL_BYTES)/1099511627776,10) as TOTAL_TIME_TRAVEL_BYTES_IN_TB,
    ROUND(sum(TIME_TRAVEL_BYTES)/1099511627776,10) * 16.86 as TOTAL_TIME_TRAVEL_COST_PER_MONTH
FROM
    SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE DELETED = 'FALSE'
-- AND DATE_TRUNC('month',TABLE_CREATED) <= DATE_TRUNC('month',dateadd('month',-1,current_date()))
--AND DATE_TRUNC('year',TABLE_CREATED) <= DATE_TRUNC('year',dateadd('month',-1,current_date()))
GROUP BY ID,TABLE_CATALOG,TABLE_SCHEMA,TABLE_NAME
),
auto_cluster_data as (
SELECT 
  TABLE_ID,
  database_name,
  schema_name,
  table_name,
  SUM(credits_used) AS credits_used
FROM snowflake.account_usage.automatic_clustering_history
WHERE DATE_TRUNC('month',start_time) = DATE_TRUNC('month',DATEADD(month,-1,CURRENT_TIMESTAMP()))
GROUP BY 1,2,3,4
),
agg as (
select ST.*,ACT.credits_used as AUTO_CLUSTERING_CREDITS,
ACT.credits_used * 2.32 as AUTO_CLUSTERING_COST,
from storage_data ST left join auto_cluster_data ACT 
on ST.ID = ACT.TABLE_ID
)
select TABLE_CATALOG,
TABLE_SCHEMA,
TABLE_NAME,
TOTAL_ACTIVE_BYTES_IN_TB,
TOTAL_ACTIVE_COST_PER_MONTH,
TOTAL_FAILSAFE_BYTES_IN_TB,
TOTAL_FAILSAFE_COST_PER_MONTH,
TOTAL_TIME_TRAVEL_BYTES_IN_TB,
TOTAL_TIME_TRAVEL_COST_PER_MONTH,
CASE WHEN AUTO_CLUSTERING_CREDITS is null then 0 else AUTO_CLUSTERING_CREDITS END as TOTAL_AUTO_CLUSTERING_CREDITS,
CASE WHEN AUTO_CLUSTERING_COST is null then 0 else AUTO_CLUSTERING_COST END as TOTAL_AUTO_CLUSTERING_COST
from agg
ORDER BY 1,2,3