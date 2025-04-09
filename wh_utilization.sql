with data_7_days as (select * from snowflake.account_usage.warehouse_load_history where WAREHOUSE_NAME LIKE '%EDB%' and date(start_time) > dateadd(days,-7,current_date()) order by start_time desc),
metrics_stats as (
select warehouse_name,
datediff('seconds',start_time,end_time) as wh_uptime_seconds,
wh_uptime_seconds * (avg_running + avg_queued_load + avg_queued_provisioning + avg_blocked) as running_time,
wh_uptime_seconds * (1 - (avg_running + avg_queued_load + avg_queued_provisioning + avg_blocked)) as idle_time
from
data_7_days
)
select warehouse_name,
sum(wh_uptime_seconds/60) as total_wh_uptime_mins,
sum(running_time/60) as total_running_mins,
sum(idle_time/60) as total_idle_mins
from 
metrics_stats
group by 
warehouse_name




----------------------------------------------------------

WITH data_7_days AS (
    SELECT * 
    FROM snowflake.account_usage.warehouse_load_history 
    WHERE WAREHOUSE_NAME LIKE '%EDB%' 
    AND DATE(start_time) > DATEADD(DAY, -7, CURRENT_DATE()) 
    ORDER BY start_time DESC
),
metrics_stats AS (
    SELECT 
        TO_DATE(start_time) AS usage_date, -- Extract the date part of start_time
        DATEDIFF('seconds', start_time, end_time) AS wh_uptime_seconds,
        wh_uptime_seconds * (avg_running + avg_queued_load + avg_queued_provisioning + avg_blocked) AS running_time,
        wh_uptime_seconds * (1 - (avg_running + avg_queued_load + avg_queued_provisioning + avg_blocked)) AS idle_time
    FROM
        data_7_days
)
SELECT 
    usage_date,  -- Group by date only
    SUM(wh_uptime_seconds / 60) AS total_wh_uptime_mins,
    SUM(running_time / 60) AS total_running_mins,
    SUM(idle_time / 60) AS total_idle_mins
FROM 
    metrics_stats
GROUP BY 
    usage_date
ORDER BY 
    usage_date DESC;
