-----------------------------------
---- For Account Usage------------
-----------------------------------

create or replace temporary table t_accout_bil_usage
as
select account_name, date_trunc(Month,usage_date) UsageMonth,round(sum(usage_in_currency),3) Billable_Usage
from snowflake.organization_usage.usage_in_currency_daily
where usage_date >= date_trunc('Year',current_date) and usage_date <= last_day(add_months(current_date,-1))
GROUP BY 1,2
order by 1,2;


create or replace temporary table t_usage
as
select 
account_name,
year(usagemonth)||'-'||monthname(usagemonth) as Month,billable_usage 
from t_accout_bil_usage;

-- select distinct month from t_usage order by month

--- pivot
-- -- Get the Results by Month For the Accounts

select * from t_usage
pivot(SUM(Billable_Usage) for Month IN ('2024-Jan','2024-Feb','2024-Mar','2024-Apr','2024-May','2024-Jun','2024-Jul','2024-Aug','2024-Sep','2024-Oct','2024-Nov')) as P
(Account_Name,Jan_24,Feb_24,Mar_24,Apr_24,May_24,Jun_24,Jul_24,Aug_24,Sep_24,Oct_24,Nov_24)
where account_name not in ('DMKTP_AWS_US_EAST_1','UAT_DMKTP_AWS_US_EAST_1','WUDNA_AWS_US_WEST_2')
order by account_name;
