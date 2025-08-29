create or replace table DBA_DB.PUBLIC.WH_Credits_1
AS
select account_name , warehouse_name
,credits_used_compute
,start_time
,SERVICE_TYPE
from DBA_DB.PUBLIC.WH_Credits
Where warehouse_name is not null
AND warehouse_name <> 'CLOUD_SERVICES_ONLY';


CREATE OR REPLACE TABLE DBA_DB.PUBLIC.WH_Credits_2
AS
select 
account_name,SERVICE_TYPE,
warehouse_name, 
date_trunc(month, start_time) as USAGE_MONTH,
round(sum(credits_used_compute),3) as Total_credits,
case when usage_month >= '2025-05-01 00:00:00.000 -0400' then 2.56 * Total_credits
else 2.32 * Total_credits end as BILLABLE_USAGE
from DBA_DB.PUBLIC.WH_Credits_1
group by account_name,SERVICE_TYPE,warehouse_name,USAGE_MONTH;

select * from DBA_DB.PUBLIC.WH_Credits_2


create or replace table DBA_DB.PUBLIC.WH_Credits_final
as select 
Account_name,
warehouse_name ,
year(usage_month)||'-'||monthname(usage_month) as Month,
BILLABLE_USAGE 
from DBA_DB.PUBLIC.WH_Credits_2;


select * from DBA_DB.PUBLIC.WH_Credits_final 

create or replace table DBA_DB.PUBLIC.WH_Credits_final_before_Pivot_Owner as 
(select a.account_name,Warehouse_name,a.month,BILLABLE_USAGE,
case when application is null then 'Other' else application end application_1,
case when owner is null then 'Other' else owner end owner_1
from DBA_DB.PUBLIC.WH_Credits_final  a
left join dba_db.public.wh_owner_xref b
 on b.account_name = a.account_name
 and b.wh_name = a.warehouse_name
 order by account_name,warehouse_name)

select * from DBA_DB.PUBLIC.WH_Credits_final_before_Pivot_Owner


create or replace table DBA_DB.PUBLIC.WH_Credits_final_Month
as 
(select Account_name,warehouse_name,
AUG_24,SEP_24,OCT_24,NOV_24,DEC_24,JAN_25,FEB_25,MAR_25,APR_25,MAY_25,JUN_25,JUL_25 from DBA_DB.PUBLIC.WH_Credits_final
pivot(SUM(Billable_Usage) for Month IN (
'2024-Aug','2024-Sep','2024-Oct','2024-Nov','2024-Dec', '2025-Jan',
'2025-Feb','2025-Mar','2025-Apr','2025-May','2025-Jun','2025-Jul' )
) as P
(Account_name,warehouse_name,AUG_24,SEP_24,OCT_24,NOV_24,DEC_24,JAN_25,FEB_25,MAR_25,APR_25,MAY_25,JUN_25,JUL_25));

select * from DBA_DB.PUBLIC.WH_Credits_final_Month

create or replace table DBA_DB.PUBLIC.WH_final as(
select 
case when Account_name='WUDATAUAT' then 'WUDATAUAT1'  else Account_name end as Act_name,
case when aug_24 is null then 0 else aug_24 end aug_24_1,
case when sep_24 is null then 0 else sep_24 end sep_24_1,
case when oct_24 is null then 0 else oct_24 end oct_24_1,
case when nov_24 is null then 0 else nov_24 end nov_24_1,
case when dec_24 is null then 0 else dec_24 end dec_24_1,
case when jan_25 is null then 0 else jan_25 end jan_25_1,
case when feb_25 is null then 0 else feb_25 end feb_25_1,
case when mar_25 is null then 0 else mar_25 end mar_25_1,
case when apr_25 is null then 0 else apr_25 end apr_25_1,
case when may_25 is null then 0 else may_25 end may_25_1,
case when jun_25 is null then 0 else jun_25 end jun_25_1,
case when jul_25 is null then 0 else jul_25 end jul_25_1,
* from DBA_DB.PUBLIC.WH_Credits_final_Month);


select count(*) from table DBA_DB.PUBLIC.WH_final

create or replace table DBA_DB.PUBLIC.WH_final_Owner_XREF as 
(select a.account_name,Warehouse_name,
case when application is null then 'Other' else application end application_1,
case when owner is null then 'Other' else owner end owner_1,
Aug_24_1,Sep_24_1,Oct_24_1,nov_24_1,dec_24_1,jan_25_1,feb_25_1,mar_25_1,apr_25_1,may_25_1,
jun_25_1,jul_25_1
from DBA_DB.PUBLIC.WH_final a
left join dba_db.public.wh_owner_xref b
 on b.account_name = a.account_name
 and b.wh_name = a.warehouse_name
 order by account_name,warehouse_name)

 
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column application_1 to application;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column owner_1 to owner;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column aug_24_1 to aug_24;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column sep_24_1 to sep_24;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column oct_24_1 to oct_24;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column nov_24_1 to nov_24;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column dec_24_1 to dec_24;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column jan_25_1 to jan_25;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column feb_25_1 to feb_25;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column mar_25_1 to mar_25;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column apr_25_1 to apr_25;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column may_25_1 to may_25;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column jun_25_1 to jun_25;
 alter table DBA_DB.PUBLIC.WH_final_Owner_XREF rename column jul_25_1 to jul_25;

   
DELETE from DBA_DB.PUBLIC.WH_final_Owner_XREF WHERE  
AUG_24 = 0 AND SEP_24 = 0 AND OCT_24 = 0 AND NOV_24 = 0 AND DEC_24 = 0 AND JAN_25 = 0 AND
FEB_25 = 0 AND MAR_25 = 0 AND APR_25 = 0 AND MAY_25 = 0 AND JUN_25 = 0 AND JUL_25 = 0

SELECT * FROM DBA_DB.PUBLIC.WH_final_Owner_XREF
