SELECT
  CASE
    WHEN Q.total_elapsed_time <= 60000 THEN '<60s'
    WHEN Q.total_elapsed_time <= 300000 THEN '60s-5m'
    WHEN Q.total_elapsed_time <= 600000 THEN '5m-10m'
    WHEN Q.total_elapsed_time <= 1800000 THEN '10m-30m'
    ELSE '>30m'
  END AS BUCKETS,
  COUNT(query_id) AS number_of_queries
FROM snowflake.account_usage.query_history Q
WHERE  month(TO_DATE(Q.START_TIME)) = '4' AND year(TO_DATE(Q.START_TIME)) = '2025' 
  AND total_elapsed_time > 0
  AND warehouse_name = 'MDE_RET_EU_LARGE_WH'
GROUP BY 1;

------------------------------------------------------------------------------------------------

select concat(u_month,'|',buckets,'|',queries_c)
from(
select date_part(month,q.start_time) as u_month,
 CASE
     WHEN Q.total_elapsed_time <= 60000 THEN '<60s'
     WHEN Q.total_elapsed_time <= 300000 THEN '60s-5m'
     WHEN Q.total_elapsed_time <= 600000 THEN '5m-10m'
     WHEN Q.total_elapsed_time <= 1800000 THEN '10m-30m'
     ELSE '>30m'
 END as buckets,COUNT(query_id) as queries_c
FROM snowflake.account_usage.query_history Q 
WHERE date_part(month,q.start_time) in (5,6) AND 
date_part(year,q.start_time) = 2025 AND total_elapsed_time > 0
AND warehouse_name = 'UNP_APP_SMALL_WH' 
 GROUP BY 1,2 order by 1);


select concat(date_part(month,q.start_time),'|', COUNT(query_id))
FROM snowflake.account_usage.query_history Q WHERE 
date_part(month,q.start_time) in (4,5) AND 
date_part(year,q.start_time) = 2025 AND 
Q.total_elapsed_time > 0 AND 
Q.warehouse_name = 'UNP_APP_SMALL_WH'
group by date_part(month,q.start_time)
order by date_part(month,q.start_time),COUNT(query_id) desc;


select concat(date_part(month,q.start_time),'|',query_type,'|', COUNT(query_id))
FROM snowflake.account_usage.query_history Q WHERE 
date_part(month,q.start_time) in (5,6) AND 
date_part(year,q.start_time) = 2025 AND 
Q.total_elapsed_time > 0 AND 
Q.warehouse_name = 'USR_CRO_MEDIUM_WH'
group by date_part(month,q.start_time),query_type
order by date_part(month,q.start_time),COUNT(query_id) desc;


---------------------------------------------------------------------------------------------------------
---- Costliest Queries

with ranked_data as (
select QUERY_ID,QUERY_PARAMETERIZED_HASH,QUERY_TEXT,WAREHOUSE_NAME,USER_NAME,ROLE_NAME from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE DATE(START_TIME) >= DATEADD('days',-30,CURRENT_DATE())
QUALIFY ROW_NUMBER() OVER (PARTITION BY QUERY_PARAMETERIZED_HASH ORDER BY START_TIME DESC) = 1
),
agg_data as (
select 
QH.QUERY_PARAMETERIZED_HASH,
round(sum(QH.total_elapsed_time/1000/60),2) as TOTAL_EXECUTION_TIME_MINS,
COUNT(QH.QUERY_ID) as no_of_runs,
round(avg(QH.total_elapsed_time)/1000/60,2) as AVG_EXECUTION_TIME_MINS,
round(sum(QA.CREDITS_ATTRIBUTED_COMPUTE) * 2.56,2) as TOTAL_QUERY_COST,
avg(QH.bytes_spilled_to_local_storage) as BYTES_SPILLED_LOCAL,
avg(QH.bytes_spilled_to_remote_storage) as BYTES_SPILLED_REMOTE,
avg(QH.bytes_sent_over_the_network) as BYTES_SENT_OVER_NETWORK,
sum(QH.rows_produced) as total_rows_produced,
avg(QH.bytes_scanned) as total_bytes_scanned
FROM 
(select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE DATE(START_TIME) >= DATEADD('days',-30,CURRENT_DATE())) QH
LEFT JOIN 
(select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY WHERE DATE(START_TIME) >= DATEADD('days',-30,CURRENT_DATE()))QA
ON QH.query_id = QA.query_id
GROUP BY QH.QUERY_PARAMETERIZED_HASH
)
select b.query_text,a.query_parameterized_hash,a.total_execution_time_mins,a.total_query_cost,a.BYTES_SPILLED_LOCAL,a.BYTES_SPILLED_REMOTE,a.BYTES_SENT_OVER_NETWORK,a.total_rows_produced,a.total_bytes_scanned,a.no_of_runs,a.avg_execution_time_mins,b.warehouse_name,b.user_name,b.role_name
from agg_data a
inner join ranked_data b
on a.query_parameterized_hash = b.query_parameterized_hash
where a.total_query_cost is not null
order by (a.total_query_cost,a.total_execution_time_mins,a.bytes_spilled_local) desc;


---------------------------------------------------------------------------------------------------------------------------------------------------

-- Longest running queries 

SELECT Q.query_hash,
  Q.query_id,
  Q.query_text,
  Q.total_elapsed_time/1000/60 AS query_execution_time_mins,
  Q.start_time,
  Q.end_time,
  Q.partitions_scanned,
  Q.partitions_total,
  QA.CREDITS_ATTRIBUTED_COMPUTE * 2.56 as total_query_cost,
  Q.user_name,
  Q.warehouse_name
FROM snowflake.account_usage.query_history Q
LEFT JOIN
snowflake.account_usage.query_attribution_history QA
ON Q.query_id = QA.query_id
WHERE 
  MONTH(Q.start_time) = 5
  -- AND total_elapsed_time/1000/60 > 120 --onlyget queries that actually used compute
  AND QA.CREDITS_ATTRIBUTED_COMPUTE * 2.56 >= 500 
  AND partitions_scanned IS NOT NULL
ORDER BY total_query_cost desc;

----------------------------------------------------------------------------------------------------------------------------
--------------WH Usage Analysis


show warehouses in account;
with usage_data as(
select 
WAREHOUSE_NAME,
DATE_TRUNC('month',START_TIME) as month_start,
SUM(CREDITS_USED) as total_credits
from 
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE
WAREHOUSE_NAME IN (select "name" from table(result_scan(last_query_id())))
AND START_TIME >= DATEADD('month',-4,DATE_TRUNC('month',CURRENT_DATE))
GROUP BY
WAREHOUSE_NAME,
DATE_TRUNC('month',START_TIME)
),
pivot_data as (
select
    WAREHOUSE_NAME,
    CASE 
    WHEN month_start = DATE_TRUNC('month',current_date) then 'current_month'
    WHEN month_start = DATEADD(month,-1,DATE_TRUNC('month',CURRENT_DATE)) then 'month-1'
    WHEN month_start = DATEADD(month,-2,DATE_TRUNC('month',CURRENT_DATE)) then 'month-2'
    WHEN month_start = DATEADD(month,-3,DATE_TRUNC('month',CURRENT_DATE)) then 'month-3'
    WHEN month_start = DATEADD(month,-4,DATE_TRUNC('month',CURRENT_DATE)) then 'month-4'
    END as month_bucket,
    total_credits
FROM
    usage_data
WHERE
    month_start >= DATEADD(month,-4,DATE_TRUNC('month',CURRENT_DATE))
),
aggregated_data as (
SELECT
    WAREHOUSE_NAME,
    MAX(CASE WHEN month_bucket = 'month-4' THEN total_credits ELSE 0 END) as month_4,
    MAX(CASE WHEN month_bucket = 'month-3' THEN total_credits ELSE 0 END) as month_3,
    MAX(CASE WHEN month_bucket = 'month-2' THEN total_credits ELSE 0 END) as month_2,
    MAX(CASE WHEN month_bucket = 'month-1' THEN total_credits ELSE 0 END) as last_month,
    MAX(CASE WHEN month_bucket = 'current_month' THEN total_credits ELSE 0 END) as current_month
FROM
pivot_data
GROUP BY
WAREHOUSE_NAME
)
select
    WAREHOUSE_NAME,
    month_4 * 2.56 as month_4_costs,
    month_3 * 2.56 as month_3_costs,
    month_2 * 2.56 as month_2_costs,
    last_month * 2.56 as last_month_costs,
    current_month * 2.56 as current_month_costs,
    CASE WHEN last_month > 0 THEN ROUND(((current_month - last_month)/last_month)*100,2)
    ELSE NULL END as percent_change
FROM
aggregated_data order by warehouse_name;


-------------------------------------------------------------------------------------------------------------------------------------------
-----------------------Unused Tables


create or replace table DBA_DB.PUBLIC.temp_unused_tables_180_days (
TABLE_CATALOG VARCHAR(100),
TABLE_SCHEMA VARCHAR(100),
TABLE_NAME VARCHAR(100),
ROW_COUNT INTEGER,
SIZE_IN_MB NUMBER(20,9),
LAST_ALTERED VARCHAR(50),
TABLE_OWNER VARCHAR(100)
)


EXECUTE IMMEDIATE $$
DECLARE
    rs RESULTSET;
    rs1 RESULTSET;
    rs2 RESULTSET;
    rs3 RESULTSET;
    rs4 RESULTSET;
    rc INTEGER;
    db_stmt VARCHAR;
    stmt VARCHAR;
    stmt1 VARCHAR;
    flg INTEGER;
BEGIN
    show databases in account;
    db_stmt := 'select distinct database_name as db_name from SNOWFLAKE.ACCOUNT_USAGE.DATABASES where type = ' || '''' || 'STANDARD' || '''' || ' and database_name <> ' || '''' || 'WUIB' || '''' || ' order by db_name';
    --db_stmt := 'select ' || '''' || 'WUPOS_CARDPAYMENTS_EU' || '''' || ' as db_name';
    rs1 := (EXECUTE IMMEDIATE :db_stmt);
    LET record cursor for rs1;
    OPEN record; 
    FOR record in rs1 DO
        BEGIN
            LET flg := 0;
            BEGIN
                stmt := 'use ' || '"' || record.db_name || '"';
                rs2 := (EXECUTE IMMEDIATE :stmt);
            EXCEPTION
                WHEN OTHER THEN
                    SET flg := 1;
            END;
            BEGIN
                IF (flg = 0) THEN
                    stmt1 := 'WITH access_hist AS (   SELECT query_id,user_name,query_start_time as start_time,split(base.value:objectName, ' || '''' || '.' || '''' || ')[0]::string as DATABASE_NAME,split(base.value:objectName, ' || '''' || '.' || '''' || ')[1]::string as SCHEMA_NAME,split(base.value:objectName, ' || '''' || '.' || '''' || ')[2]::string as TABLE_NAME FROM snowflake.account_usage.access_history,lateral flatten (base_objects_accessed) base),access_hist_last_30 AS (SELECT * FROM access_hist WHERE DATABASE_NAME = ' || '''' || record.db_name || '''' || ' and date(start_time) > dateadd(days,-180,current_date()) QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_NAME,DATABASE_NAME,SCHEMA_NAME,TABLE_NAME ORDER BY START_TIME DESC) = 1) select A.TABLE_CATALOG,A.TABLE_SCHEMA, A.TABLE_NAME,A.ROW_COUNT,A.BYTES/1000/1000 AS SIZE_IN_MB,A.LAST_ALTERED,A.TABLE_OWNER from (select * from snowflake.account_usage.TABLES where table_catalog = ' || '''' || record.db_name || '''' || ' and deleted is null)A LEFT JOIN access_hist_last_30 B ON A.TABLE_CATALOG = B.DATABASE_NAME AND A.TABLE_SCHEMA = B.SCHEMA_NAME AND A.TABLE_NAME = B.TABLE_NAME WHERE A.TABLE_CATALOG = ' || '''' || record.db_name || '''' || ' AND A.TABLE_TYPE = ' || '''' || 'BASE TABLE' || '''' || ' AND B.TABLE_NAME IS NULL AND date(LAST_ALTERED) < dateadd(days,-180,current_date()) ORDER BY SIZE_IN_MB DESC';
                    rs3 := (EXECUTE IMMEDIATE :stmt1);
                    rs4 := (INSERT INTO DBA_DB.PUBLIC.temp_unused_tables_180_days SELECT "TABLE_CATALOG","TABLE_SCHEMA","TABLE_NAME","ROW_COUNT","SIZE_IN_MB","LAST_ALTERED","TABLE_OWNER" from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
                ELSE
                    continue;
                END IF;
            END;
        END;
    END FOR;
END
$$;


--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--------------Time Travel Information and Change

create or replace TABLE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION (
	TABLE_CATALOG VARCHAR(16777216),
	TABLE_SCHEMA VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	STORAGE_BYTES NUMBER(38,0),
	OWNER VARCHAR(16777216),
	CURRENT_TIME_TRAVEL_DURATION NUMBER(38,0),
	CHANGE_TT VARCHAR(1),
	LAST_UPDATED TIMESTAMP_NTZ(9),
	COMMENTS VARCHAR(500)
);

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.TIME_TRAVEL_INFORMATION_EXTRACT()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    db_resultset RESULTSET;
    schema_resultset RESULTSET;
    output_resultset RESULTSET;
    db_stmt VARCHAR;
    schema_stmt VARCHAR;
    table_stmt VARCHAR;
    insert_stmt VARCHAR;
    flg INTEGER;
    nr_flag INTEGER;
    sch_flg INTEGER;
BEGIN
    --Truncate the table and create resultset of all the database names available in account
    TRUNCATE TABLE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION;
    SHOW DATABASES in ACCOUNT;
    db_resultset := (SELECT "name" as db_name from TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "name" not like '%TXODS%');
    LET record cursor for db_resultset;
    OPEN record; 
    FOR record in db_resultset DO
        BEGIN
            LET flg := 0;
            BEGIN
                db_stmt := 'use ' || '"' || record.db_name || '"';
                EXECUTE IMMEDIATE :db_stmt;
            --Exception handling to ensure Proc doesn't fail if executing user is not having access to Database
            EXCEPTION
                WHEN OTHER THEN
                    SET flg := 1;
            END;
            BEGIN
                IF (flg = 0) THEN
                    schema_stmt := 'show schemas in database ' || record.db_name;
                    EXECUTE IMMEDIATE :schema_stmt;
                    schema_resultset := (SELECT "name" as schema_name from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
                    LET schema_record cursor for schema_resultset;
                    OPEN schema_record; 
                    FOR schema_record in schema_resultset DO
                        LET sch_flg := 0;
                        BEGIN
                            table_stmt := 'show tables in ' || record.db_name || '.' || schema_record.schema_name;
                            EXECUTE IMMEDIATE :table_stmt;
                        --Exception Handling if the executing user doesn't have access to Schema
                        EXCEPTION
                            WHEN OTHER THEN
                            SET sch_flg := 1;
                        END;
                        BEGIN
                            if (sch_flg = 0) THEN
                                LET nr_flag := 0;
                                --Select the tables from the Schema where Retention time is > 3 days
                                output_resultset := (SELECT "database_name" as db_name,"schema_name" as sch_name,"name" as tbl_name,"bytes" as storage_bytes,"owner" as t_owner,"retention_time" as ret_time from TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "retention_time" > 3);
                                nr_flag := (select count(*) from table(RESULT_SCAN(LAST_QUERY_ID())));
                                --No Record Flag to check if there exists any records where retention time > 3 days. If exists, populate them in table
                                if (nr_flag > 0) THEN
                                    LET row_record cursor for output_resultset;
                                    OPEN row_record;
                                    FOR row_record in output_resultset DO
                                        BEGIN
                                            insert_stmt := 'INSERT INTO DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION values (' || '''' || row_record.db_name || '''' || ',' || '''' || row_record.sch_name || '''' || ',' || '''' || row_record.tbl_name || '''' || ',' || row_record.storage_bytes || ',' || '''' || row_record.t_owner || '''' || ',' || row_record.ret_time || ',' || '''' || 'Y' || '''' || ',null,' || '''' || '''' || ')';
                                            EXECUTE IMMEDIATE :insert_stmt;
                                        END;
                                    END FOR;
                                ELSE
                                    continue;
                                END IF;
                            ELSE
                                continue;
                            END IF;
                        END;
                    END FOR;
                ELSE
                    continue;
                END IF;
            END;
        END;
    END FOR;
    return 'Success!';
END;
$$;


----------------------------

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.ALTER_USER_STMT_TIMEOUT()
RETURNS STRING
LANGUAGE SQL
EXECUTE as CALLER
AS
$$
DECLARE
    table_result RESULTSET;
    alter_stmt VARCHAR;
    updt_stmt VARCHAR;
    flg INTEGER;
    cnt INTEGER;
BEGIN
    --Fetching data from the Audit i.e. config table
    table_result := (SELECT "TABLE_CATALOG" as db_name,"TABLE_SCHEMA" as db_schema,"TABLE_NAME" as db_table,"CURRENT_TIME_TRAVEL_DURATION" as retention_time,"STORAGE_BYTES" as storage_bytes,"OWNER" as t_owner from DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION where "CHANGE_TT" = 'Y');
    cnt := (select count(*) from table(RESULT_SCAN(LAST_QUERY_ID())));
    --Fetching count to ensure if there are any tables for Time travel to be changed, If available then proceed else quit
    if (cnt > 0) THEN
        LET record cursor for table_result;
        OPEN record; 
        FOR record in table_result DO
            BEGIN
                LET flg := 0;
                BEGIN
                    alter_stmt := 'ALTER TABLE ' || record.db_name || '.' || record.db_schema || '.' || record.db_table || ' SET DATA_RETENTION_TIME_IN_DAYS = 3';
                    EXECUTE IMMEDIATE :alter_stmt;
                EXCEPTION
                    WHEN OTHER THEN
                        SET flg := 1;
                END;
                BEGIN
                    --If Alter table succeeded then Update the config i.e. audit table comments with success
                    IF (flg = 0) THEN
                        updt_stmt := 'UPDATE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION set COMMENTS =  ' || '''' || 'Time Travel Duration Changed' || '''' || ',LAST_UPDATED = CURRENT_TIMESTAMP(),CHANGE_TT = ' || '''' || 'N' || '''' || ' where TABLE_CATALOG = ' || '''' || record.db_name || '''' || ' AND TABLE_SCHEMA = ' || '''' || record.db_schema || '''' || ' AND TABLE_NAME = ' || '''' || record.db_table || '''';
                        EXECUTE IMMEDIATE :updt_stmt;
                    ELSE
                        -- If alter table failedmm then update the table with failed comments
                        updt_stmt := 'UPDATE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION set COMMENTS =  ' || '''' || 'Failed to Change Time Travel Duration' || '''' || ',LAST_UPDATED = CURRENT_TIMESTAMP() where TABLE_CATALOG = ' || '''' || record.db_name || '''' || ' AND TABLE_SCHEMA = ' || '''' || record.db_schema || '''' || ' AND TABLE_NAME = ' || '''' || record.db_table || '''';
                        EXECUTE IMMEDIATE :updt_stmt;
                    END IF;
                END;
            END;
        END FOR;
        RETURN 'Time Travel audit completed successfully.';
    ELSE
        RETURN 'No Tables to Proceed!';
    END IF;
END;
$$;


----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------
------------------Storage Analysis

select 
database_name,
month(usage_date) as usage_month,
max(round(average_database_bytes / power(2, 40), 1)) as DATABASE_SIZE_TB,
max(round(average_failsafe_bytes / power(2, 40), 1)) as FAILSAFE_SIZE_TB
from
snowflake.account_usage.database_storage_usage_history
where month(usage_date) in ('1','2','3','4','5','6')
and year(usage_date) = '2025'
group by database_name,usage_month
order by database_name,usage_month;

select * 
from 
(
    select 
    database_name,
    'Average Active Size in TB',
    max(case when usage_month = '1' then DATABASE_SIZE_TB END) as "January 2025 Size",
    max(case when usage_month = '2' then DATABASE_SIZE_TB END) as "February 2025 Size",
    max(case when usage_month = '3' then DATABASE_SIZE_TB END) as "March 2025 Size",
    max(case when usage_month = '4' then DATABASE_SIZE_TB END) as "April 2025 Size",
    max(case when usage_month = '5' then DATABASE_SIZE_TB END) as "May 2025 Size",
    max(case when usage_month = '6' then DATABASE_SIZE_TB END) as "June 2025 Size"
    from table(result_scan(last_query_id()))
    group by database_name
    
    union 
    
    select 
    database_name,
    'Average Failsafe Size in TB',
    max(case when usage_month = '1' then FAILSAFE_SIZE_TB END) as "January 2025 Size",
    max(case when usage_month = '2' then FAILSAFE_SIZE_TB END) as "February 2025 Size",
    max(case when usage_month = '3' then FAILSAFE_SIZE_TB END) as "March 2025 Size",
    max(case when usage_month = '4' then FAILSAFE_SIZE_TB END) as "April 2025 Size",
    max(case when usage_month = '5' then FAILSAFE_SIZE_TB END) as "May 2025 Size",
    max(case when usage_month = '6' then FAILSAFE_SIZE_TB END) as "June 2025 Size"
    from table(result_scan(last_query_id()))
    group by database_name
)
order by 1,2;


--------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------
----------------Metadata Backup Procedure

CREATE TABLE CMPL_AUDIT_EXTRACTS.PUBLIC.METADATA_LOAD_AUDIT_STATS(
RUN_DATE DATE,
TABLE_NAME STRING,
LOAD_TYPE STRING,
START_TIME TIMESTAMP,
ROW_COUNT INT
);


CREATE OR REPLACE PROCEDURE CMPL_AUDIT_EXTRACTS.PUBLIC.METADATA_AUDIT_EXTRACT(
    table_name STRING,
    column_name STRING,
    load_type STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rs RESULTSET;
    stmt VARCHAR;
    backup_table STRING;
    create_stmt VARCHAR;
    max_start_stmt VARCHAR;
    max_start_date TIMESTAMP;
    insert_backup_stmt VARCHAR;
    start_time TIMESTAMP;
    count_stmt VARCHAR;
    rows_loaded INT;
    loaded_date VARCHAR;
    loaded_rs RESULTSET;
    failed_flag INT;
BEGIN
    IF (:table_name = '') THEN
        return 'Table Name is Null. Please enter a Table Name!';
    END IF;
    IF (:column_name = '') THEN
        return 'Column Name is Null. Please enter a Column Name!';
    END IF;
    IF (:load_type = '') THEN
        return 'Load Type is Null. Please enter either "initial" or "delta" !';
    END IF;
    SET backup_table := table_name || '_AUDIT_EXTRACT';
    SET start_time := CURRENT_TIMESTAMP();

    SET failed_flag := 0;
    -- Full load from source to backup
    IF (load_type = 'initial') THEN
        BEGIN
            create_stmt := 'CREATE TABLE ' || backup_table || ' AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.' || table_name ;
            EXECUTE IMMEDIATE :create_stmt;
        EXCEPTION
            WHEN OTHER THEN
                SET failed_flag := 1;
        END;
        
        BEGIN
            IF (failed_flag = 1) THEN
                return 'Failed while Creating table. Check if it already exists!';
            ELSE
                -- Grouped by date
                stmt := 'SELECT DATE(' || :column_name || ') as loaded_date,COUNT(*) as cnt FROM ' || backup_table || ' GROUP BY DATE(' || column_name || ')';
                rs := (EXECUTE IMMEDIATE :stmt);
                LET record cursor for rs;
                OPEN record; 
                FOR record in rs DO
                    SET rows_loaded := record.cnt;
                    SET loaded_date := record.loaded_date;
                    
                    -- Insert audit record
                    INSERT INTO CMPL_AUDIT_EXTRACTS.PUBLIC.METADATA_LOAD_AUDIT_STATS(RUN_DATE, TABLE_NAME, LOAD_TYPE, START_TIME, ROW_COUNT) VALUES (:loaded_date, :table_name, :load_type, :start_time, :rows_loaded);
                END FOR;
            END IF;
        END;
    ELSEIF (load_type = 'delta') THEN
        BEGIN
            -- Get max start date from backup
            max_start_stmt := 'SELECT COALESCE(MAX(' || column_name || '), DATEADD(DAY, -3, CURRENT_DATE())) as max_date FROM ' || backup_table;
            rs := (EXECUTE IMMEDIATE :max_start_stmt);
            LET record cursor for rs;
            OPEN record; 
            FOR record in rs DO
                SET max_start_date := record.max_date;
            END FOR;
            CLOSE record;
        
            IF (max_start_date >= DATEADD(DAY, -3, CURRENT_DATE())) THEN
                return 'Already loaded the latest data till D-3 days';
            END IF;
        
            -- Insert delta records into backup
            insert_backup_stmt := 'INSERT INTO ' || backup_table || ' SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.' || table_name || ' WHERE ' || column_name || ' > ' || '''' || max_start_date || '''' || ' AND DATE(' || column_name || ') <= DATEADD(DAY, -3, CURRENT_DATE())';
            EXECUTE IMMEDIATE :insert_backup_stmt;
        
            --Writing Date wise
            stmt := 'select DATE(' || column_name || ')as loaded_date,count(*) as cnt from (SELECT * from ' || backup_table || ' WHERE ' || column_name || ' > ' || '''' || max_start_date || '''' || ' AND DATE(' || column_name || ') <= DATEADD(DAY, -3, CURRENT_DATE()))tmp GROUP BY DATE(' || column_name || ')' ;
        
            loaded_rs := (EXECUTE IMMEDIATE :stmt);
            LET record_loaded cursor for loaded_rs;
            OPEN record_loaded; 
            FOR record_loaded in loaded_rs DO
                SET rows_loaded := record_loaded.cnt;
                SET loaded_date := record_loaded.loaded_date;
                    
                -- Insert audit record
                INSERT INTO CMPL_AUDIT_EXTRACTS.PUBLIC.METADATA_LOAD_AUDIT_STATS(RUN_DATE, TABLE_NAME, LOAD_TYPE, START_TIME, ROW_COUNT) VALUES (:loaded_date, :table_name, :load_type, :start_time, :rows_loaded);
            END FOR;
            CLOSE record_loaded;
        EXCEPTION
            WHEN OTHER THEN
                return 'Failed due to one of the reason: 1.Backup table doesnt exist. 2.Table Name is Invalid. 3.Column Name Invalid.';
        END;
        
    ELSE
        RETURN 'Invalid load type';
    END IF;

    RETURN 'Success ! Data loaded into ' || backup_table;
END;
$$;

--CALL CMPL_AUDIT_EXTRACTS.PUBLIC.METADATA_AUDIT_EXTRACT('QUERY_HISTORY','START_TIME','delta');

