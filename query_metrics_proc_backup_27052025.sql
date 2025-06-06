CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_EXPLODING_JOINS()
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
    cnt_8_14_days INTEGER;
    result_object OBJECT;
BEGIN
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:output_rows output_rows,
               OPERATOR_STATISTICS:input_rows input_rows,
               CASE WHEN operator_statistics:input_rows>0 THEN operator_statistics:output_rows / operator_statistics:input_rows ELSE 0 END as row_multiple,
                   CASE WHEN row_multiple > 1 THEN 1 ELSE 0 END AS EXPLODING_JOIN  
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               EXPLODING_JOIN 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,EXPLODING_JOIN from table(result_scan(last_query_id())) where "EXPLODING_JOIN" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_7_days := cnt_7_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    -- 8_to_14 days
    SET cnt_8_14_days := 0;
    rs := (select query_id from snowflake.account_usage.query_history where datediff(''day'',date(start_time),current_date()) between 8 and 14 order by total_elapsed_time desc limit 100);
    LET record_2 cursor for rs;
    OPEN record_2;
    FOR record_2 in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:output_rows output_rows,
               OPERATOR_STATISTICS:input_rows input_rows,
               CASE WHEN operator_statistics:input_rows>0 THEN operator_statistics:output_rows / operator_statistics:input_rows ELSE 0 END as row_multiple,
                   CASE WHEN row_multiple > 1 THEN 1 ELSE 0 END AS EXPLODING_JOIN  
            from table(get_query_operator_stats('' || '''''''' || record_2.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               EXPLODING_JOIN  
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,EXPLODING_JOIN from table(result_scan(last_query_id())) where "EXPLODING_JOIN" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_8_14_days := cnt_8_14_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record_2;
    result_object := OBJECT_CONSTRUCT(''7_days'',:cnt_7_days,''8_14_days'',:cnt_8_14_days);
    return result_object;
END;
';


-----------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_EXPLODING_JOINS_EXTENDED()
RETURNS TABLE ("QUERY_ID" VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
BEGIN
    TRUNCATE TABLE DBA_DB.PUBLIC.TEMP_EXPLODING_JOINS_QUERIES;
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:output_rows output_rows,
               OPERATOR_STATISTICS:input_rows input_rows,
               CASE WHEN operator_statistics:input_rows>0 THEN operator_statistics:output_rows / operator_statistics:input_rows ELSE 0 END as row_multiple,
                   CASE WHEN row_multiple > 1 THEN 1 ELSE 0 END AS EXPLODING_JOIN  
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               EXPLODING_JOIN 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                BEGIN
                    INSERT INTO DBA_DB.PUBLIC.TEMP_EXPLODING_JOINS_QUERIES select distinct QUERY_ID from table(result_scan(last_query_id())) where "EXPLODING_JOIN" = 1;
                EXCEPTION
                    WHEN OTHER THEN
                        continue;
                END;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    output_rs := (select * from DBA_DB.PUBLIC.TEMP_EXPLODING_JOINS_QUERIES);
    return table(output_rs);
END;
';


-----------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_GREATER_THAN_MEMORY()
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
    cnt_8_14_days INTEGER;
    result_object OBJECT;
BEGIN
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage bytes_spilled_local,
               OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage bytes_spilled_remote,
                   CASE WHEN bytes_spilled_local>0 OR bytes_spilled_remote>0 THEN 1 ELSE 0 END AS QUERIES_TOO_LARGE_MEMORY 
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               QUERIES_TOO_LARGE_MEMORY 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,QUERIES_TOO_LARGE_MEMORY from table(result_scan(last_query_id())) where "QUERIES_TOO_LARGE_MEMORY" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_7_days := cnt_7_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    -- 8_to_14 days
    SET cnt_8_14_days := 0;
    rs := (select query_id from snowflake.account_usage.query_history where datediff(''day'',date(start_time),current_date()) between 8 and 14 order by total_elapsed_time desc limit 100);
    LET record_2 cursor for rs;
    OPEN record_2;
    FOR record_2 in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage bytes_spilled_local,
               OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage bytes_spilled_remote,
                   CASE WHEN bytes_spilled_local>0 OR bytes_spilled_remote>0 THEN 1 ELSE 0 END AS QUERIES_TOO_LARGE_MEMORY 
            from table(get_query_operator_stats('' || '''''''' || record_2.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               QUERIES_TOO_LARGE_MEMORY
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,QUERIES_TOO_LARGE_MEMORY from table(result_scan(last_query_id())) where "QUERIES_TOO_LARGE_MEMORY" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_8_14_days := cnt_8_14_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record_2;
    result_object := OBJECT_CONSTRUCT(''7_days'',:cnt_7_days,''8_14_days'',:cnt_8_14_days);
    return result_object;
END;
';


--------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_GREATER_THAN_MEMORY_EXTENDED()
RETURNS TABLE ("QUERY_ID" VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
BEGIN
    TRUNCATE TABLE DBA_DB.PUBLIC.TEMP_GREATER_THAN_MEMORY_QUERIES;
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage bytes_spilled_local,
               OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage bytes_spilled_remote,
                   CASE WHEN bytes_spilled_local>0 OR bytes_spilled_remote>0 THEN 1 ELSE 0 END AS QUERIES_TOO_LARGE_MEMORY 
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               QUERIES_TOO_LARGE_MEMORY 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                BEGIN
                    INSERT INTO DBA_DB.PUBLIC.TEMP_GREATER_THAN_MEMORY_QUERIES select distinct QUERY_ID from table(result_scan(last_query_id())) where "QUERIES_TOO_LARGE_MEMORY" = 1;
                EXCEPTION
                    WHEN OTHER THEN
                        continue;
                END;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    output_rs := (select * from DBA_DB.PUBLIC.TEMP_GREATER_THAN_MEMORY_QUERIES);
    return table(output_rs);
END;
';


-------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_INEFFICIENT_PRUNING()
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
    cnt_8_14_days INTEGER;
    result_object OBJECT;
BEGIN
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:pruning:partitions_scanned partitions_scanned,
               OPERATOR_STATISTICS:pruning:partitions_total partitions_total,
               OPERATOR_STATISTICS:pruning:partitions_scanned/OPERATOR_STATISTICS:pruning:partitions_total::float as partition_scan_ratio,
                   CASE WHEN partition_scan_ratio >= .8 AND partitions_total >= 20000 THEN 1 ELSE 0 END AS INEFFICIENT_PRUNING_FLAG 
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               INEFFICIENT_PRUNING_FLAG 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,INEFFICIENT_PRUNING_FLAG from table(result_scan(last_query_id())) where "INEFFICIENT_PRUNING_FLAG" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_7_days := cnt_7_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    -- 8_to_14 days
    SET cnt_8_14_days := 0;
    rs := (select query_id from snowflake.account_usage.query_history where datediff(''day'',date(start_time),current_date()) between 8 and 14 order by total_elapsed_time desc limit 100);
    LET record_2 cursor for rs;
    OPEN record_2;
    FOR record_2 in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:pruning:partitions_scanned partitions_scanned,
               OPERATOR_STATISTICS:pruning:partitions_total partitions_total,
               OPERATOR_STATISTICS:pruning:partitions_scanned/OPERATOR_STATISTICS:pruning:partitions_total::float as partition_scan_ratio,
                   CASE WHEN partition_scan_ratio >= .8 AND partitions_total >= 20000 THEN 1 ELSE 0 END AS INEFFICIENT_PRUNING_FLAG 
            from table(get_query_operator_stats('' || '''''''' || record_2.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               INEFFICIENT_PRUNING_FLAG 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,INEFFICIENT_PRUNING_FLAG from table(result_scan(last_query_id())) where "INEFFICIENT_PRUNING_FLAG" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_8_14_days := cnt_8_14_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record_2;
    result_object := OBJECT_CONSTRUCT(''7_days'',:cnt_7_days,''8_14_days'',:cnt_8_14_days);
    return result_object;
END;
';

---------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_INEFFICIENT_PRUNING_EXTENDED()
RETURNS TABLE ("QUERY_ID" VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
BEGIN
    TRUNCATE TABLE DBA_DB.PUBLIC.TEMP_INEFFICIENT_PRUNING_QUERIES;
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
               OPERATOR_STATISTICS:pruning:partitions_scanned partitions_scanned,
               OPERATOR_STATISTICS:pruning:partitions_total partitions_total,
               OPERATOR_STATISTICS:pruning:partitions_scanned/OPERATOR_STATISTICS:pruning:partitions_total::float as partition_scan_ratio,
                   CASE WHEN partition_scan_ratio >= .8 AND partitions_total >= 20000 THEN 1 ELSE 0 END AS INEFFICIENT_PRUNING_FLAG 
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               INEFFICIENT_PRUNING_FLAG 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                BEGIN
                    INSERT INTO DBA_DB.PUBLIC.TEMP_INEFFICIENT_PRUNING_QUERIES select distinct QUERY_ID from table(result_scan(last_query_id())) where "INEFFICIENT_PRUNING_FLAG" = 1;
                EXCEPTION
                    WHEN OTHER THEN
                        continue;
                END;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;
    output_rs := (select * from DBA_DB.PUBLIC.TEMP_INEFFICIENT_PRUNING_QUERIES);
    return table(output_rs);

END;
';

------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_UNION_WITHOUT_ALL()
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
    cnt_8_14_days INTEGER;
    result_object OBJECT;
BEGIN
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
                   CASE WHEN OPERATOR_TYPE = '' || '''''''' || ''UnionAll'' || '''''''' || '' and lag(OPERATOR_TYPE) over (ORDER BY OPERATOR_ID) = '' || '''''''' || ''Aggregate'' || '''''''' || '' THEN 1 ELSE 0 END AS UNION_WITHOUT_ALL 
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               UNION_WITHOUT_ALL 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,UNION_WITHOUT_ALL from table(result_scan(last_query_id())) where "UNION_WITHOUT_ALL" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_7_days := cnt_7_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    -- 8_to_14 days
    SET cnt_8_14_days := 0;
    rs := (select query_id from snowflake.account_usage.query_history where datediff(''day'',date(start_time),current_date()) between 8 and 14 order by total_elapsed_time desc limit 100);
    LET record_2 cursor for rs;
    OPEN record_2;
    FOR record_2 in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
                   CASE WHEN OPERATOR_TYPE = '' || '''''''' || ''UnionAll'' || '''''''' || '' and lag(OPERATOR_TYPE) over (ORDER BY OPERATOR_ID) = '' || '''''''' || ''Aggregate'' || '''''''' || '' THEN 1 ELSE 0 END AS UNION_WITHOUT_ALL 
            from table(get_query_operator_stats('' || '''''''' || record_2.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               UNION_WITHOUT_ALL
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                output_rs := (select QUERY_ID,OPERATOR_TYPE,UNION_WITHOUT_ALL from table(result_scan(last_query_id())) where "UNION_WITHOUT_ALL" = 1);
                result_cnt := (select count(*) from table(result_scan(last_query_id())));
                SET cnt_8_14_days := cnt_8_14_days + result_cnt;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record_2;
    result_object := OBJECT_CONSTRUCT(''7_days'',:cnt_7_days,''8_14_days'',:cnt_8_14_days);
    return result_object;
END;
';

-------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_METRIC_QUERIES_UNION_WITHOUT_ALL_EXTENDED()
RETURNS TABLE ("QUERY_ID" VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    cnt_7_days INTEGER;
BEGIN
    TRUNCATE TABLE DBA_DB.PUBLIC.TEMP_UNION_WITHOUT_ALL_QUERIES;
    -- rs := (select ''01bb9734-0614-b193-001e-7383798b3e7e'' as query_id);
    rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
    LET record cursor for rs;
    OPEN record;
    SET cnt_7_days := 0;
    FOR record in rs DO
        BEGIN
            SET flg := 0;
            sql_query := ''with query_stats as (
            select
               QUERY_ID,
               STEP_ID,
               OPERATOR_ID,
               OPERATOR_TYPE,
               OPERATOR_STATISTICS,
                   CASE WHEN OPERATOR_TYPE = '' || '''''''' || ''UnionAll'' || '''''''' || '' and lag(OPERATOR_TYPE) over (ORDER BY OPERATOR_ID) = '' || '''''''' || ''Aggregate'' || '''''''' || '' THEN 1 ELSE 0 END AS UNION_WITHOUT_ALL 
            from table(get_query_operator_stats('' || '''''''' || record.query_id || '''''''' || ''))
            ORDER BY STEP_ID,OPERATOR_ID
            )
            SELECT
               QUERY_ID,
               OPERATOR_TYPE,
               UNION_WITHOUT_ALL 
            FROM query_stats'';
            EXECUTE IMMEDIATE :sql_query;
        EXCEPTION
            WHEN OTHER THEN
                SET flg := 1;
        END;
        BEGIN
            IF (flg = 0) THEN
                BEGIN
                    INSERT INTO DBA_DB.PUBLIC.TEMP_UNION_WITHOUT_ALL_QUERIES select distinct QUERY_ID from table(result_scan(last_query_id())) where "UNION_WITHOUT_ALL" = 1;
                EXCEPTION
                    WHEN OTHER THEN
                        continue;
                END;
            ELSE
                continue;
            END IF;
        END;
    END FOR;
    CLOSE record;

    output_rs := (select * from DBA_DB.PUBLIC.TEMP_UNION_WITHOUT_ALL_QUERIES);
    return table(output_rs);
END;
';


-------------------------------------------------------------------------------------------------


