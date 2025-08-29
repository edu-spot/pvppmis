CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.GET_QUERY_METRICS_GENERIC()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    max_date DATE;
    rs RESULTSET;
    sql_query VARCHAR;
    output_rs RESULTSET;
    flg INTEGER;
    result_cnt INTEGER;
    query_id_7d VARCHAR;
    count_7_ej INTEGER;
    count_7_uwa INTEGER;
    count_7_qlm INTEGER;
    count_7_ipl INTEGER;
    insert_stmt_7d VARCHAR;
    query_id_8_14d VARCHAR;
    count_8_14_ej INTEGER;
    count_8_14_uwa INTEGER;
    count_8_14_qlm INTEGER;
    count_8_14_ipl INTEGER;
    insert_stmt_8_14d VARCHAR;
BEGIN
    max_date := (select max(load_date) from DBA_DB.PUBLIC.QUERY_OPTIMIZATION_METRICS_SUMMARY_7_DAYS);
    IF (max_date = CURRENT_DATE()) THEN
        return 'Latest Data Available';
    ELSE
        DELETE FROM DBA_DB.PUBLIC.QUERY_OPTIMIZATION_METRICS_SUMMARY_7_DAYS where load_date < DATEADD('days',-2,CURRENT_DATE());
        DELETE FROM DBA_DB.PUBLIC.QUERY_OPTIMIZATION_METRICS_SUMMARY_8_14_DAYS where load_date < DATEADD('days',-2,CURRENT_DATE());
        -- rs := (select '01bb9734-0614-b193-001e-7383798b3e7e' as query_id);
        rs := (select query_id from snowflake.account_usage.query_history where date(start_time) > dateadd(days,-7,current_date()) order by total_elapsed_time desc limit 100);
        LET record cursor for rs;
        OPEN record;
        FOR record in rs DO
            BEGIN
                SET flg := 0;
                sql_query := 'with query_stats as (
                select
                   QUERY_ID,
                   STEP_ID,
                   OPERATOR_ID,
                   OPERATOR_TYPE,
                   OPERATOR_STATISTICS,
                   OPERATOR_STATISTICS:output_rows output_rows,
                   OPERATOR_STATISTICS:input_rows input_rows,
                   CASE WHEN operator_statistics:input_rows>0 THEN operator_statistics:output_rows / operator_statistics:input_rows ELSE 0 END as row_multiple,
                   OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage bytes_spilled_local,
                   OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage bytes_spilled_remote,
                  
                   operator_statistics:io:percentage_scanned_from_cache::float percentage_scanned_from_cache,
                   OPERATOR_STATISTICS:pruning:partitions_scanned partitions_scanned,
                   OPERATOR_STATISTICS:pruning:partitions_total partitions_total,
                   OPERATOR_STATISTICS:pruning:partitions_scanned/OPERATOR_STATISTICS:pruning:partitions_total::float as partition_scan_ratio,
                       CASE WHEN row_multiple > 1 THEN 1 ELSE 0 END AS EXPLODING_JOIN,
                       CASE WHEN OPERATOR_TYPE = ' || '''' || 'UnionAll' || '''' || ' and lag(OPERATOR_TYPE) over (ORDER BY OPERATOR_ID) = ' || '''' || 'Aggregate' || '''' || ' THEN 1 ELSE 0 END AS UNION_WITHOUT_ALL,
                       CASE WHEN bytes_spilled_local>0 OR bytes_spilled_remote>0 THEN 1 ELSE 0 END AS QUERIES_TOO_LARGE_MEMORY,
                       CASE WHEN partition_scan_ratio >= .8 AND partitions_total >= 20000 THEN 1 ELSE 0 END AS INEFFICIENT_PRUNING_FLAG  
                from table(get_query_operator_stats(' || '''' || record.query_id || '''' || '))
                ORDER BY STEP_ID,OPERATOR_ID
                )
                SELECT
                   QUERY_ID,
                   OPERATOR_TYPE,
                   EXPLODING_JOIN,
                   UNION_WITHOUT_ALL,
                   QUERIES_TOO_LARGE_MEMORY,
                   INEFFICIENT_PRUNING_FLAG 
                FROM query_stats';
                EXECUTE IMMEDIATE :sql_query;
            EXCEPTION
                WHEN OTHER THEN
                    SET flg := 1;
            END;
            BEGIN
                IF (flg = 0) THEN
                    CREATE TEMP TABLE DBA_DB.PUBLIC.temp_tbl_query_metrics as select QUERY_ID,OPERATOR_TYPE,EXPLODING_JOIN,UNION_WITHOUT_ALL,QUERIES_TOO_LARGE_MEMORY,INEFFICIENT_PRUNING_FLAG from table(result_scan(last_query_id()));
                    query_id_7d := (select distinct query_id from DBA_DB.PUBLIC.temp_tbl_query_metrics);
                    count_7_ej := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where EXPLODING_JOIN = 1);
                    count_7_uwa := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where UNION_WITHOUT_ALL = 1);
                    count_7_qlm := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where QUERIES_TOO_LARGE_MEMORY = 1);
                    count_7_ipl := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where INEFFICIENT_PRUNING_FLAG = 1);
                    insert_stmt_7d := 'INSERT INTO DBA_DB.PUBLIC.QUERY_OPTIMIZATION_METRICS_SUMMARY_7_DAYS values(' || '''' || query_id_7d || '''' || ',' || count_7_ej || ',' || count_7_uwa || ',' || count_7_qlm || ',' || count_7_ipl || ',CURRENT_DATE())';
                    EXECUTE IMMEDIATE :insert_stmt_7d;
                    DROP TABLE IF EXISTS DBA_DB.PUBLIC.temp_tbl_query_metrics;
                ELSE
                    continue;
                END IF;
            END;
        END FOR;
        CLOSE record;
    
        -- 8_to_14 days
        rs := (select query_id from snowflake.account_usage.query_history where datediff('day',date(start_time),current_date()) between 8 and 14 order by total_elapsed_time desc limit 100);
        LET record_2 cursor for rs;
        OPEN record_2;
        FOR record_2 in rs DO
            BEGIN
                SET flg := 0;
                sql_query := 'with query_stats as (
                select
                   QUERY_ID,
                   STEP_ID,
                   OPERATOR_ID,
                   OPERATOR_TYPE,
                   OPERATOR_STATISTICS,
                   OPERATOR_STATISTICS:output_rows output_rows,
                   OPERATOR_STATISTICS:input_rows input_rows,
                   CASE WHEN operator_statistics:input_rows>0 THEN operator_statistics:output_rows / operator_statistics:input_rows ELSE 0 END as row_multiple,
                   OPERATOR_STATISTICS:spilling:bytes_spilled_local_storage bytes_spilled_local,
                   OPERATOR_STATISTICS:spilling:bytes_spilled_remote_storage bytes_spilled_remote,
                  
                   operator_statistics:io:percentage_scanned_from_cache::float percentage_scanned_from_cache,
                   OPERATOR_STATISTICS:pruning:partitions_scanned partitions_scanned,
                   OPERATOR_STATISTICS:pruning:partitions_total partitions_total,
                   OPERATOR_STATISTICS:pruning:partitions_scanned/OPERATOR_STATISTICS:pruning:partitions_total::float as partition_scan_ratio,
                       CASE WHEN row_multiple > 1 THEN 1 ELSE 0 END AS EXPLODING_JOIN,
                       CASE WHEN OPERATOR_TYPE = ' || '''' || 'UnionAll' || '''' || ' and lag(OPERATOR_TYPE) over (ORDER BY OPERATOR_ID) = ' || '''' || 'Aggregate' || '''' || ' THEN 1 ELSE 0 END AS UNION_WITHOUT_ALL,
                       CASE WHEN bytes_spilled_local>0 OR bytes_spilled_remote>0 THEN 1 ELSE 0 END AS QUERIES_TOO_LARGE_MEMORY,
                       CASE WHEN partition_scan_ratio >= .8 AND partitions_total >= 20000 THEN 1 ELSE 0 END AS INEFFICIENT_PRUNING_FLAG   
                from table(get_query_operator_stats(' || '''' || record_2.query_id || '''' || '))
                ORDER BY STEP_ID,OPERATOR_ID
                )
                SELECT
                   QUERY_ID,
                   OPERATOR_TYPE,
                   EXPLODING_JOIN,
                   UNION_WITHOUT_ALL,
                   QUERIES_TOO_LARGE_MEMORY,
                   INEFFICIENT_PRUNING_FLAG   
                FROM query_stats';
                EXECUTE IMMEDIATE :sql_query;
            EXCEPTION
                WHEN OTHER THEN
                    SET flg := 1;
            END;
            BEGIN
                IF (flg = 0) THEN
                    CREATE TEMP TABLE DBA_DB.PUBLIC.temp_tbl_query_metrics as select QUERY_ID,OPERATOR_TYPE,EXPLODING_JOIN,UNION_WITHOUT_ALL,QUERIES_TOO_LARGE_MEMORY,INEFFICIENT_PRUNING_FLAG from table(result_scan(last_query_id()));
                    query_id_8_14d := (select distinct query_id from DBA_DB.PUBLIC.temp_tbl_query_metrics);
                    count_8_14_ej := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where EXPLODING_JOIN = 1);
                    count_8_14_uwa := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where UNION_WITHOUT_ALL = 1);
                    count_8_14_qlm := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where QUERIES_TOO_LARGE_MEMORY = 1);
                    count_8_14_ipl := (select count(*) from DBA_DB.PUBLIC.temp_tbl_query_metrics where INEFFICIENT_PRUNING_FLAG = 1);
                    insert_stmt_8_14d := 'INSERT INTO DBA_DB.PUBLIC.QUERY_OPTIMIZATION_METRICS_SUMMARY_8_14_DAYS values(' || '''' || query_id_8_14d || '''' || ',' || count_8_14_ej || ',' || count_8_14_uwa || ',' || count_8_14_qlm || ',' || count_8_14_ipl || ',CURRENT_DATE())';
                    EXECUTE IMMEDIATE :insert_stmt_8_14d;
                    DROP TABLE IF EXISTS DBA_DB.PUBLIC.temp_tbl_query_metrics;
                ELSE
                    continue;
                END IF;
            END;
        END FOR;
        CLOSE record_2;
    
        return 'Success!';
    END IF;
END;
$$;
