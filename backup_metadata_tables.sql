CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.backup_metadata_table(
    table_name STRING,
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
    failed_flag INT;
BEGIN
    SET backup_table := table_name || '_BACKUP';
    SET start_time := CURRENT_TIMESTAMP();

    SET failed_flag := 0;
    -- Full load from source to backup
    IF (load_type = 'initial') THEN
        BEGIN
            create_stmt := 'CREATE TABLE ' || backup_table || ' AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.' || table_name || ' limit 5000';
            EXECUTE IMMEDIATE :create_stmt;
        EXCEPTION
            WHEN OTHER THEN
                SET failed_flag := 1;
        END;
        
        BEGIN
            IF (failed_flag = 1) THEN
                return 'Failed while Creating table. Check if it already exists!';
            ELSE
                stmt := 'SELECT COUNT(*) as cnt FROM ' || backup_table;
                rs := (EXECUTE IMMEDIATE :stmt);
                LET record cursor for rs;
                OPEN record; 
                FOR record in rs DO
                    SET rows_loaded := record.cnt;
                END FOR;

                -- Insert audit record
                INSERT INTO DBA_DB.PUBLIC.METADATA_BACKUP_LOAD_STATS(RUN_DATE, TABLE_NAME, LOAD_TYPE, START_TIME, ROW_COUNT) VALUES (CURRENT_TIMESTAMP(), :table_name, :load_type, :start_time, :rows_loaded);
            END IF;
        END;
    ELSEIF (load_type = 'delta') THEN
        -- Get max start date from backup
        max_start_stmt := 'SELECT COALESCE(MAX(start_time), DATEADD(DAY, -3, CURRENT_DATE)) FROM ' || backup_table;
        EXECUTE IMMEDIATE :max_start_stmt;

        -- IF (max_start_date >= DATEADD(DAY, -3, CURRENT_DATE)) THEN
        --     BREAK;
        -- END IF;

        -- Insert delta records into backup
        insert_backup_stmt := 'INSERT INTO ' || backup_table || ' SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.' || table_name || ' WHERE DATE(START_TIME) > ' || max_start_date || ' AND DATE(START_TIME) <= DATEADD(DAY, -3, CURRENT_DATE)';
        EXECUTE IMMEDIATE :insert_backup_stmt;

        --Getting no of rows inserted
        rows_loaded := (SELECT "number of rows inserted" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));

        -- Insert audit record
        INSERT INTO DBA_DB.PUBLIC.METADATA_BACKUP_LOAD_STATS(RUN_DATE, TABLE_NAME, LOAD_TYPE, START_TIME, ROW_COUNT) VALUES (CURRENT_TIMESTAMP(), :table_name, :load_type, :start_time, :rows_loaded);

    ELSE
        RETURN 'Invalid load type';
    END IF;

    RETURN 'Success: ' || rows_loaded || ' records loaded into ' || backup_table;
END;
$$;
