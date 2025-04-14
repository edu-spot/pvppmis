CREATE OR REPLACE PROCEDURE backup_metadata_table(
    table_name STRING,
    load_type STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    backup_table STRING;
    max_load_date TIMESTAMP;
    start_time TIMESTAMP;
    rows_loaded INT;
BEGIN
    LET backup_table = table_name || '_BACKUP';
    LET start_time = CURRENT_TIMESTAMP;

    IF load_type = 'Initial' THEN
        -- Full load from source to backup
        EXECUTE IMMEDIATE
        'INSERT INTO ' || backup_table || '
         SELECT * FROM ' || table_name;

    ELSIF load_type = 'Delta' THEN
        -- Get max load date from backup
        LET max_load_date = (
            SELECT COALESCE(MAX(LOAD_DATE), DATEADD(DAY, -3, CURRENT_DATE))
            FROM IDENTIFIER(backup_table)
        );

        -- Insert delta records into backup
        EXECUTE IMMEDIATE
        'INSERT INTO ' || backup_table || '
         SELECT * FROM ' || table_name || '
         WHERE LOAD_DATE > ?'
        USING max_load_date;

    ELSE
        RETURN 'Invalid load type';
    END IF;

    -- Audit: count rows loaded
    LET rows_loaded = (
        SELECT COUNT(*)
        FROM IDENTIFIER(backup_table)
        WHERE LOAD_DATE >= start_time
    );

    -- Insert audit record
    INSERT INTO METADATA_BACKUP_LOAD_STATS(RUN_DATE, TABLE_NAME, LOAD_TYPE, START_TIME, ROW_COUNT)
    VALUES (CURRENT_TIMESTAMP, table_name, load_type, start_time, rows_loaded);

    RETURN 'Success: ' || rows_loaded || ' records loaded into ' || backup_table;
END;
$$;
