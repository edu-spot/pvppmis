CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.delete_metadata_older_than_3_years(
    table_name STRING,
    column_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rs RESULTSET;
    nrecords INT;
    delete_stmt VARCHAR;
    count_stmt VARCHAR;
BEGIN
    IF (:table_name = '') THEN
        return 'Table Name is Null. Please enter a Table Name!';
    END IF;
    IF (:column_name = '') THEN
        return 'Column Name is Null. Please enter a Table Name!';
    END IF;
    BEGIN
        count_stmt := 'SELECT count(*) as cnt from ' || table_name || ' where ' || column_name || ' < ' || 'DATEADD(YEAR,-3,DATEADD(DAY,-3,CURRENT_DATE()))';
        rs := (EXECUTE IMMEDIATE :count_stmt);
        LET record cursor for rs;
        OPEN record; 
        FOR record in rs DO
            SET nrecords := record.cnt;
        END FOR;

        delete_stmt := 'DELETE from ' || table_name || ' where ' || column_name || ' < ' || 'DATEADD(YEAR,-3,CURRENT_DATE())';
        -- return delete_stmt;
        EXECUTE IMMEDIATE :delete_stmt;
    EXCEPTION
        WHEN OTHER THEN
            return 'Failed due to one of the reasons: 1. Incorrect Table Name. 2. Incorrect Column Name.';
    END;

    return 'No of records : ' || :nrecords || ' deleted from table : ' || :table_name;
END;
$$;
