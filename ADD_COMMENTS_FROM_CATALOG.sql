CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.ADD_COLUMN_COMMENTS_FROM_CATALOG(db_arg STRING DEFAULT 'NO_DB',db_schema STRING DEFAULT 'NO_SCHEMA', db_table STRING DEFAULT 'NO_TABLE')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rs RESULTSET;
    alter_stmt VARCHAR;
    insert_stmt VARCHAR;
    filter_query VARCHAR;
    flg INTEGER;
    exc_flg INTEGER;
    count INTEGER;
BEGIN
    SET flg := 0;
    BEGIN
        if (db_arg = 'NO_DB') THEN
            filter_query := 'SELECT DATABASE_NAME as db_name,SCHEMA_NAME as schema_name,TABLE_NAME as tbl_name,COLUMN_NAME as col_name,COMMENTS as comment from DBA_DB.PUBLIC.CATALOG_TABLE_POC';
        ELSE
            if (db_schema = 'NO_SCHEMA') THEN
                filter_query := 'SELECT DATABASE_NAME as db_name,SCHEMA_NAME as schema_name,TABLE_NAME as tbl_name,COLUMN_NAME as col_name,COMMENTS as comment from DBA_DB.PUBLIC.CATALOG_TABLE_POC WHERE DATABASE_NAME = ' || '''' || db_arg || '''';
            ELSE
                if (db_table = 'NO_TABLE') THEN
                    filter_query := 'SELECT DATABASE_NAME as db_name,SCHEMA_NAME as schema_name,TABLE_NAME as tbl_name,COLUMN_NAME as col_name,COMMENTS as comment from DBA_DB.PUBLIC.CATALOG_TABLE_POC WHERE DATABASE_NAME = ' || '''' || db_arg || '''' || ' AND SCHEMA_NAME = ' || '''' || db_schema || '''';
                ELSE
                    filter_query := 'SELECT DATABASE_NAME as db_name,SCHEMA_NAME as schema_name,TABLE_NAME as tbl_name,COLUMN_NAME as col_name,COMMENTS as comment from DBA_DB.PUBLIC.CATALOG_TABLE_POC WHERE DATABASE_NAME = ' || '''' || db_arg || '''' || ' AND SCHEMA_NAME = ' || '''' || db_schema || '''' || ' AND TABLE_NAME = ' || '''' || db_table || '''';
                END IF;
            END IF;
        END IF;
        rs := (EXECUTE IMMEDIATE :filter_query);
        count := (SELECT count(*) from table(result_scan(last_query_id())));
        if (count > 0) THEN
            LET record cursor for rs;
            OPEN record;
            FOR record in rs DO
                BEGIN
                    SET exc_flg := 0;
                    BEGIN
                        alter_stmt := 'ALTER TABLE ' || record.db_name || '.' || record.schema_name || '.' || record.tbl_name || ' ALTER COLUMN ' || record.col_name || ' comment ' || '''' || record.comment || '''';
                        EXECUTE IMMEDIATE :alter_stmt;
                        -- return alter_stmt;
                    EXCEPTION
                        WHEN OTHER THEN
                            SET exc_flg := 1;
                    END;
                    BEGIN
                        if (exc_flg = 1) THEN
                            insert_stmt := 'INSERT INTO DBA_DB.PUBLIC.COMMENT_TABLE_FAILED VALUES (' || '''' || record.db_name || '''' || ',' || '''' || record.schema_name || '''' || ',' || '''' || record.tbl_name || '''' || ')';
                            EXECUTE IMMEDIATE :insert_stmt;
                        ELSE
                            continue;
                        END IF;
                    END;
                END;
            END FOR;
        ELSE
            return 'No tables to process!';
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            SET flg := 1;
    END;
    BEGIN
        if (flg = 1) THEN
            return 'Failed due to some issue.';
        ELSE
            return 'Success !';
        END IF;
    END;
END;
$$;