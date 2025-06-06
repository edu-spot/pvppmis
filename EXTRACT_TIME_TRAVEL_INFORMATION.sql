CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.TIME_TRAVEL_INFORMATION_EXTRACT()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    rs RESULTSET;
    rs1 RESULTSET;
    rs2 RESULTSET;
    rs3 RESULTSET;
    rs4 RESULTSET;
    rs5 RESULTSET;
    rs6 RESULTSET;
    rs7 RESULTSET;
    rs8 RESULTSET;
    db_stmt VARCHAR;
    stmt VARCHAR;
    stmt1 VARCHAR;
    stmt2 VARCHAR;
    insert_stmt VARCHAR;
    flg INTEGER;
    nr_flag INTEGER;
    sch_flg INTEGER;
BEGIN
    TRUNCATE TABLE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION;
    SHOW DATABASES in ACCOUNT;
    rs := (SELECT "name" as db_name from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    LET record cursor for rs;
    OPEN record; 
    FOR record in rs DO
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
                    stmt1 := 'show schemas in database ' || record.db_name;
                    rs3 := (EXECUTE IMMEDIATE :stmt1);
                    rs4 := (SELECT "name" as schema_name from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
                    LET schema_record cursor for rs4;
                    OPEN schema_record; 
                    FOR schema_record in rs4 DO
                        LET sch_flg := 0;
                        BEGIN
                            stmt2 := 'show tables in ' || record.db_name || '.' || schema_record.schema_name;
                            rs5 :=  (EXECUTE IMMEDIATE :stmt2);
                        EXCEPTION
                            WHEN OTHER THEN
                            SET sch_flg := 1;
                        END;
                        BEGIN
                            if (sch_flg = 0) THEN
                                LET nr_flag := 0;
                                rs6 := (SELECT "database_name" as db_name,"schema_name" as sch_name,"name" as tbl_name,"rows" as nrows,"owner" as t_owner,"retention_time" as ret_time from TABLE(RESULT_SCAN(LAST_QUERY_ID())) where "retention_time" > 3);
                                nr_flag := (select count(*) from table(RESULT_SCAN(LAST_QUERY_ID())));
                                if (nr_flag > 0) THEN
                                    LET row_record cursor for rs6;
                                    OPEN row_record;
                                    FOR row_record in rs6 DO
                                        BEGIN
                                            insert_stmt := 'INSERT INTO DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION values (' || '''' || row_record.db_name || '''' || ',' || '''' || row_record.sch_name || '''' || ',' || '''' || row_record.tbl_name || '''' || ',' || row_record.nrows || ',' || '''' || row_record.t_owner || '''' || ',' || row_record.ret_time || ',' || '''' || 'Y' || '''' || ')';
                                            rs8 := (EXECUTE IMMEDIATE :insert_stmt);
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


---------------------------------------------------
--Housecleaning done Proc
---------------------------------------------------



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
    db_resultset := (SELECT "name" as db_name from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
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