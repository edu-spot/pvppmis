----------------------------------------------------------------------
--Table DDL
----------------------------------------------------------------------

CREATE OR REPLACE TABLE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION (
    table_catalog STRING,
    table_schema STRING,
    table_name STRING,
    no_of_rows INTEGER,
    owner STRING,
    current_time_travel_duration INT,
    change_tt VARCHAR(1),
    last_updated TIMESTAMP,
    comments STRING
);


----------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.ALTER_USER_STMT_TIMEOUT()
RETURNS STRING
LANGUAGE SQL
EXECUTE as CALLER
AS
$$
DECLARE
    rs RESULTSET;
    rs1 RESULTSET;
    rs2 RESULTSET;
    rs3 RESULTSET;
    rs4 RESULTSET;
    stmt VARCHAR;
    updt_stmt VARCHAR;
    delete_stmt VARCHAR;
    insert_stmt VARCHAR;
    flg INTEGER;
    cnt INTEGER;
BEGIN
    rs1 := (SELECT * from DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION);
    rs := (SELECT "TABLE_CATALOG" as db_name,"TABLE_SCHEMA" as db_schema,"TABLE_NAME" as db_table,"CURRENT_TIME_TRAVEL_DURATION" as retention_time,"NO_OF_ROWS" as nrows,"OWNER" as t_owner from table(result_scan(LAST_QUERY_ID())) where "CHANGE_TT" = 'Y');
    cnt := (select count(*) from table(RESULT_SCAN(LAST_QUERY_ID())));
    if (cnt > 0) THEN
        LET record cursor for rs;
        OPEN record; 
        FOR record in rs DO
            BEGIN
                LET flg := 0;
                BEGIN
                    stmt := 'ALTER TABLE ' || record.db_name || '.' || record.db_schema || '.' || record.db_table || ' SET DATA_RETENTION_TIME_IN_DAYS = 3';
                    rs2 := (EXECUTE IMMEDIATE :stmt);
                EXCEPTION
                    WHEN OTHER THEN
                        SET flg := 1;
                END;
                BEGIN
                    IF (flg = 0) THEN
                        insert_stmt := 'INSERT INTO DBA_DB.PUBLIC.TIME_TRAVEL_AUDIT_INFORMATION values (' || '''' || record.db_name || '''' || ',' || '''' || record.db_schema || '''' || ',' || '''' || record.db_table || '''' || ',' || record.nrows || ',' || '''' || record.t_owner || '''' || ',' || record.retention_time || ',CURRENT_TIMESTAMP(),' || '''' || 'Time Travel Duration Changed to 3 Days' || '''' || ')';

                        --Update the record for Testing cases | Use DELETE later
                        rs3 := (EXECUTE IMMEDIATE :insert_stmt);
                        updt_stmt := 'UPDATE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION SET CHANGE_TT = ' || '''' || 'N' || '''' || ',CURRENT_TIME_TRAVEL_DURATION = 3 WHERE TABLE_CATALOG = ' || '''' || record.db_name || '''' || ' AND TABLE_SCHEMA = ' || '''' || record.db_schema || '''' || ' AND TABLE_NAME = ' || '''' || record.db_table || '''';
                        rs4 := (EXECUTE IMMEDIATE :updt_stmt);

                        -- Use delete once finalized
                        --delete_stmt := 'DELETE FROM DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION WHERE TABLE_CATALOG = ' || '''' || record.db_name || '''' || ' AND TABLE_SCHEMA = ' || '''' || record.db_schema || '''' || ' AND TABLE_NAME = ' || '''' || record.db_table || '''';
                        --rs4 := (EXECUTE IMMEDIATE :delete_stmt);
                    ELSE
                        insert_stmt := 'INSERT INTO DBA_DB.PUBLIC.TIME_TRAVEL_AUDIT_INFORMATION values (' || '''' || record.db_name || '''' || ',' || '''' || record.db_schema || '''' || ',' || '''' || record.db_table || '''' || ',' || record.nrows || ',' || '''' || record.t_owner || '''' || ',' || record.retention_time || ',CURRENT_TIMESTAMP(),' || '''' || 'Failed to Change Time Travel Duration' || '''' || ')';
                        rs3 := (EXECUTE IMMEDIATE :insert_stmt);
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



--------------------------------------------------------------
--Housecleaning done Proc
--------------------------------------------------------------

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
                        updt_stmt := 'UPDATE DBA_DB.PUBLIC.TIME_TRAVEL_TABLE_INFORMATION set COMMENTS =  ' || '''' || 'Time Travel Duration Changed' || '''' || ',CURRENT_TIME_TRAVEL_DURATION = 3,LAST_UPDATED = CURRENT_TIMESTAMP(),CHANGE_TT = ' || '''' || 'N' || '''' || ' where TABLE_CATALOG = ' || '''' || record.db_name || '''' || ' AND TABLE_SCHEMA = ' || '''' || record.db_schema || '''' || ' AND TABLE_NAME = ' || '''' || record.db_table || '''';
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


----------------------------------------------

USE ROLE USERADMIN;
CREATE ROLE tag_admin;
USE ROLE ACCOUNTADMIN;
GRANT CREATE TAG on SCHEMA <database.schema_name> to ROLE tag_admin;

# grants usage access on database and schema to tag_admin role where the tags are created
USE ROLE SYSADMIN;
GRANT USAGE on DATABASE <database> to ROLE tag_admin;
GRANT USAGE on SCHEMA <database.schema_name> to ROLE tag_admin;


# Assume the tags are created in database  DBA_DB and schema  PUBLIC
USE ROLE tag_admin;
USE SCHEMA DBA_DB.PUBLIC;
CREATE TAG department ALLOWED_VALUES 'transfers','finance' COMMENT='department tag';


# global privilege - tag_admin role can apply tags on account
USE ROLE ACCOUNTADMIN;
GRANT APPLY TAG on ACCOUNT to ROLE tag_admin;

# grant privileges to apply a single tag
GRANT APPLY on TAG DBA_DB.PUBLIC.cost_center to ROLE Data_Owner;



USE ROLE tag_admin;
# Apply tag while creating a new warehouse
CREATE WAREHOUSE my_warehouse1 WITH TAG(DBA_DB.PUBLIC.department='TDB');

# Apply tag for an existing warehouse by altering the object
ALTER WAREHOUSE my_warehouse2 SET TAG DBA_DB.PUBLIC.department='TDB';


# Assume the tags are stored in database  DBA_DB and schema PUBLIC
SHOW TAGS in ACCOUNT;
SHOW TAGS in DATABASE DBA_DB;
SHOW TAGS in SCHEMA DBA_DB.PUBLIC;


