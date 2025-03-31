CREATE OR REPLACE PROCEDURE DBA_DB.PUBLIC.Alter_Type_ServiceAccount()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    users_resultset RESULTSET;
    alter_stmt VARCHAR;
    failed_stmt VARCHAR;
    flg INTEGER;
BEGIN
    show users like 'S_%' in account;
    users_resultset := (SELECT "name" as service_acc_name from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    --users_resultset := (SELECT 'S_ADPT_DM_DEV' as service_acc_name);
    LET record cursor for users_resultset;
    OPEN record; 
    FOR record in users_resultset DO
        BEGIN
            LET flg := 0;
            BEGIN
                alter_stmt := 'ALTER USER ' || '"' || record.service_acc_name || '"' || ' SET TYPE = LEGACY_SERVICE';
                EXECUTE IMMEDIATE :alter_stmt;
            --Exception handling to ensure Proc doesn't fail if executing user is not having access to Database
            EXCEPTION
                WHEN OTHER THEN
                    SET flg := 1;
            END;
            BEGIN
                IF (flg = 0) THEN
                    continue;
                ELSE
                    failed_stmt := 'Failed for ' || record.service_acc_name || ' : ' || alter_stmt;
                    return failed_stmt;
                END IF;
            END;
        END;
    END FOR;
    return 'Success!';
END;
$$;