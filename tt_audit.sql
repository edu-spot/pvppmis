CREATE OR REPLACE TABLE DBA_DB.PUBLIC.TIME_TRAVEL_INFORMATION (
    table_catalog STRING,
    table_schema STRING,
    table_name STRING,
    time_travel_duration INT,
    last_updated TIMESTAMP
);


CREATE OR REPLACE PROCEDURE audit_time_travel_duration()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    db_name STRING;
    schema_name STRING;
    table_name STRING;
    time_travel_duration INT;
    db_cursor CURSOR FOR SHOW DATABASES;
    schema_cursor CURSOR FOR SHOW SCHEMAS IN DATABASE identifier(:db_name);
    table_cursor CURSOR FOR SHOW TABLES IN SCHEMA identifier(:db_name || '.' || :schema_name);
BEGIN
    -- Open the database cursor to iterate through all databases
    OPEN db_cursor;
    LOOP
        FETCH db_cursor INTO db_name;
        IF (NOT FOUND) THEN
            LEAVE;
        END IF;

        -- Open the schema cursor to iterate through all schemas in the current database
        OPEN schema_cursor;
        LOOP
            FETCH schema_cursor INTO schema_name;
            IF (NOT FOUND) THEN
                LEAVE;
            END IF;

            -- Open the table cursor to iterate through all tables in the current schema
            OPEN table_cursor;
            LOOP
                FETCH table_cursor INTO table_name;
                IF (NOT FOUND) THEN
                    LEAVE;
                END IF;

                -- Get the Time Travel duration for the current table
                LET get_time_travel_duration STRING := 'SELECT COALESCE(retention_time, 0) FROM ' || db_name || '.' || schema_name || '.' || table_name || ';';
                EXECUTE IMMEDIATE :get_time_travel_duration INTO :time_travel_duration;

                -- Insert the data into the audit table
                INSERT INTO DBA_DB.PUBLIC.TIME_TRAVEL_INFORMATION (table_catalog, table_schema, table_name, time_travel_duration, last_updated)
                VALUES (:db_name, :schema_name, :table_name, :time_travel_duration, CURRENT_TIMESTAMP());

            END LOOP;
            CLOSE table_cursor;

        END LOOP;
        CLOSE schema_cursor;

    END LOOP;
    CLOSE db_cursor;

    RETURN 'Time Travel audit completed successfully.';
END;
$$;
