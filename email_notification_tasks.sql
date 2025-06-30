






--Step 1.1: Create an Email Integration (admin task)

CREATE NOTIFICATION INTEGRATION my_email_integration
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('your.email@example.com');

--Step 1.2: Grant usage
GRANT USAGE ON INTEGRATION my_email_integration TO ROLE <your_role>;

-------------------------------------------------------------------------------
--Create Alert

CREATE OR REPLACE ALERT task_failure_alert
WAREHOUSE = <your_warehouse>
SCHEDULE = '1 MINUTE'
IF (
    EXISTS (
        SELECT 1
        FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TIMESTAMPADD('HOUR', -1, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP())) 
        WHERE STATE = 'FAILED'
    )
)
THEN
    CALL NOTIFY_TASK_FAILURE();

-------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE NOTIFY_TASK_FAILURE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    task_report STRING;
BEGIN
    -- Fetch and format failed task info from the past 1 hour
    SELECT LISTAGG(
        'Task Name: ' || TASK_NAME || 
        ', Database: ' || DATABASE_NAME ||
        ', Schema: ' || SCHEMA_NAME ||
        ', Error: ' || ERROR_MESSAGE ||
        ', Start Time: ' || TO_CHAR(START_TIME, 'YYYY-MM-DD HH24:MI:SS'),
        '\n'
    )
    INTO :task_report
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TIMESTAMPADD('HOUR', -1, CURRENT_TIMESTAMP()), 
        CURRENT_TIMESTAMP()
    ))
    WHERE STATE = 'FAILED';

    IF :task_report IS NULL THEN
        LET task_report = 'No task failures found in the past hour.';
    END IF;

    -- Send email with the report
    CALL SYSTEM$SEND_EMAIL(
        'my_email_integration',
        'your.email@example.com',
        'Task Failure Alert',
        'The following tasks have failed in the past hour:\n\n' || :task_report
    );

    RETURN 'Email Sent with Task Failure Details';
END;
$$;
