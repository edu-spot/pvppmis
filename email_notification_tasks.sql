






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
    html_body STRING;
    table_rows STRING;
BEGIN
    SELECT LISTAGG(
        '<tr>' ||
        '<td>' || NAME || '</td>' ||
        '<td>' || DATABASE_NAME || '</td>' ||
        '<td>' || SCHEMA_NAME || '</td>' ||
        '<td>' || LEFT(ERROR_MESSAGE, 100) || '</td>' ||
        '<td>' || TO_CHAR(QUERY_START_TIME, 'YYYY-MM-DD HH24:MI:SS') || '</td>' ||
        '</tr>',
        ''
    )
    INTO :table_rows
    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
        TIMESTAMPADD('HOUR', -1, CURRENT_TIMESTAMP()), 
        CURRENT_TIMESTAMP()
    ))
    WHERE STATE = 'FAILED';

    IF (table_rows IS NULL) THEN
        SET html_body = '<p>No task failures found in the past hour.</p>';
    ELSE
        SET html_body = 
            '<p>The following tasks have failed in the past hour:</p>' ||
            '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">' ||
            '<tr>' ||
            '<th>Task Name</th>' ||
            '<th>Database</th>' ||
            '<th>Schema</th>' ||
            '<th>Error</th>' ||
            '<th>Start Time</th>' ||
            '</tr>' || 
            :table_rows || 
            '</table>';
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
