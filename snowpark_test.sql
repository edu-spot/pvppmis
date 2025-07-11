-- Step 1: Create the procedure in SQL using Snowpark
CREATE OR REPLACE PROCEDURE join_tables_sp()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

def run(session: Session) -> str:
    # Read input tables
    df1 = session.table("TABLE1")
    df2 = session.table("TABLE2")
    
    # Join on 'id' column (adjust as needed)
    joined_df = df1.join(df2, df1["id"] == df2["id"], "inner")
    
    # Select relevant columns (adjust as needed)
    result_df = joined_df.select(df1["id"], df1["name"], df2["value"])
    
    # Save result to target table (overwrite mode)
    result_df.write.save_as_table("JOINED_TABLE", mode="overwrite")
    
    return "Join successful and written to JOINED_TABLE"
$$;
