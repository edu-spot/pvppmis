-- Create Python UDF in Snowflake to extract table names including SELECT, JOIN, INSERT, and COPY queries
CREATE OR REPLACE FUNCTION extract_table_names_from_query(query STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('re')
HANDLER = 'extract_table_names'
AS
$$
import re

# Regular expression to match table names in INSERT, COPY, SELECT, and JOIN queries
table_name_pattern = re.compile(r'(?i)(?:(?:INSERT INTO|COPY INTO)\s+|(?:FROM|JOIN)\s+)([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+)\b')

def extract_table_names(query):
    """
    Extract table name from the given query, considering INSERT, COPY, SELECT, and JOIN queries.
    It will return all matched table names as a comma-separated string (if multiple matches).
    """
    # Find all table names based on the regex pattern
    matches = table_name_pattern.findall(query)
    if matches:
        # Return the matched table names, comma-separated
        return ', '.join(matches)
    return None
$$;
