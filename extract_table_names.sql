-- Create Python UDF in Snowflake to extract table names including SELECT, JOIN, INSERT, and COPY queries (no 're' package)
CREATE OR REPLACE FUNCTION extract_table_names_from_query(query STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'extract_table_names'
AS
$$
def extract_table_names(query):
    """
    Extract table names from a query, including INSERT, COPY, SELECT, and JOIN queries.
    The function does not use regular expressions and relies on basic string manipulation.
    """
    # Convert query to uppercase for case-insensitive matching
    query = query.upper()
    
    # Define keywords to match
    keywords = ['INSERT INTO', 'COPY INTO', 'FROM', 'JOIN']
    
    # Initialize a list to hold table names
    table_names = []
    
    # Search for table names after "INSERT INTO" or "COPY INTO"
    for keyword in keywords:
        if keyword in query:
            # Split the query by the keyword and process each part
            parts = query.split(keyword)
            for part in parts[1:]:
                # For "INSERT INTO" or "COPY INTO", find the table name immediately following
                table_part = part.strip().split(' ')[0]  # Take the first word after the keyword
                # Check if it's a valid table name (simple or fully qualified)
                if '.' in table_part or table_part.isalnum():
                    table_names.append(table_part)
    
    # If table names are found, return them as a comma-separated string
    return ', '.join(table_names) if table_names else None
$$;
