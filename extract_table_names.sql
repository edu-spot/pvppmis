CREATE OR REPLACE FUNCTION EXTRACT_TABLE_NAMES(QUERY_TEXT STRING)
RETURNS ARRAY
LANGUAGE JAVASCRIPT
AS
$$
// Regular expression to match table names in FROM and JOIN clauses
const regex = /(?:FROM|JOIN)\s+([^\s;(),]+)/gi;
let matches = [];
let match;
while ((match = regex.exec(QUERY_TEXT)) {
    matches.push(match[1]);
}
return matches;
$$;
