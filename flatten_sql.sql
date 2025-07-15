CREATE OR REPLACE PROCEDURE flatten_table_with_dynamic_attributes()
RETURNS TABLE ()
LANGUAGE SQL
AS
$$
DECLARE
  dynamic_sql STRING;
  column_list STRING;
BEGIN
  -- Extract dynamic keys from attributes
  WITH keys AS (
    SELECT DISTINCT f.key
    FROM my_table,
         LATERAL FLATTEN(input => attributes) f
  )
  SELECT LISTAGG(
  'COALESCE(' ||
    'TRY_TO_NUMBER(attributes:"' || key || '"), ' ||
    'TRY_TO_BOOLEAN(attributes:"' || key || '"), ' ||
    'attributes:"' || key || '"::STRING' ||
  ') AS "' || key || '"',
  ', '
  ) INTO :column_list
  FROM keys;

  -- Construct full SQL
  LET dynamic_sql = '
    SELECT 
      t.id,
      flattened_id.value::STRING AS individual_id,
      ' || column_list || '
    FROM my_table t,
         LATERAL FLATTEN(input => t.ids) AS flattened_id
  ';

  -- Return the dynamic query
  RETURN TABLE(EXECUTE IMMEDIATE :dynamic_sql);
END;
$$;
