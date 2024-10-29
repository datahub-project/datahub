SELECT
    name
  , type_name(user_type_id) AS 'type'
FROM sys.parameters
WHERE object_id = object_id('{procedure_name}');