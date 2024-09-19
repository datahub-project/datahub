SELECT
    create_date AS date_created
  , modify_date AS date_modified
FROM sys.procedures
WHERE object_id = object_id('{{procedure_name}}');