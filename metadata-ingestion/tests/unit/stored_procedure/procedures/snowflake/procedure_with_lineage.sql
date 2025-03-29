
BEGIN
  CREATE TABLE TEST_DB.PUBLIC.new_table_1 as select id, value from TEST_DB.PUBLIC.processed_transactions;
  RETURN message;
END;
