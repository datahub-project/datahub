BEGIN
  CREATE TEMPORARY TABLE temp1 as select id, value from TEST_DB.PUBLIC.processed_transactions;
  CREATE TABLE TEST_DB.PUBLIC.new_table_2 as select * from temp1;
  RETURN message;
END