BEGIN
  CREATE TABLE GSL_TEST_DB.PUBLIC.new_table_1 as select * from GSL_TEST_DB.PUBLIC.processed_transactions;
  RETURN message;
END;