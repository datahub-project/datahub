-- SPDX-License-Identifier: Apache-2.0
--
-- This file is unmodified from its original version developed by Acryl Data, Inc.,
-- and is now included as part of a repository maintained by the National Digital Twin Programme.
-- All support, maintenance and further development of this code is now the responsibility
-- of the National Digital Twin Programme.


BEGIN
  CREATE TABLE TEST_DB.PUBLIC.new_table_1 as select id, value from TEST_DB.PUBLIC.processed_transactions;
  RETURN message;
END;
