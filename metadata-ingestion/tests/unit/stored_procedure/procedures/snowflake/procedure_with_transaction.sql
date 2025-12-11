-- SPDX-License-Identifier: Apache-2.0
--
-- This file is unmodified from its original version developed by Acryl Data, Inc.,
-- and is now included as part of a repository maintained by the National Digital Twin Programme.
-- All support, maintenance and further development of this code is now the responsibility
-- of the National Digital Twin Programme.

BEGIN
-- snowflake stored procedure with begin transaction and commit, write to table in transaction
BEGIN transaction
insert into "QUERY_HISTORY"
SELECT query_id, start_time, user FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY";
COMMIT
RETURN 'Success';
EXCEPTION
WHEN OTHER THEN
ROLLBACK;
raise;
--RETURN 'Error';
END;

