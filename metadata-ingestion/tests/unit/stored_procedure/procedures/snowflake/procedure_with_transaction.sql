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

