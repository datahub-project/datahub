connection: "my_connection"

explore: included_sql_preamble {
  sql_preamble:
    CREATE TEMP FUNCTION CONCAT_VERBOSE(a STRING, b STRING)
    RETURNS STRING AS (
      CONCAT(a, b)
    );
  ;;
}
