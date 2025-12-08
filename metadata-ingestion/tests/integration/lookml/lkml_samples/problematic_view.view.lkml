view: problematic_view {
  sql_table_name: test_db.test_schema.problematic_table ;;

  dimension: field_0 {
    type: string
    sql: ${TABLE}.field_0 ;;
  }

  dimension: field_1 {
    type: string
    sql: ${TABLE}.field_1 ;;
  }

  # Note: This view is used for testing individual field fallback.
  # The actual fields are mocked via the Looker API in tests.
  # We only define a couple of fields here for the view to be valid.
}

