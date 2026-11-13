view: partial_view {
  sql_table_name: test_db.test_schema.partial_table ;;

  dimension: field_0 {
    type: string
    sql: ${TABLE}.field_0 ;;
  }

  dimension: field_1 {
    type: string
    sql: ${TABLE}.field_1 ;;
  }

  # Note: This view is used for testing partial lineage results.
  # The actual fields are mocked via the Looker API in tests.
  # We only define a couple of fields here for the view to be valid.
}

