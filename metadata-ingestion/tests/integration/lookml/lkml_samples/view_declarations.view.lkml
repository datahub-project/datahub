include: "/included_view_file.view"

view: looker_events {
  sql_table_name: looker_schema.events ;;
}

view: extending_looker_events {
  extends: [looker_events]

  measure: additional_measure {
    type: count
  }
}

view: autodetect_sql_name_based_on_view_name {}

view: test_include_external_view {
  extends: [include_able_view]
}
