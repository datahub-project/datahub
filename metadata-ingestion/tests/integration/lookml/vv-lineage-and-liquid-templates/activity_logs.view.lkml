view: activity_logs {
  sql_table_name:
        {{ _dev_database_attributes['dev_database_prefix'] }}_analytics.{{ _user_attributes['dev_schema_prefix'] }}_staging_app.stg_app__activity_logs;;

  dimension: generated_message_id {
    group_label: "IDs"
    primary_key: yes
    type: number
    sql: ${TABLE}."GENERATED_MESSAGE_ID" ;;
  }
}
