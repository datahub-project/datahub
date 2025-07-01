view: environment_activity_logs {
  sql_table_name:  -- if prod -- prod.staging_app.stg_app__activity_logs
                   -- if dev -- {{ _user_attributes['dev_database_prefix'] }}analytics.{{ _user_attributes['dev_schema_prefix'] }}staging_app.stg_app__activity_logs
                   ;;

  dimension: generated_message_id {
    group_label: "IDs"
    primary_key: yes
    type: number
    sql: ${TABLE}."GENERATED_MESSAGE_ID" ;;
  }
}
