view: activity_logs {
  sql_table_name:
        {% if _user_attributes['looker_env'] == 'dev' %}
          {{ _user_attributes['dev_database_prefix'] }}analytics.{{ _user_attributes['dev_schema_prefix'] }}staging_app.stg_app__activity_logs
        {% elsif _user_attributes['looker_env'] == 'prod' %}
          analytics.staging_app.stg_app__activity_logs
        {% else %}
          analytics.staging_app.stg_app__activity_logs
        {% endif %}
        ;;

  dimension: generated_message_id {
    group_label: "IDs"
    primary_key: yes
    type: number
    sql: ${TABLE}."GENERATED_MESSAGE_ID" ;;
  }
}
