view: top_10_employee_income_source {
  derived_table: {
    sql: SELECT id,
                name,
                source
         FROM ${employee_income_source.SQL_TABLE_NAME}
         ORDER BY source desc
         LIMIT 10
  ;;
  }

  dimension: id {
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }
}