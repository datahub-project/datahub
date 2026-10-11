view: top_10_employee_income_source {
  derived_table: {
    sql: SELECT id,
                name,
                source
         FROM hive.employee_db.income_source
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