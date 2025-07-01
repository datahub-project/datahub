view: employee_total_income {
  sql_table_name: ${employee_income_source.SQL_TABLE_NAME} ;;

  dimension: id {
    type: number
    sql: ${TABLE}.id;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name;;
  }

  measure: total_income {
    type: sum
    sql: ${TABLE}.income;;
  }
}
