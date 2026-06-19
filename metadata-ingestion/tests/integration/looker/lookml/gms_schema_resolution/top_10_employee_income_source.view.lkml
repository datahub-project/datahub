view: top_10_employee_income_source {
   sql_table_name: "db.public.employee"
    ;;
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