view: rent_as_employee_income_source {
  sql_table_name: (
         SELECT id,
                name,
                source
         FROM ${employee_income_source.SQL_TABLE_NAME}
         WHERE source = "RENT"
         ORDER BY source desc
         LIMIT 10
  );;


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