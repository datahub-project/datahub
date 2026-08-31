view: employee_tax_report {
  sql_table_name: data-warehouse.finance.form-16;;

  dimension: id {
    type: number
    sql: ${TABLE}.id;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name;;
  }

  measure: taxable_income {
    type: sum
    sql: ${TABLE}.tax;;
  }
}
