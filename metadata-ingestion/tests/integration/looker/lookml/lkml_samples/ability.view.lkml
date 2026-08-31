view: ability {
  sql_table_name: "ECOMMERCE"."ABILITY"
    ;;

  dimension: pk {
    type: number
    sql: ${TABLE}."PK" ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
