view: book {
  sql_table_name: public.book ;;

  dimension: name {
    type: string
    sql: ${TABLE}."name" ;;
  }

  measure: count {
    type: count
    drill_fields: [name]
  }
}

view: +book {
  dimension: date {
    type: string
    sql: ${TABLE}."date" ;;
  }
}
