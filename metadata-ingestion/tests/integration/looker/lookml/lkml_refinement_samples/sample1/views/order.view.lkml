view: order {
  sql_table_name: public.order ;;

  dimension: order_id {
    type: number
    sql: ${TABLE}."order_id" ;;
  }

  dimension: book_id {
    type: number
    sql: ${TABLE}."book_id" ;;
  }

}