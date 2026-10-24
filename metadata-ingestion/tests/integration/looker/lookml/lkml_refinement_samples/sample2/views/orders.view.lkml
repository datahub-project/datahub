view: orders {
  sql_table_name: public.orders ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}."id" ;;
  }

  dimension: amount {
    type: number
    sql: ${TABLE}."amount" ;;
  }
}
