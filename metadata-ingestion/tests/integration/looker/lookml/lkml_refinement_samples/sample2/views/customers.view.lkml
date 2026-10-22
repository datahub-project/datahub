view: customers {
  sql_table_name: public.customers ;;

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}."id" ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}."name" ;;
  }
}
