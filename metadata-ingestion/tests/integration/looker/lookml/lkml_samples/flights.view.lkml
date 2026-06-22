view: flights {
  sql_table_name: flightstats.accidents ;;

  dimension: id {
    label: "id"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }
}

# override type of id parameter
view: +flights {
  dimension: id {
    label: "id"
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }
}