view: owners {
  dimension: id {
    primary_key: yes
    sql: ${TABLE}.id ;;
  }
  dimension: owner_name {
    sql: ${TABLE}.owner_name ;;
  }
}