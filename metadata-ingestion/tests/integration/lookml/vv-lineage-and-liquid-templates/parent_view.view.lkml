view: parent_view {
  sql_table_name: `dataset.table` ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: parent_dimension_1 {
    type: string
    sql: ${TABLE}.parent_dimension_1 ;;
  }

  measure: parent_count {
    type: count
  }
}