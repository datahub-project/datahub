include: "parent_view.view.lkml"

view: child_view {
  extends: [parent_view]

  dimension: id {
    primary_key: yes
    type: integer
    sql: ${TABLE}.id ;;
  }

  dimension: child_dimension_1 {
    type: string
    sql: ${TABLE}.child_dimension_1 ;;
  }
}