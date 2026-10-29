include: "/views/orders.view"

view: +orders {
  # extends in a refinement is not applied, and must not drop the view
  extends: [audit_mixin]
  sql_table_name: analytics.orders ;;

  dimension: amount {
    sql: ${TABLE}."amount_usd" ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}."status" ;;
  }
}
