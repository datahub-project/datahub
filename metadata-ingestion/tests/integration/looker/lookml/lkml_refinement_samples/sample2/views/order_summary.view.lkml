view: order_summary {
  derived_table: {
    sql: SELECT customer_id, SUM(amount) AS total_amount FROM public.orders GROUP BY 1 ;;
    persist_for: "24 hours"
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}."customer_id" ;;
  }

  dimension: total_amount {
    type: number
    sql: ${TABLE}."total_amount" ;;
  }
}
