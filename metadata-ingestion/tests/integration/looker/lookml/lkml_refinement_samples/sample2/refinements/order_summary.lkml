include: "/views/order_summary.view"

view: +order_summary {
  derived_table: {
    sql: SELECT customer_id, SUM(amount_usd) AS total_amount FROM analytics.orders GROUP BY 1 ;;
  }
}
