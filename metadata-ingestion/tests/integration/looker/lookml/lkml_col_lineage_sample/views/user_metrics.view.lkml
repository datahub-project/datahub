view: user_metrics {
  derived_table: {
    sql: SELECT
           user_fk as user_id,
           COUNT(DISTINCT pk) as purchase_count,
           SUM(total_amount) as total_spent
         FROM ${purchases.SQL_TABLE_NAME}
         GROUP BY user_id ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
    primary_key: yes
  }

  dimension: purchase_count {
    type: number
    sql: ${TABLE}.purchase_count ;;
  }

  dimension: total_spent {
    type: number
    sql: ${TABLE}.total_spent ;;
  }

# Cross-view dimension with conditional logic
  dimension: customer_segment {
    type: string
    sql: CASE
           WHEN ${total_spent} > 1000 THEN 'High Value'
           WHEN ${total_spent} > 500 THEN 'Medium Value'
           ELSE 'Low Value'
         END ;;
  }

# Cross-view measure with filtering
  measure: high_value_customer_count {
    type: count_distinct
    sql: CASE WHEN ${total_spent} > 1000 THEN ${users.pk} END ;;
    description: "Count of customers who spent over $1000"
  }
}
