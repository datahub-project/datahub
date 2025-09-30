# The name of this view in Looker is "Users"
view: users {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: "ECOMMERCE"."USERS" ;;

  # No primary key is defined for this view. In order to join this view in an Explore,
  # define primary_key: yes on a dimension that has no repeated values.

  # Dates and timestamps can be represented in Looker using a dimension group of type: time.
  # Looker converts dates and timestamps to the specified timeframes within the dimension group.

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}."CREATED_AT" ;;
  }
    # Here's what a typical dimension looks like in LookML.
    # A dimension is a groupable field that can be used to filter query results.
    # This dimension will be called "Email" in Explore.

  dimension: email {
    type: string
    sql: ${TABLE}."EMAIL" ;;
  }

  dimension: pk {
    primary_key: yes
    type: number
    sql: ${TABLE}."PK" ;;
  }

  dimension_group: updated {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}."UPDATED_AT" ;;
  }

  measure: lifetime_purchase_count{
    type: count_distinct
    sql: ${purchases.pk} ;;
    description: "Total lifetime purchases count by user"
  }

  measure: lifetime_total_purchase_amount{
    type: sum
    sql: ${purchases.total_amount};;
    value_format_name: usd
    description: "Total lifetime revenue from purchases by user"
  }

  dimension: user_purchase_status{
    type: string
    sql:
      CASE
        WHEN ${user_metrics.purchase_count} <= 1 THEN 'First Purchase'
        WHEN ${user_metrics.purchase_count} <= 3 THEN 'Early Customer'
        WHEN ${user_metrics.purchase_count} <= 10 THEN 'Regular Customer'
        ELSE 'Loyal Customer'
      END ;;
  }

  dimension_group: user_age {
    type: duration
    sql_start: ${TABLE}."CREATED_AT" ;;
    sql_end: CURRENT_TIMESTAMP ;;
    intervals: [day, week, month, quarter, year]
  }

  measure: count {
    type: count
  }
}
