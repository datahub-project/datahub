# The name of this view in Looker is "Purchases"
view: purchases {
  # The sql_table_name parameter indicates the underlying database table
  # to be used for all fields in this view.
  sql_table_name: "ECOMMERCE"."PURCHASES" ;;

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
    # This dimension will be called "Pk" in Explore.

  dimension: pk {
    primary_key: yes
    type: number
    sql: ${TABLE}."PK" ;;
  }

  dimension: purchase_amount {
    type: number
    sql: ${TABLE}."PURCHASE_AMOUNT" ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}."STATUS" ;;
  }

  dimension: tax_amount {
    type: number
    sql: ${TABLE}."TAX_AMOUNT" ;;
  }

  dimension: total_amount {
    type: number
    sql: ${TABLE}."TOTAL_AMOUNT" ;;
  }

  dimension_group: updated {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}."UPDATED_AT" ;;
  }

  dimension: user_fk {
    type: number
    sql: ${TABLE}."USER_FK" ;;
  }

  # Inter View Dimension References
  dimension: is_expensive_purchase {
    type: yesno
    sql: ${total_amount} > 100 ;;
  }

  # Inter View Nested Dimension References
  measure: num_of_expensive_purchases {
    type: count
    drill_fields: [is_expensive_purchase]
  }

  # Intra View Dimension Reference
  dimension: user_email{
    type: string
    sql:${users.email} ;;
  }

  measure: average_purchase_value{
    type: average
    sql: ${total_amount} ;;
    value_format_name: usd
  }

  measure: count {
    type: count
  }

  dimension_group: purchase_age {
    type: duration
    sql_start: ${TABLE}."CREATED_AT" ;;
    sql_end: CURRENT_TIMESTAMP ;;
    intervals: [day, week, month, quarter, year]
  }
}
