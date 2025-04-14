view: my_derived_view {
  derived_table: {
    sql:
        SELECT
          country,
          city,
          timestamp,
          measurement
        FROM
          ${my_view.SQL_TABLE_NAME} AS my_view ;;
  }

  dimension: country {
    type: string
    description: "The country"
    sql: ${TABLE}.country ;;
  }

  dimension: city {
    type: string
    description: "City"
    sql: ${TABLE}.city ;;
  }

  dimension_group: timestamp {
    group_label: "Timestamp"
    type: time
    description: "Timestamp of measurement"
    sql: ${TABLE}.timestamp ;;
    timeframes: [hour, date, week, day_of_week]
  }

  measure: average_measurement {
    group_label: "Measurement"
    type: average
    description: "My measurement"
    sql: ${TABLE}.measurement ;;
  }

}
