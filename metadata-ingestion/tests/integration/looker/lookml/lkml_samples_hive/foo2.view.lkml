view: my_view2 {
  derived_table: {
    sql:
        SELECT
          is_latest,
          country,
          city,
          timestamp,
          measurement
        FROM
          my_table ;;
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

  dimension: is_latest {
    type: yesno
    description: "Is latest data"
    sql: ${TABLE}.is_latest ;;
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
