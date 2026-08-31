# File was added to check duplicate field issue

view: dataset_lineages {
  sql_table_name: "PUBLIC"."DATASET_LINEAGES"
    ;;

  dimension: createdon {
    type:  date
    sql: ${TABLE}."CREATEDON" ;;
  }

  dimension_group: createdon {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}."CREATEDON" ;;
  }

  dimension: entity {
    type: string
    sql: ${TABLE}."ENTITY" ;;
  }

  dimension: metadata {
    type: string
    sql: ${TABLE}."METADATA" ;;
  }

  dimension: urn {
    type: string
    sql: ${TABLE}."URN" ;;
  }

  dimension: version {
    type: number
    tags: ["softVersion"]
    sql: ${TABLE}."VERSION" ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
