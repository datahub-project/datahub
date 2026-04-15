view: star_award_winner_dev {
  sql_table_name: @{customer_support_db}.@{customer_support_schema}.@{winner_table};;


  dimension: id {
    label: "id"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name;;
  }

}