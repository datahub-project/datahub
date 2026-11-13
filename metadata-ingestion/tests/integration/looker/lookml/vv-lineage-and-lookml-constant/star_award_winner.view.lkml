view: star_award_winner {
  sql_table_name: @{customer_support_db}.@{customer_support_schema}.@{invalid_constant};;


  dimension: id {
    label: "id"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

}