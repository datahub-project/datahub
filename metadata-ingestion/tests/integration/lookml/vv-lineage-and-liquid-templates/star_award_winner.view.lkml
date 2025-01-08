view: star_award_winner {
  sql_table_name: ${db}.@{star_award_winner_year};;


  dimension: id {
    label: "id"
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  parameter: star_award_winner_year {
    type: string
    allowed_value: {
      label: "Star Award Winner Of 2025"
      value: "public.winner_2025"
    }
    allowed_value: {
      label: "Star Award Winner Of 2024"
      value: "public.winner_2024"
    }
  }

}