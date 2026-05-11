include: "book.view"

view: +book {
  dimension: issue_date {
    type: string
    sql: ${TABLE}."date" ;;
  }
}
