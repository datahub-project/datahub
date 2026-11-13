include: "book.view"

view: +book {
  dimension: issue_date_3 {
    type: number
    sql: ${TABLE}."date" ;;
  }
}


view: extend_book {
  extends: [book]
}
