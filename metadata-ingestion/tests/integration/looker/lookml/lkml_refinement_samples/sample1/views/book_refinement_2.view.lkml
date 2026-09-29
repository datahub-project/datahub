include: "book.view"
include: "/views/book_refinement_1.view"

view: +book {
  dimension: issue_date {
    type: number
    sql: ${TABLE}."date" ;;
  }
}
