connection: "db-connection"

# include all the views
include: "/views/book.view"
include: "/views/book_refinement_2.view"
include: "/views/extend_book.view"
include: "/views/order.view"

datagroup: model_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: model_default_datagroup

explore: order {}

explore: book {}

explore: +book {
  extends: [order]
}

explore: extend_book {}

explore: issue_history {}
