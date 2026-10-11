connection: "db-connection"

# include all the views
include: "/views/book.view"
include: "/views/issue_history.view"

explore: book_with_additional_properties {
  view_name: book
}

explore: issue_history {}
