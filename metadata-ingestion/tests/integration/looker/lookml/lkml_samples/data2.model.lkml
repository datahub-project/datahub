connection: "my_other_connection"
include: "**/*.view.lkml"

explore: aliased_explore2 {
  from: my_view2
}

explore: duplicate_explore {
  from: my_view
}