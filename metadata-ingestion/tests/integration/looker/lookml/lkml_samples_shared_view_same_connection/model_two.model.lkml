connection: "my_connection"

include: "shared.view.lkml"

explore: explore_two {
  from: shared_view
}
