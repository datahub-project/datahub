connection: "my_connection"

include: "/view_declarations.view"

explore: second_model {
  label: "Second model!"
  description: "Lorem ipsum"

## This section doesn't work yet because of a bug in the lkml parser.
## See https://github.com/joshtemple/lkml/issues/59.
##  measure: bookings_measure {
##    label: "Number of new bookings"
##    group_label: "New bookings"
##    description: "A distinct count of all new bookings"
##    sql: ${booking_id} ;;
##    type: count_distinct
##    filters: [ state: "CLOSED" ,
##      name: "New Bookings"]
##  }
}
