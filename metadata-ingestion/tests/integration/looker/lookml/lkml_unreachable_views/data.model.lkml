connection: "my_connection"

include: "employee_income_source.view.lkml"
include: "employee_total_income.view.lkml"

explore: employee_income_source {
}

explore: employee_total_income {
}
