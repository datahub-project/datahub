connection: "my_connection"

include: "activity_logs.view.lkml"
include: "employee_income_source.view.lkml"
include: "employee_total_income.view.lkml"
include: "top_10_employee_income_source.view.lkml"
include: "employee_tax_report.view.lkml"
include: "employee_salary_rating.view.lkml"
include: "environment_activity_logs.view.lkml"
include: "employee_income_source_as_per_env.view.lkml"
include: "rent_as_employee_income_source.view.lkml"
include: "child_view.view.lkml"

explore: activity_logs {
}

explore: employee_income_source {
}

explore: employee_total_income {
}

explore: top_10_employee_income_source {
}

explore: employee_tax_report {
}

explore: employee_salary_rating {
}

explore: environment_activity_logs {
}

explore: employee_income_source_as_per_env {
}

explore: rent_as_employee_income_source {
}

explore: child_view {
}
