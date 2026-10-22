connection: "db-connection"

include: "/views/orders.view"
include: "/views/customers.view"
include: "/views/order_summary.view"
include: "/views/audit_mixin.view"
# Refinements in LookML files that are not view files
include: "/refinements/orders.layer.lkml"
include: "/refinements/order_summary.lkml"

explore: orders {}

explore: customers {}

explore: order_summary {}

# Refinement in the model file itself
view: +customers {
  sql_table_name: @{analytics_schema}.customers ;;
}

# Applied after the refinements from included files
view: +orders {
  sql_table_name: analytics.orders_final ;;
}
