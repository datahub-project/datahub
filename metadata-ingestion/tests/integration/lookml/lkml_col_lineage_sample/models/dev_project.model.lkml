# Define the database connection to be used for this model.
connection: "long-tail-companions-snowflake"

# include all the views
include: "/views/**/*.view.lkml"

# Datagroups define a caching policy for an Explore. To learn more,
# use the Quick Help panel on the right to see documentation.

datagroup: dev_project_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: dev_project_default_datagroup

explore: purchases {
  join: users {
    type: left_outer
    sql_on: ${purchases.user_fk} = ${users.pk} ;;
    relationship: many_to_one
  }

  join: user_metrics{
    type: left_outer
    sql_on:${user_metrics.user_id} = ${users.pk} ;;
    relationship: many_to_one
  }
}

# explore: users{
#   join: purchases {
#     type: left_outer
#     sql_on: ${users.pk} = ${purchases.user_fk} ;;
#     relationship: one_to_many
#   }

#   join: user_metrics{
#     type: left_outer
#     sql_on:${user_metrics.user_id} = ${users.pk} ;;
#     relationship: many_to_one
#   }
# }

explore: user_metrics {
  description: "Analyze customer segments, lifetime value, and purchasing patterns"

  join: users {
    type: inner
    sql_on: ${user_metrics.user_id} = ${users.pk} ;;
    relationship: many_to_one
  }

  join: purchases{
    type: left_outer
    sql_on: ${user_metrics.user_id} = ${purchases.user_fk} ;;
    relationship: one_to_many
  }
}

explore: customer_analysis{
  from:  users
  description: "Customer analysis and demographics"

  join: purchases {
    type: left_outer
    sql_on: ${customer_analysis.pk} = ${purchases.user_fk} ;;
    relationship: one_to_many
  }

  join: user_metrics{
    type: left_outer
    sql_on:${user_metrics.user_id} = ${customer_analysis.pk} ;;
    relationship: one_to_one

  }

  join: users {
    type: inner
    sql_on: ${customer_analysis.pk} = ${users.pk} ;;
    relationship: one_to_one
  }
}
