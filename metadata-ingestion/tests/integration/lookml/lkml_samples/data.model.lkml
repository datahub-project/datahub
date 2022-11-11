connection: "my_connection"

include: "foo.view.lkml"
include: "bar.view.lkml"
include: "nested/*"
include: "liquid.view.lkml"
include: "ability.view.lkml"

explore: aliased_explore {
  from: my_view
}

explore: dataset_owners{
  join: all_entities {
    relationship: many_to_one
    sql_on: ${all_entities.urn} =  ${dataset_owners.urn};;
  }
}

explore: data_model {
  label: "Data model!"
  description: "Lorem ipsum"

  always_filter: {
    filters: {
      field: is_latest_forecast
      value: "TRUE"
    }
    filters: {
      field: granularity
      value: "day"
    }
  }
}