connection: "my_connection"

include: "foo.view.lkml"

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
