include: "foo.view.lkml"

explore: my_view_explore {
  from: my_view
}

# Adapted from https://github.com/looker-open-source/healthcare_demo/blob/eeb03ac61c87bab65a9b6cfeb76915f38eebfe56/simplified_views/observation_vitals.view.lkml#L365

view: view_derived_explore {
  derived_table: {
    explore_source: my_view_explore {
      bind_all_filters: yes
      column: country {
        field: my_view_explore.country
      }
      column: city {
        field: my_view_explore.city
      }
      column: is_latest {
        field: my_view_explore.is_latest
      }
      derived_column: derived_col {
        sql: coalesce(country, 'US') ;;
      }
    }
  }
  dimension: country {
    type: string
  }
  dimension: city {
    type: string
  }
  measure: unique_countries {
    type: count_distinct
    sql: ${TABLE}.country ;;
  }
  measure: derived_col {
    type: sum
    sql: ${TABLE}.is_latest ;;
  }
}
