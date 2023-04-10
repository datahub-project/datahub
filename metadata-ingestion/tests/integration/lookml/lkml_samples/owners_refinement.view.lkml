view: +owners {
  dimension: has_owner_name {
    type: yesno
    sql: ${TABLE}.owner_name::string is not null;;
  }
}