view: audit_mixin {
  extension: required

  dimension: updated_by {
    type: string
    sql: ${TABLE}."updated_by" ;;
  }
}
