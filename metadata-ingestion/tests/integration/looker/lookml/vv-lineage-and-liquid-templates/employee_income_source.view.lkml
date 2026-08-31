view: employee_income_source {
  derived_table: {
    sql: SELECT
      employee_id,
      employee_name,
      {% if dw_eff_dt_date._is_selected or finance_dw_eff_dt_date._is_selected %}
        prod_core.data.r_metric_summary_v2
      {% elsif dw_eff_dt_week._is_selected or finance_dw_eff_dt_week._is_selected %}
        prod_core.data.r_metric_summary_v3
      {% else %}
        'default_table' as source
      {% endif %},
      employee_income
    FROM source_table
    WHERE
        {% condition source_region %} source_table.region {% endcondition %}
  ;;
  }

  dimension: id {
    type: number
    sql: ${TABLE}.employee_id;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.employee_name;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: income {
    type: number
    sql: ${TABLE}.employee_income ;;
  }

}
