view: customer_facts {
  derived_table: {
    sql:
      SELECT
        customer_id,
        SUM(sale_price) AS lifetime_spend
      FROM
        order
      WHERE
        {% if order.region == "ap-south-1" %}
            region = "AWS_AP_SOUTH_1"
        {% else %}
            region = "GCP_SOUTH_1"
        {% endif %}
      GROUP BY 1
    ;;
    }
}
