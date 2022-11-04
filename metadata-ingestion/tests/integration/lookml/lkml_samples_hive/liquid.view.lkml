view: customer_facts {
  derived_table: {
sql:
      SELECT
        customer_id,
        SUM(sale_price) AS lifetime_spend
      FROM
        order
      WHERE
        {% condition order_region %} order.region {% endcondition %}
      GROUP BY 1
    ;;
    }
}
