view: last_ptr_holder_fake_notes_base {
  filter: finance_date_range {
    default_value: "60 days ago for 60 days"
    datatype: date
    type: date
  }

  dimension: acc_number {
    hidden: yes
  }

  dimension: fnc_in_engage {
    hidden: yes
  }

  dimension_group: last_finance_fake_notes {
    type: time
    timeframes: [date, week, month, year]
    convert_tz: no
    datatype: timestamp
    sql: ${TABLE}.last_timestamp_created ;;
  }

  dimension: fake_type {
    type: string
    sql: ${TABLE}.fake_type ;;
  }

  dimension: fid {
    hidden: yes
    type: string
    sql: ${TABLE}.fid ;;
  }
}

view: last_ptr_holder_fake_notes {
  extends: [last_ptr_holder_fake_notes_base]
  derived_table: {
    sql: WITH fake_notes AS (
      SELECT foo_id,
        created_on_date,
        opt_id
      FROM `at-meta-platform-dev`.db_testing.finance_quotes
      WHERE DATE(created_on_date) < CURRENT_DATE()
      {% if finance_date_range._is_filtered %}
        AND {% condition finance_date_range %} DATE (created_on_date) {% endcondition %}
      {% elsif employee_testing_rpt.snapshot_timestamp._is_filtered %}
        AND {% condition employee_testing_rpt.snapshot_timestamp %} DATE(created_on_date) {% endcondition %}
      {% else %}
        AND DATE(created_on_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      {% endif %}
      AND medium NOT IN ('WINDOW', 'Foo Sale')
      AND medium IS NOT NULL
      AND foo_id IS NOT NULL
    ),

    abc AS (
      SELECT DISTINCT abc,
        LOWER(primary_sales_person.cus_name) AS cus_name,
        acc_number,
        snapshot_timestamp,
        fid
      FROM `at-yp-fin-emp-product-dev.finance_customer_and_product.odp_dim_dealer`
      WHERE snapshot_timestamp < CURRENT_DATE()
      {% if finance_date_range._is_filtered %}
        AND {% condition finance_date_range %} DATE (snapshot_timestamp) {% endcondition %}
      {% elsif employee_testing_rpt.snapshot_timestamp._is_filtered %}
        AND {% condition employee_testing_rpt.snapshot_timestamp %} DATE(snapshot_timestamp) {% endcondition %}
      {% else %}
        AND snapshot_timestamp >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      {% endif %}
    ),

    fns_with_abc AS (
      SELECT *,
        CASE
          WHEN opt_id = cus_name THEN 'AM'
          WHEN regexp_contains(cus_name, 'foo') THEN 'foo'
          WHEN regexp_contains(cus_name, 'abc') THEN cus_name
          ELSE 'Other'
        END AS fake_type
      FROM fake_notes n
      LEFT JOIN abc s ON n.foo_id = s.acc_number
      AND DATE(n.created_on_date) = s.snapshot_timestamp
    ),

    fake_notes_with_acc_numbered AS (
      SELECT foo_id,
        created_on_date,
        opt_id,
        cus_name,
        abc,
        fake_type,
        fid,
        ROW_NUMBER() OVER(PARTITION BY foo_id ORDER BY created_on_date DESC) AS rn
      FROM fns_with_abc
      WHERE fake_type NOT IN ('Other')
    )

    SELECT
      foo_id,
      created_on_date AS last_timestamp_created,
      fake_type,
      fid
    FROM fake_notes_with_acc_numbered
    WHERE rn = 1 ;;
  }
}
