view: latest_account_holder_notes_base {
  filter: note_date_window {
    description: "Date window in which to look for notes"
    default_value: "90 days ago for 90 days"
    datatype: date
    type: date
  }

  dimension: account_number {
    hidden: yes
  }

  dimension: account_in_engage {
    hidden: yes
  }

  dimension_group: latest_am_note {
    type: time
    timeframes: [date, week, month, year]
    convert_tz: no
    datatype: timestamp
    sql: ${TABLE}.latest_time_created ;;
  }

  dimension: interaction_type {
    type: string
    sql: ${TABLE}.interaction_type ;;
  }

  dimension: did {
    hidden: yes
    type: string
    sql: ${TABLE}.did ;;
  }
}

view: latest_account_holder_notes {
  extends: [latest_account_holder_notes_base]
  derived_table: {
    sql: WITH notes AS (
      SELECT account_id,
        time_created,
        operator_id
      FROM `at-meta-platform-dev`.rds_performance.fct_notes
      WHERE DATE(time_created) < CURRENT_DATE()
      {% if note_date_window._is_filtered %}
        AND {% condition note_date_window %} DATE (time_created) {% endcondition %}
      {% elsif customer_performance_retailer.snapshot_date._is_filtered %}
        AND {% condition customer_performance_retailer.snapshot_date %} DATE(time_created) {% endcondition %}
      {% else %}
        AND DATE(time_created) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      {% endif %}
      AND medium NOT IN ('PORTAL', 'Trade Marketing')
      AND medium IS NOT NULL
      AND account_id IS NOT NULL
    ),

    squad AS (
      SELECT DISTINCT squad,
        LOWER(primary_sales_person.am_username) AS am_username,
        account_number,
        snapshot_date,
        did
      FROM `at-yp-fin-emp-product-dev.finance_customer_and_product.odp_dim_dealer`
      WHERE snapshot_date < CURRENT_DATE()
      {% if note_date_window._is_filtered %}
        AND {% condition note_date_window %} DATE (snapshot_date) {% endcondition %}
      {% elsif customer_performance_retailer.snapshot_date._is_filtered %}
        AND {% condition customer_performance_retailer.snapshot_date %} DATE(snapshot_date) {% endcondition %}
      {% else %}
        AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      {% endif %}
    ),

    notes_with_squad AS (
      SELECT *,
        CASE
          WHEN operator_id = am_username THEN 'AM'
          WHEN regexp_contains(am_username, 'vacant') THEN 'Vacant'
          WHEN regexp_contains(am_username, 'squad') THEN am_username
          ELSE 'Other'
        END AS interaction_type
      FROM notes n
      LEFT JOIN squad s ON n.account_id = s.account_number
      AND DATE(n.time_created) = s.snapshot_date
    ),

    notes_with_acc_numbered AS (
      SELECT account_id,
        time_created,
        operator_id,
        am_username,
        squad,
        interaction_type,
        did,
        ROW_NUMBER() OVER(PARTITION BY account_id ORDER BY time_created DESC) AS rn
      FROM notes_with_squad
      WHERE interaction_type NOT IN ('Other')
    )

    SELECT
      account_id,
      time_created AS latest_time_created,
      interaction_type,
      did
    FROM notes_with_acc_numbered
    WHERE rn = 1 ;;
  }
}
