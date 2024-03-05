-- filter_type = [form_id, domain, assignee_urn]; informed by page context
-- filter_value informed by user-entered value on "By Form", "By Domain", or "By Asssignee tab"
-- lookback_window = [7 days, 30 days, 90 days, 365 days]; informed by date selection; default = 7 days

-- Overall documentation progress by assigned date; can be filtered by a specific Form, Domain, or Assignee
select
  form_status

-- truncate aggregation window based on lookback filter applied; 7d & 30d remain at daily grain
  {% if {{ lookback_window }} == '90 days' %}
    , date_trunc('week', form_assigned_date)::date as form_assigned_week
  {% elif {{ lookback_window }} == '365 days' %}
    , date_trunc('month', form_assigned_date)::date as form_assigned_month
  {% else %}
    , form_assigned_date
   {% endif %}

  , count(distinct(asset_urn)) as asset_count
from
  {{ table }}
where
  assignee_urn is not null
  and snapshot_date = (select max(snapshot_date) from {{table}})
--- TBD: what happens when a form is assigned to an entity, but there is no implicit owner?  
  and form_assigned_date >= current_date() - interval '{{ lookback_window }}'

--- JINJA BLOCK NOT TESTED
-- If user is on the "Overall" tab, the following should be omitted
-- If the user is on the "By Form", "By Domain", or "By Assignee" tab, apply filter_type & filter_value
{% if pageName not 'overall' %}

  and {{ filter_type }} = '{{ filter_value }}'

{% endif %}

group by
  form_status
  {% if {{ lookback_window }} == '90 days' %}
    , date_trunc('week', form_assigned_date)::date as form_assigned_week
  {% elif {{ lookback_window }} == '365 days' %}
    , date_trunc('month', form_assigned_date)::date as form_assigned_month
  {% else %}
    , form_assigned_date
  {% endif %}
;