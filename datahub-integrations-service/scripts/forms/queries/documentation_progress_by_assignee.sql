-- filter_type = [form_id, domain]; informed by page context
-- filter_value informed by user-entered value on "By Form" or "By Domain" tab
-- lookback_window = [7 days, 30 days, 90 days, 365 days]; informed by date selection; default = 7 days

-- Documentation progress by assignee; can be filtered by a specific Form or Domain
select
  assignee_urn 
  , count(distinct(case when form_status = 'not_started' then asset_urn end)) as not_started_asset_count
  , count(distinct(case when form_status = 'in_progress' then asset_urn end)) as in_progress_asset_count
  , count(distinct(case when form_status = 'complete' then asset_urn end)) as completed_asset_count
  , count(distinct(asset_urn)) as total_asset_count
from
  '{{ table }}'
where
  snapshot_date = (select max(snapshot_date) from {{ table }})
  and form_assigned_date >= current_date() - interval '{{ lookback_window }}'

--- JINJA BLOCK NOT TESTED
-- If user is on the "Overall" tab, the following should be omitted
-- If the user is on the "By Form" or "By Domain" tab, apply filter_type & filter_value
{% if pageName not 'overall' %}

  and {{ filter_type }} = '{{ filter_value }}'

{% endif %}

--- TBD: what happens when a form is assigned to an entity, but there is no implicit owner?
  and assignee_urn is not null

group by
  assignee_urn
;