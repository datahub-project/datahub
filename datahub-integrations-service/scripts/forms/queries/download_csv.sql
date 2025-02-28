-- filter_type = [form_id, domain, assignee_urn]; informed by page context
-- lookback_window = [7 days, 30 days, 90 days, 365 days]; informed by date selection; default = 7 days

-- Logic to be used anytime someone clicks "Download CSV"
select
  today() as export_date
  , form_id
  , form_assigned_date
  , form_completed_date
  , form_type
  , form_status
  , domain
  , asset_urn
  , assignee_urn
  , count(distinct(case when question_status = 'complete' then question_id end)) as questions_complete
  , count(distinct(case when question_status = 'not_started' then question_id end)) as questions_not_started
from
  '{{table}}'
where
  snapshot_date = (select max(snapshot_date) from '{{table}}')
  and form_assigned_date >= current_date() - interval '{{ interval_day | default('365 DAY', true) }}'


--- TBD: what happens when a form is assigned to an entity, but there is no implicit owner? 
  and assignee_urn is not null
group by
    current_date()
  , form_id
  , form_assigned_date
  , form_completed_date
  , form_type
  , form_status
  , domain
  , asset_urn
  , assignee_urn
;