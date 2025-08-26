-- Documentation progress by form
select
  form_id 
  , count(distinct(case when form_status = 'non_started' then asset_urn end)) as not_started_asset_count
  , count(distinct(case when form_status = 'in_progress' then asset_urn end)) as in_progress_asset_count
  , count(distinct(case when form_status = 'complete' then asset_urn end)) as completed_asset_count
  , count(distinct(asset_urn)) as total_asset_count
from
  {{table}}
where
  snapshot_date = (select max(snapshot_date) from {{table}})
--- TBD: what happens when a form is assigned to an entity, but there is no implicit owner?
  and assignee_urn is not null
group by
  form_id
;