-- Top Performing Forms
select
  form_id 
  , count(distinct(case when form_status = 'complete' then asset_urn end)) 
		/ count(distinct(asset_urn)) as completed_asset_percent
from
  {{table}}
where
  snapshot_date = (select max(snapshot_date) from {{table}})
--- TBD: what happens when a form is assigned to an entity, but there is no implicit owner?
  and assignee_urn is not null
group by
  form_id
order by
  count(distinct(case when form_status = 'complete' then asset_urn end)) 
		/ count(distinct(asset_urn)) desc
limit 2 -- limit should be enforced by front end
;