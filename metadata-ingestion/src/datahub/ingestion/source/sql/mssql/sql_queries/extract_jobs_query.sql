SELECT
    job.job_id
  , job.name
  , job.description
  , job.date_created
  , job.date_modified
  , steps.step_id
  , steps.step_name
  , steps.subsystem
  , steps.command
  , steps.database_name
FROM msdb.dbo.sysjobs AS job
INNER JOIN msdb.dbo.sysjobsteps AS steps
    ON job.job_id = steps.job_id
WHERE database_name = '{{db_name}}';
