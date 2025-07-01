SELECT TASK.TASK_ID                     AS WORKFLOW_ID,
       TASK.TASK_NAME                   AS WORKFLOW_NAME,
       TASK.COMMENTS                    AS WORKFLOW_DESC,
       SERVER.SERVER_NAME               AS SERVER_NAME,
       SCHEDULER.SCHEDULER_NAME         AS SCHEDULER_NAME,
       SCHEDULER.START_TIME             AS START_TIME,
       SCHEDULER.END_TIME               AS END_TIME,
       SCHEDULER.RUN_OPTIONS            AS RUN_OPTIONS,
       SCHEDULER.DELTA_VALUE            AS DELTA_VALUE,
       SCHEDULE_LOGIC.USER_LOGIC_TYPE   AS USER_LOGIC_TYPE,
       SCHEDULE_LOGIC.FREQUENCY_INTERVL AS FREQUENCY_INTERVAL,
       SCHEDULE_LOGIC.DAILY_LOGIC       AS DAILY_LOGIC,
       SCHEDULE_LOGIC.WEEKLY_LOGIC      AS WEEKLY_LOGIC,
       SCHEDULE_LOGIC.MONTHLY_LOGIC     AS MONTHLY_LOGIC,
       TASK.LAST_SAVED                  AS LAST_UPDATE_TIME
FROM {metadata_schema}.OPB_TASK task
            JOIN {metadata_schema}.OPB_SUBJECT subject
ON
    task.subject_id = subject.subj_id
    LEFT OUTER JOIN {metadata_schema}.OPB_WORKFLOW wf ON
    task.task_id = wf.workflow_id
    LEFT OUTER JOIN {metadata_schema}.OPB_SERVER_INFO server ON
    wf.server_id = server.server_id
    LEFT OUTER JOIN {metadata_schema}.OPB_SCHEDULER scheduler ON
    wf.scheduler_id = scheduler.scheduler_id
    LEFT OUTER JOIN {metadata_schema}.OPB_SCHEDULE_LOGIC schedule_logic ON
    scheduler.scheduler_id = schedule_logic.scheduler_id
WHERE subject.subj_id = :folder_id
  AND task.task_type = '71'
  AND task.is_enabled = 1
ORDER BY task_name
