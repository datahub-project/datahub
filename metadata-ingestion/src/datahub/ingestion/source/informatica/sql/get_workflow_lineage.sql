WITH DEPS AS (
SELECT DEP.FROM_INST_ID,
       DEP.TO_INST_ID,
       TO_SESSION.SESSION_ID   AS TO_SESSION_ID,
       TO_MAPPING.MAPPING_ID   AS TO_MAPPING_ID,
       TO_MAPPING.MAPPING_NAME AS TO_MAPPING_NAME,
       FROM_TI.INSTANCE_NAME   AS FROM_INSTANCE,
       FROM_TI.TASK_TYPE       AS FROM_TASK_TYPE,
       TO_TI.INSTANCE_NAME     AS TO_INSTANCE,
       TO_TI.TASK_TYPE         AS TO_TASK_TYPE
FROM {metadata_schema}.OPB_WFLOW_DEP dep
            JOIN {metadata_schema}.OPB_TASK_INST from_ti
ON dep.FROM_INST_ID = from_ti.INSTANCE_ID
    JOIN {metadata_schema}.OPB_TASK_INST to_ti ON dep.TO_INST_ID = to_ti.INSTANCE_ID
    LEFT OUTER JOIN {metadata_schema}.OPB_SESSION to_session ON to_ti.task_id = to_session.session_id
    LEFT OUTER JOIN {metadata_schema}.OPB_MAPPING to_mapping ON to_session.mapping_id = to_mapping.mapping_id
WHERE dep.workflow_id = :workflow_id
    )
SELECT FROM_INST_ID,
       TO_INST_ID,
       TO_SESSION_ID,
       TO_MAPPING_ID,
       TO_MAPPING_NAME,
       FROM_INSTANCE,
       FROM_TASK_TYPE,
       TO_INSTANCE,
       TO_TASK_TYPE,
       LEVEL AS FLOW_DEPTH
FROM DEPS START WITH
            FROM_INST_ID IN (
                SELECT FROM_INST_ID
                FROM deps
                WHERE from_task_type = 62
            )
CONNECT BY
    PRIOR TO_INST_ID = FROM_INST_ID
ORDER BY
    FLOW_DEPTH, FROM_INSTANCE, TO_INSTANCE
