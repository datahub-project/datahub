WITH DEPS AS (
    SELECT DISTINCT mapping_id, from_instance_id, to_instance_id
    FROM {metadata_schema}.OPB_WIDGET_DEP
    WHERE mapping_id = :mapping_id
),
WIDGET_DEP AS (
    SELECT
        CONNECT_BY_ROOT to_instance_id     AS root_to_instance_id,
        to_instance_id                     AS pre_from_instance_id,
        from_instance_id                   AS final_from_instance_id,
        CONNECT_BY_ISLEAF                  AS is_leaf,
        mapping_id
    FROM DEPS
    START WITH to_instance_id NOT IN (
        SELECT DISTINCT from_instance_id FROM DEPS
    )
    CONNECT BY NOCYCLE PRIOR from_instance_id = to_instance_id
)
SELECT DISTINCT
    WIDGET_DEP.mapping_id,
    WIDGET_DEP.pre_from_instance_id,
    WIDGET_DEP.final_from_instance_id from_instance_id,
    FROM_WI.widget_id   AS from_widget_id,
    FROM_WI.instance_name AS from_widget_name,
    FROM_WI.widget_type AS from_widget_type,
    WIDGET_DEP.root_to_instance_id as to_instance_id,
    TO_WI.widget_id     AS to_widget_id,
    TO_WI.instance_name AS to_widget_name,
    TO_WI.widget_type AS to_widget_type
FROM WIDGET_DEP
JOIN {metadata_schema}.OPB_WIDGET_INST FROM_WI
  ON WIDGET_DEP.final_from_instance_id = FROM_WI.instance_id
 AND FROM_WI.mapping_id = WIDGET_DEP.mapping_id
JOIN {metadata_schema}.OPB_WIDGET_INST TO_WI
  ON WIDGET_DEP.root_to_instance_id = TO_WI.instance_id
 AND TO_WI.mapping_id = WIDGET_DEP.mapping_id
WHERE FROM_WI.widget_type IN (1, 3, 45)
ORDER BY FROM_WI.widget_type DESC
