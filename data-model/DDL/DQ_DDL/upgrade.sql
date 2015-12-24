-- script to run after loading dump file from old data quality db
alter table dq_agg_log change column run_id run_id bigint NOT NULL AUTO_INCREMENT COMMENT 'execution id of aggregation';
alter table dq_agg_log add column request_time timestamp NULL DEFAULT NULL COMMENT 'timestamp when the aggregation requested';
alter table dq_agg_log change column run_time start_time timestamp NULL DEFAULT NULL COMMENT 'timestamp when the aggregation run';

alter table dq_agg_def drop column last_run;
alter table dq_agg_def drop column last_run_id;