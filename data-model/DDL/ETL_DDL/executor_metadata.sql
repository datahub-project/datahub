--
-- Copyright 2015 LinkedIn Corp. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--

CREATE TABLE flow (
  app_id               SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id              INT UNSIGNED      NOT NULL
  COMMENT 'flow id either inherit from source or generated',
  flow_name            VARCHAR(255) COMMENT 'name of the flow',
  flow_group           VARCHAR(255) COMMENT 'flow group or project name',
  flow_path            VARCHAR(1024) COMMENT 'flow path from top level',
  flow_level           SMALLINT COMMENT 'flow level, 0 for top level flow',
  source_created_time  INT UNSIGNED COMMENT 'source created time of the flow',
  source_modified_time INT UNSIGNED COMMENT 'latest source modified time of the flow',
  source_version       VARCHAR(255) COMMENT 'latest source version of the flow',
  is_active            CHAR(1) COMMENT 'determine if it is an active flow',
  is_scheduled         CHAR(1) COMMENT 'determine if it is a scheduled flow',
  pre_flows            VARCHAR(2048) COMMENT 'comma separated flow ids that run before this flow',
  main_tag_id          INT COMMENT 'main tag id',
  created_time         INT UNSIGNED COMMENT 'wherehows created time of the flow',
  modified_time        INT UNSIGNED COMMENT 'latest wherehows modified time of the flow',
  wh_etl_exec_id       BIGINT COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (app_id, flow_id),
  INDEX flow_path_idx (app_id, flow_path(255)),
  INDEX flow_name_idx (app_id, flow_group(127), flow_name(127))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow (
  app_id               SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id              INT UNSIGNED COMMENT 'flow id either inherit from source or generated',
  flow_name            VARCHAR(255) COMMENT 'name of the flow',
  flow_group           VARCHAR(255) COMMENT 'flow group or project name',
  flow_path            VARCHAR(1024) COMMENT 'flow path from top level',
  flow_level           SMALLINT COMMENT 'flow level, 0 for top level flow',
  source_created_time  INT UNSIGNED COMMENT 'source created time of the flow',
  source_modified_time INT UNSIGNED COMMENT 'latest source modified time of the flow',
  source_version       VARCHAR(255) COMMENT 'latest source version of the flow',
  is_active            CHAR(1) COMMENT 'determine if it is an active flow',
  is_scheduled         CHAR(1) COMMENT 'determine if it is a scheduled flow',
  pre_flows            VARCHAR(2048) COMMENT 'comma separated flow ids that run before this flow',
  main_tag_id          INT COMMENT 'main tag id',
  created_time         INT UNSIGNED COMMENT 'wherehows created time of the flow',
  modified_time        INT UNSIGNED COMMENT 'latest wherehows modified time of the flow',
  wh_etl_exec_id       BIGINT COMMENT 'wherehows etl execution id that modified this record',
  INDEX flow_id_idx (app_id, flow_id),
  INDEX flow_path_idx (app_id, flow_path(255))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_source_id_map (
  app_id           SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id          INT UNSIGNED      NOT NULL AUTO_INCREMENT
  COMMENT 'flow id generated ',
  source_id_string VARCHAR(1024) COMMENT 'source string id of the flow',
  source_id_uuid   VARCHAR(255) COMMENT 'source uuid id of the flow',
  source_id_uri    VARCHAR(255) COMMENT 'source uri id of the flow',
  PRIMARY KEY (app_id, flow_id),
  INDEX flow_path_idx (app_id, source_id_string(255))
)
  ENGINE = MyISAM
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow id mapping table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_job (
  app_id               SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id              INT UNSIGNED      NOT NULL
  COMMENT 'flow id',
  first_source_version VARCHAR(255) COMMENT 'first source version of the flow under this dag version',
  last_source_version  VARCHAR(255) COMMENT 'last source version of the flow under this dag version',
  dag_version          INT               NOT NULL
  COMMENT 'derived dag version of the flow',
  job_id               INT UNSIGNED      NOT NULL
  COMMENT 'job id either inherit from source or generated',
  job_name             VARCHAR(255) COMMENT 'job name',
  job_path             VARCHAR(1024) COMMENT 'job path from top level',
  job_type_id          SMALLINT COMMENT 'type id of the job',
  job_type             VARCHAR(63) COMMENT 'type of the job',
  ref_flow_id          INT UNSIGNED NULL COMMENT 'the reference flow id of the job if the job is a subflow',
  pre_jobs             VARCHAR(20000) CHAR SET latin1 COMMENT 'comma separated job ids that run before this job',
  post_jobs            VARCHAR(20000) CHAR SET latin1 COMMENT 'comma separated job ids that run after this job',
  is_current           CHAR(1) COMMENT 'determine if it is a current job',
  is_first             CHAR(1) COMMENT 'determine if it is the first job',
  is_last              CHAR(1) COMMENT 'determine if it is the last job',
  created_time         INT UNSIGNED COMMENT 'wherehows created time of the flow',
  modified_time        INT UNSIGNED COMMENT 'latest wherehows modified time of the flow',
  wh_etl_exec_id       BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, job_id, dag_version),
  INDEX flow_id_idx (app_id, flow_id),
  INDEX ref_flow_id_idx (app_id, ref_flow_id),
  INDEX job_path_idx (app_id, job_path(255))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler job table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow_job (
  app_id         SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id        INT UNSIGNED COMMENT 'flow id',
  flow_path      VARCHAR(1024) COMMENT 'flow path from top level',
  source_version VARCHAR(255) COMMENT 'last source version of the flow under this dag version',
  dag_version    INT COMMENT 'derived dag version of the flow',
  job_id         INT UNSIGNED COMMENT 'job id either inherit from source or generated',
  job_name       VARCHAR(255) COMMENT 'job name',
  job_path       VARCHAR(1024) COMMENT 'job path from top level',
  job_type_id    SMALLINT COMMENT 'type id of the job',
  job_type       VARCHAR(63) COMMENT 'type of the job',
  ref_flow_id    INT UNSIGNED  NULL COMMENT 'the reference flow id of the job if the job is a subflow',
  ref_flow_path  VARCHAR(1024) COMMENT 'the reference flow path of the job if the job is a subflow',
  pre_jobs       VARCHAR(20000) CHAR SET latin1 COMMENT 'comma separated job ids that run before this job',
  post_jobs      VARCHAR(20000) CHAR SET latin1 COMMENT 'comma separated job ids that run after this job',
  is_current     CHAR(1) COMMENT 'determine if it is a current job',
  is_first       CHAR(1) COMMENT 'determine if it is the first job',
  is_last        CHAR(1) COMMENT 'determine if it is the last job',
  wh_etl_exec_id BIGINT COMMENT 'wherehows etl execution id that create this record',
  INDEX (app_id, job_id, dag_version),
  INDEX flow_id_idx (app_id, flow_id),
  INDEX flow_path_idx (app_id, flow_path(255)),
  INDEX ref_flow_path_idx (app_id, ref_flow_path(255)),
  INDEX job_path_idx (app_id, job_path(255)),
  INDEX job_type_idx (job_type)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler job table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE job_source_id_map (
  app_id           SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  job_id           INT UNSIGNED      NOT NULL AUTO_INCREMENT
  COMMENT 'job id generated',
  source_id_string VARCHAR(1024) COMMENT 'job full path string',
  source_id_uuid   VARCHAR(255) COMMENT 'source uuid id of the flow',
  source_id_uri    VARCHAR(255) COMMENT 'source uri id of the flow',
  PRIMARY KEY (app_id, job_id),
  INDEX job_path_idx (app_id, source_id_string(255))
)
  ENGINE = MyISAM
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow id mapping table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_dag (
  app_id         SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id        INT UNSIGNED NOT NULL
  COMMENT 'flow id',
  source_version VARCHAR(255) COMMENT 'last source version of the flow under this dag version',
  dag_version    INT COMMENT 'derived dag version of the flow',
  dag_md5        VARCHAR(255) COMMENT 'md5 checksum for this dag version',
  is_current     CHAR(1) COMMENT 'if this source version of the flow is current',
  wh_etl_exec_id BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, flow_id, source_version),
  INDEX flow_dag_md5_idx (app_id, flow_id, dag_md5),
  INDEX flow_id_idx (app_id, flow_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Flow dag reference table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow_dag (
  app_id         SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id        INT UNSIGNED NOT NULL
  COMMENT 'flow id',
  source_version VARCHAR(255) COMMENT 'last source version of the flow under this dag version',
  dag_version    INT COMMENT 'derived dag version of the flow',
  dag_md5        VARCHAR(255) COMMENT 'md5 checksum for this dag version',
  wh_etl_exec_id BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, flow_id, source_version),
  INDEX flow_dag_md5_idx (app_id, flow_id, dag_md5),
  INDEX flow_id_idx (app_id, flow_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Flow dag reference table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow_dag_edge (
  app_id          SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id         INT UNSIGNED COMMENT 'flow id',
  flow_path       VARCHAR(1024) COMMENT 'flow path from top level',
  source_version  VARCHAR(255) COMMENT 'last source version of the flow under this dag version',
  source_job_id   INT UNSIGNED COMMENT 'job id either inherit from source or generated',
  source_job_path VARCHAR(1024) COMMENT 'source job path from top level',
  target_job_id   INT UNSIGNED COMMENT 'job id either inherit from source or generated',
  target_job_path VARCHAR(1024) COMMENT 'target job path from top level',
  wh_etl_exec_id  BIGINT COMMENT 'wherehows etl execution id that create this record',
  INDEX flow_version_idx (app_id, flow_id, source_version),
  INDEX flow_id_idx (app_id, flow_id),
  INDEX flow_path_idx (app_id, flow_path(255)),
  INDEX source_job_path_idx (app_id, source_job_path(255)),
  INDEX target_job_path_idx (app_id, target_job_path(255))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Flow dag table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_execution (
  app_id           SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_exec_id     BIGINT UNSIGNED   NOT NULL
  COMMENT 'flow execution id either from the source or generated',
  flow_exec_uuid   VARCHAR(255) COMMENT 'source flow execution uuid',
  flow_id          INT UNSIGNED      NOT NULL
  COMMENT 'flow id',
  flow_name        VARCHAR(255) COMMENT 'name of the flow',
  source_version   VARCHAR(255) COMMENT 'source version of the flow',
  flow_exec_status VARCHAR(31) COMMENT 'status of flow execution',
  attempt_id       SMALLINT COMMENT 'attempt id',
  executed_by      VARCHAR(127) COMMENT 'people who executed the flow',
  start_time       INT UNSIGNED COMMENT 'start time of the flow execution',
  end_time         INT UNSIGNED COMMENT 'end time of the flow execution',
  is_adhoc         CHAR(1) COMMENT 'determine if it is a ad-hoc execution',
  is_backfill      CHAR(1) COMMENT 'determine if it is a back-fill execution',
  created_time     INT UNSIGNED COMMENT 'etl create time',
  modified_time    INT UNSIGNED COMMENT 'etl modified time',
  wh_etl_exec_id   BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, flow_exec_id),
  INDEX flow_id_idx (app_id, flow_id),
  INDEX flow_name_idx (app_id, flow_name)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow execution table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_execution_id_map (
  app_id             SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_exec_id       BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT
  COMMENT 'generated flow execution id',
  source_exec_string VARCHAR(1024) COMMENT 'source flow execution string',
  source_exec_uuid   VARCHAR(255) COMMENT 'source uuid id of the flow execution',
  source_exec_uri    VARCHAR(255) COMMENT 'source uri id of the flow execution',
  PRIMARY KEY (app_id, flow_exec_id),
  INDEX flow_exec_uuid_idx (app_id, source_exec_uuid)
)
  ENGINE = MyISAM
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow execution id mapping table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow_execution (
  app_id           SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_exec_id     BIGINT UNSIGNED COMMENT 'flow execution id',
  flow_exec_uuid   VARCHAR(255) COMMENT 'source flow execution uuid',
  flow_id          INT UNSIGNED COMMENT 'flow id',
  flow_name        VARCHAR(255) COMMENT 'name of the flow',
  flow_path        VARCHAR(1024) COMMENT 'flow path from top level',
  source_version   VARCHAR(255) COMMENT 'source version of the flow',
  flow_exec_status VARCHAR(31) COMMENT 'status of flow execution',
  attempt_id       SMALLINT COMMENT 'attempt id',
  executed_by      VARCHAR(127) COMMENT 'people who executed the flow',
  start_time       INT UNSIGNED COMMENT 'start time of the flow execution',
  end_time         INT UNSIGNED COMMENT 'end time of the flow execution',
  is_adhoc         CHAR(1) COMMENT 'determine if it is a ad-hoc execution',
  is_backfill      CHAR(1) COMMENT 'determine if it is a back-fill execution',
  wh_etl_exec_id   BIGINT COMMENT 'wherehows etl execution id that create this record',
  INDEX flow_exec_idx (app_id, flow_exec_id),
  INDEX flow_id_idx (app_id, flow_id),
  INDEX flow_path_idx (app_id, flow_path(255))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow execution table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE job_execution (
  app_id          SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_exec_id    BIGINT UNSIGNED COMMENT 'flow execution id',
  job_exec_id     BIGINT UNSIGNED   NOT NULL
  COMMENT 'job execution id either inherit or generated',
  job_exec_uuid   VARCHAR(255) COMMENT 'job execution uuid',
  flow_id         INT UNSIGNED      NOT NULL
  COMMENT 'flow id',
  source_version  VARCHAR(255) COMMENT 'source version of the flow',
  job_id          INT UNSIGNED      NOT NULL
  COMMENT 'job id',
  job_name        VARCHAR(255) COMMENT 'job name',
  job_exec_status VARCHAR(31) COMMENT 'status of flow execution',
  attempt_id      SMALLINT COMMENT 'attempt id',
  start_time      INT UNSIGNED COMMENT 'start time of the execution',
  end_time        INT UNSIGNED COMMENT 'end time of the execution',
  is_adhoc        CHAR(1) COMMENT 'determine if it is a ad-hoc execution',
  is_backfill     CHAR(1) COMMENT 'determine if it is a back-fill execution',
  created_time    INT UNSIGNED COMMENT 'etl create time',
  modified_time   INT UNSIGNED COMMENT 'etl modified time',
  wh_etl_exec_id  BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, job_exec_id),
  INDEX flow_exec_id_idx (app_id, flow_exec_id),
  INDEX job_id_idx (app_id, job_id),
  INDEX flow_id_idx (app_id, flow_id),
  INDEX job_name_idx (app_id, flow_id, job_name)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler job execution table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE job_execution_id_map (
  app_id             SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the job',
  job_exec_id        BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT
  COMMENT 'generated job execution id',
  source_exec_string VARCHAR(1024) COMMENT 'source job execution string',
  source_exec_uuid   VARCHAR(255) COMMENT 'source uuid id of the job execution',
  source_exec_uri    VARCHAR(255) COMMENT 'source uri id of the job execution',
  PRIMARY KEY (app_id, job_exec_id),
  INDEX job_exec_uuid_idx (app_id, source_exec_uuid)
)
  ENGINE = MyISAM
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler job execution id mapping table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_job_execution (
  app_id          SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id         INT UNSIGNED COMMENT 'flow id',
  flow_path       VARCHAR(1024) COMMENT 'flow path from top level',
  source_version  VARCHAR(255) COMMENT 'source version of the flow',
  flow_exec_id    BIGINT UNSIGNED COMMENT 'flow execution id',
  flow_exec_uuid  VARCHAR(255) COMMENT 'flow execution uuid',
  job_id          INT UNSIGNED COMMENT 'job id',
  job_name        VARCHAR(255) COMMENT 'job name',
  job_path        VARCHAR(1024) COMMENT 'job path from top level',
  job_exec_id     BIGINT UNSIGNED COMMENT 'job execution id either inherit or generated',
  job_exec_uuid   VARCHAR(255) COMMENT 'job execution uuid',
  job_exec_status VARCHAR(31) COMMENT 'status of flow execution',
  attempt_id      SMALLINT COMMENT 'attempt id',
  start_time      INT UNSIGNED COMMENT 'start time of the execution',
  end_time        INT UNSIGNED COMMENT 'end time of the execution',
  is_adhoc        CHAR(1) COMMENT 'determine if it is a ad-hoc execution',
  is_backfill     CHAR(1) COMMENT 'determine if it is a back-fill execution',
  wh_etl_exec_id  BIGINT COMMENT 'wherehows etl execution id that create this record',
  INDEX flow_id_idx (app_id, flow_id),
  INDEX flow_path_idx (app_id, flow_path(255)),
  INDEX job_path_idx (app_id, job_path(255)),
  INDEX flow_exec_idx (app_id, flow_exec_id),
  INDEX job_exec_idx (app_id, job_exec_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler job execution table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_schedule (
  app_id               SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id              INT UNSIGNED      NOT NULL
  COMMENT 'flow id',
  unit                 VARCHAR(31) COMMENT 'unit of time',
  frequency            INT COMMENT 'frequency of the unit',
  cron_expression      VARCHAR(127) COMMENT 'cron expression',
  is_active            CHAR(1) COMMENT 'determine if it is an active schedule',
  included_instances   VARCHAR(127) COMMENT 'included instance',
  excluded_instances   VARCHAR(127) COMMENT 'excluded instance',
  effective_start_time INT UNSIGNED COMMENT 'effective start time of the flow execution',
  effective_end_time   INT UNSIGNED COMMENT 'effective end time of the flow execution',
  created_time         INT UNSIGNED COMMENT 'etl create time',
  modified_time        INT UNSIGNED COMMENT 'etl modified time',
  ref_id               VARCHAR(255) COMMENT 'reference id of this schedule',
  wh_etl_exec_id       BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, flow_id, ref_id),
  INDEX (app_id, flow_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow schedule table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow_schedule (
  app_id               SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id              INT UNSIGNED COMMENT 'flow id',
  flow_path            VARCHAR(1024) COMMENT 'flow path from top level',
  unit                 VARCHAR(31) COMMENT 'unit of time',
  frequency            INT COMMENT 'frequency of the unit',
  cron_expression      VARCHAR(127) COMMENT 'cron expression',
  included_instances   VARCHAR(127) COMMENT 'included instance',
  excluded_instances   VARCHAR(127) COMMENT 'excluded instance',
  effective_start_time INT UNSIGNED COMMENT 'effective start time of the flow execution',
  effective_end_time   INT UNSIGNED COMMENT 'effective end time of the flow execution',
  ref_id               VARCHAR(255) COMMENT 'reference id of this schedule',
  wh_etl_exec_id       BIGINT COMMENT 'wherehows etl execution id that create this record',
  INDEX (app_id, flow_id),
  INDEX (app_id, flow_path(255))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler flow schedule table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE flow_owner_permission (
  app_id         SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id        INT UNSIGNED      NOT NULL
  COMMENT 'flow id',
  owner_id       VARCHAR(63) COMMENT 'identifier of the owner',
  permissions    VARCHAR(255) COMMENT 'permissions of the owner',
  owner_type     VARCHAR(31) COMMENT 'whether is a group owner or not',
  created_time   INT UNSIGNED COMMENT 'etl create time',
  modified_time  INT UNSIGNED COMMENT 'etl modified time',
  wh_etl_exec_id BIGINT COMMENT 'wherehows etl execution id that create this record',
  PRIMARY KEY (app_id, flow_id, owner_id),
  INDEX flow_index (app_id, flow_id),
  INDEX owner_index (app_id, owner_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler owner table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE stg_flow_owner_permission (
  app_id         SMALLINT UNSIGNED NOT NULL
  COMMENT 'application id of the flow',
  flow_id        INT UNSIGNED COMMENT 'flow id',
  flow_path      VARCHAR(1024) COMMENT 'flow path from top level',
  owner_id       VARCHAR(63) COMMENT 'identifier of the owner',
  permissions    VARCHAR(255) COMMENT 'permissions of the owner',
  owner_type     VARCHAR(31) COMMENT 'whether is a group owner or not',
  wh_etl_exec_id BIGINT COMMENT 'wherehows etl execution id that create this record',
  INDEX flow_index (app_id, flow_id),
  INDEX owner_index (app_id, owner_id),
  INDEX flow_path_idx (app_id, flow_path(255))
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'Scheduler owner table' PARTITION BY HASH (app_id) PARTITIONS 8;

CREATE TABLE job_execution_ext_reference (
	app_id         	smallint(5) UNSIGNED COMMENT 'application id of the flow'  NOT NULL,
	job_exec_id    	bigint(20) UNSIGNED COMMENT 'job execution id either inherit or generated'  NOT NULL,
	attempt_id     	smallint(6) COMMENT 'job execution attempt id'  DEFAULT '0',
	ext_ref_type	varchar(50) COMMENT 'YARN_JOB_ID, DB_SESSION_ID, PID, INFA_WORKFLOW_RUN_ID, CASSCADE_WORKFLOW_ID'  NOT NULL,
    ext_ref_sort_id smallint(6) COMMENT 'sort id 0..n within each ext_ref_type' NOT NULL DEFAULT '0',
	ext_ref_id      varchar(100) COMMENT 'external reference id' NOT NULL,
	created_time   	int(10) UNSIGNED COMMENT 'etl create time'  NULL,
	wh_etl_exec_id 	bigint(20) COMMENT 'wherehows etl execution id that create this record'  NULL,
	PRIMARY KEY(app_id,job_exec_id,attempt_id,ext_ref_type,ext_ref_sort_id)
)
ENGINE = InnoDB
DEFAULT CHARSET = latin1
COMMENT = 'External reference ids for the job execution'
PARTITION BY HASH(app_id)
   (	PARTITION p0,
	PARTITION p1,
	PARTITION p2,
	PARTITION p3,
	PARTITION p4,
	PARTITION p5,
	PARTITION p6,
	PARTITION p7)
;

CREATE INDEX idx_job_execution_ext_ref__ext_ref_id USING BTREE
	ON job_execution_ext_reference(ext_ref_id);


CREATE TABLE stg_job_execution_ext_reference (
	app_id         	smallint(5) UNSIGNED COMMENT 'application id of the flow'  NOT NULL,
	job_exec_id    	bigint(20) UNSIGNED COMMENT 'job execution id either inherit or generated'  NOT NULL,
	attempt_id     	smallint(6) COMMENT 'job execution attempt id'  DEFAULT '0',
	ext_ref_type	varchar(50) COMMENT 'YARN_JOB_ID, DB_SESSION_ID, PID, INFA_WORKFLOW_RUN_ID, CASSCADE_WORKFLOW_ID'  NOT NULL,
    ext_ref_sort_id smallint(6) COMMENT 'sort id 0..n within each ext_ref_type' NOT NULL DEFAULT '0',
	ext_ref_id      varchar(100) COMMENT 'external reference id' NOT NULL,
	created_time   	int(10) UNSIGNED COMMENT 'etl create time'  NULL,
	wh_etl_exec_id 	bigint(20) COMMENT 'wherehows etl execution id that create this record'  NULL,
	PRIMARY KEY(app_id,job_exec_id,attempt_id,ext_ref_type,ext_ref_sort_id)
)
ENGINE = InnoDB
DEFAULT CHARSET = latin1
COMMENT = 'staging table for job_execution_ext_reference'
PARTITION BY HASH(app_id)
   (	PARTITION p0,
	PARTITION p1,
	PARTITION p2,
	PARTITION p3,
	PARTITION p4,
	PARTITION p5,
	PARTITION p6,
	PARTITION p7)
;

CREATE TABLE `cfg_job_type` (
  `job_type_id` SMALLINT(6) UNSIGNED NOT NULL AUTO_INCREMENT,
  `job_type`    VARCHAR(50)          NOT NULL,
  `description` VARCHAR(200)         NULL,
  PRIMARY KEY (`job_type_id`),
  UNIQUE KEY `ak_cfg_job_type__job_type` (`job_type`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 55
  DEFAULT CHARSET = utf8
  COMMENT = 'job types used in mutliple schedulers';

CREATE TABLE `cfg_job_type_reverse_map` (
  `job_type_actual`   VARCHAR(50)
                      CHARACTER SET ascii NOT NULL,
  `job_type_id`       SMALLINT(6) UNSIGNED NOT NULL,
  `description`       VARCHAR(200)         NULL,
  `job_type_standard` VARCHAR(50)          NOT NULL,
  PRIMARY KEY (`job_type_actual`),
  UNIQUE KEY `cfg_job_type_reverse_map_uk` (`job_type_actual`),
  KEY `cfg_job_type_reverse_map_job_type_id_fk` (`job_type_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'The reverse map of the actual job type to standard job type';
