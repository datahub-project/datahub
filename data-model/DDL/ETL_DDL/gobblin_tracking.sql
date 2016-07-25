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


-- creation statement for Gobblin tracking event related tables

-- staging table for Gobblin tracking event compaction
CREATE TABLE `stg_gobblin_tracking_compaction` (
  `cluster`         VARCHAR(20) NOT NULL,
  `dataset`         VARCHAR(100) NOT NULL,
  `partition_type`  VARCHAR(20) DEFAULT NULL,
  `partition_name`  VARCHAR(50) DEFAULT NULL,
  `record_count`        BIGINT(20) DEFAULT NULL,
  `late_record_count`   BIGINT(20) DEFAULT NULL,
  `dedupe_status`   VARCHAR(20) DEFAULT NULL,
  `job_context`     VARCHAR(50) DEFAULT NULL,
  `project_name`    VARCHAR(50) DEFAULT NULL,
  `flow_name`       VARCHAR(100) DEFAULT NULL,
  `job_name`        VARCHAR(100) DEFAULT NULL,
  `flow_exec_id`    INT(11) DEFAULT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  PRIMARY KEY (`dataset`,`cluster`,`log_event_time`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- staging table for Gobblin tracking event lumos
CREATE TABLE `stg_gobblin_tracking_lumos` (
  `cluster`     VARCHAR(20) NOT NULL,
  `dataset`     VARCHAR(100) NOT NULL,
  `location`    VARCHAR(200) NOT NULL,
  `partition_type`    VARCHAR(20) DEFAULT NULL,
  `partition_name`    VARCHAR(50) DEFAULT NULL,
  `subpartition_type` VARCHAR(20) DEFAULT NULL,
  `subpartition_name` VARCHAR(50) DEFAULT NULL,
  `max_data_date_epoch3`  BIGINT(20) DEFAULT NULL,
  `max_data_key`          BIGINT(20) DEFAULT NULL,
  `record_count`          BIGINT(20) DEFAULT NULL,
  `source_datacenter`     VARCHAR(10) DEFAULT NULL,
  `source_deployment_env` VARCHAR(10) DEFAULT NULL,
  `source_database` VARCHAR(50) DEFAULT NULL,
  `source_table`    VARCHAR(50) DEFAULT NULL,
  `job_context`     VARCHAR(50) DEFAULT NULL,
  `project_name`    VARCHAR(100) DEFAULT NULL,
  `flow_name`       VARCHAR(100) DEFAULT NULL,
  `job_name`        VARCHAR(100) DEFAULT NULL,
  `flow_exec_id`    INT(11) DEFAULT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  PRIMARY KEY (`dataset`,`cluster`,`log_event_time`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- staging table for Gobblin tracking event distcp_ng
CREATE TABLE `stg_gobblin_tracking_distcp_ng` (
  `cluster`     VARCHAR(20) NOT NULL,
  `dataset`     VARCHAR(100) NOT NULL,
  `partition_type`  VARCHAR(20) DEFAULT NULL,
  `partition_name`  VARCHAR(50) DEFAULT NULL,
  `upsteam_timestamp` BIGINT(20) DEFAULT NULL,
  `origin_timestamp`  BIGINT(20) DEFAULT NULL,
  `source_path`     VARCHAR(200) DEFAULT NULL,
  `target_path`     VARCHAR(200) DEFAULT NULL,
  `job_context`     VARCHAR(50) DEFAULT NULL,
  `project_name`    VARCHAR(100) DEFAULT NULL,
  `flow_name`       VARCHAR(100) DEFAULT NULL,
  `job_name`        VARCHAR(100) DEFAULT NULL,
  `flow_exec_id`    INT(11) DEFAULT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  PRIMARY KEY (`dataset`,`cluster`,`partition_name`,`log_event_time`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- staging table for Metastore Audit Event, include TableAudit / PartitionAudit
CREATE TABLE `stg_metastore_audit` (
  `server`          VARCHAR(20) NOT NULL,
  `instance`        VARCHAR(20) NOT NULL,
  `app_name`        VARCHAR(50) NOT NULL,
  `event_name`      VARCHAR(50) NOT NULL,
  `event_type`      VARCHAR(30) NOT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  `metastore_thrift_uri`  VARCHAR(200) DEFAULT NULL,
  `metastore_version`     VARCHAR(20) DEFAULT NULL,
  `is_successful`         VARCHAR(5) DEFAULT NULL,
  `is_data_deleted`       VARCHAR(5) DEFAULT NULL,
  `db_name`         VARCHAR(100) NOT NULL,
  `table_name`      VARCHAR(100) NOT NULL,
  `time_partition`  VARCHAR(50) NOT NULL,
  `location`        VARCHAR(200) DEFAULT NULL,
  `owner`           VARCHAR(100) DEFAULT NULL,
  `create_time`     BIGINT(20) DEFAULT NULL,
  `last_access_time`  BIGINT(20) DEFAULT NULL,
  `old`             MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  `new`             MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  PRIMARY KEY (`db_name`,`table_name`,`time_partition`,`instance`,`log_event_time`,`event_type`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;
