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

-- created statements for lineage related tables
CREATE TABLE IF NOT EXISTS `stg_job_execution_data_lineage` (
  `app_id`                 SMALLINT(5) UNSIGNED                                ,
  `flow_exec_id`           BIGINT(20) UNSIGNED                                 ,
  `job_exec_id`            BIGINT(20) UNSIGNED                                 ,
  `job_exec_uuid`          VARCHAR(100)                                        NULL,
  `job_name`               VARCHAR(255)                                        NULL,
  `job_start_unixtime`     BIGINT(20)                                          NOT NULL,
  `job_finished_unixtime`  BIGINT(20)                                          NOT NULL,

  `db_id`                  SMALLINT(5) UNSIGNED                                NULL,
  `abstracted_object_name` VARCHAR(255)                                        NOT NULL,
  `full_object_name`       VARCHAR(1000)                                       NOT NULL,
  `partition_start`        VARCHAR(50)                                         NULL,
  `partition_end`          VARCHAR(50)                                         NULL,
  `partition_type`         VARCHAR(20)                                         NULL,
  `layout_id`              SMALLINT(5) UNSIGNED                                NULL,
  `storage_type`           VARCHAR(16)                                         NULL,

  `source_target_type`     ENUM('source', 'target', 'lookup', 'temp') NOT NULL,
  `srl_no`                 SMALLINT(5) UNSIGNED                       NOT NULL DEFAULT '1'
  COMMENT 'the sorted number of this record in all records of this job related operation',
  `source_srl_no`          SMALLINT(5) UNSIGNED                                NULL
  COMMENT 'the related record of this record',
  `operation`              VARCHAR(64)                                         NULL,
  `record_count`           BIGINT(20) UNSIGNED                                 NULL,
  `insert_count`           BIGINT(20) UNSIGNED                                 NULL,
  `delete_count`           BIGINT(20) UNSIGNED                                 NULL,
  `update_count`           BIGINT(20) UNSIGNED                                 NULL,
  `flow_path`              VARCHAR(1024)                                       NULL,
  `created_date`           INT UNSIGNED,
  `wh_etl_exec_id`              INT(11)                                        NULL
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE IF NOT EXISTS `job_execution_data_lineage` (
  `app_id`                 SMALLINT(5) UNSIGNED                       NOT NULL,
  `flow_exec_id`           BIGINT(20) UNSIGNED                                 NOT NULL,
  `job_exec_id`            BIGINT(20) UNSIGNED                                 NOT NULL
  COMMENT 'in azkaban this is a smart key combined execution id and sort id of the job',
  `job_exec_uuid`          VARCHAR(100)                                        NULL
  COMMENT 'some scheduler do not have this value, e.g. Azkaban',
  `job_name`               VARCHAR(255)                                        NOT NULL,
  `job_start_unixtime`     BIGINT(20)                                          NOT NULL,
  `job_finished_unixtime`  BIGINT(20)                                          NOT NULL,

  `db_id`                  SMALLINT(5) UNSIGNED                                NULL,
  `abstracted_object_name` VARCHAR(255)                               NOT NULL,
  `full_object_name`       VARCHAR(1000)                                       NULL,
  `partition_start`        VARCHAR(50)                                         NULL,
  `partition_end`          VARCHAR(50)                                         NULL,
  `partition_type`         VARCHAR(20)                                         NULL,
  `layout_id`              SMALLINT(5) UNSIGNED                                NULL
  COMMENT 'layout of the dataset',
  `storage_type`           VARCHAR(16)                                         NULL,

  `source_target_type`     ENUM('source', 'target', 'lookup', 'temp') NOT NULL,
  `srl_no`                 SMALLINT(5) UNSIGNED                       NOT NULL DEFAULT '1'
  COMMENT 'the sorted number of this record in all records of this job related operation',
  `source_srl_no`          SMALLINT(5) UNSIGNED                                NULL
  COMMENT 'the related record of this record',
  `operation`              VARCHAR(64)                                         NULL,
  `record_count`           BIGINT(20) UNSIGNED                                 NULL,
  `insert_count`           BIGINT(20) UNSIGNED                                 NULL,
  `delete_count`           BIGINT(20) UNSIGNED                                 NULL,
  `update_count`           BIGINT(20) UNSIGNED                                 NULL,
  `flow_path`              VARCHAR(1024)                                       NULL,
  `created_date`           INT UNSIGNED,
  `wh_etl_exec_id`              INT(11)                                        NULL,

  PRIMARY KEY (`app_id`, `job_exec_id`, `srl_no`),
  KEY `idx_flow_path` (`app_id`, `flow_path`(300)),
  KEY `idx_job_execution_data_lineage__object_name` (`abstracted_object_name`, `source_target_type`) USING BTREE
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1
  COMMENT = 'Lineage table' PARTITION BY HASH (app_id) PARTITIONS 8;
