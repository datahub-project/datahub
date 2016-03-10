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

-- configuration tables
CREATE TABLE `wh_etl_job` (
  `wh_etl_job_id`   INT(10) UNSIGNED  NOT NULL AUTO_INCREMENT
  COMMENT 'id of the etl job',
  `wh_etl_job_name` VARCHAR(127)      NOT NULL
  COMMENT 'etl job name',
  `wh_etl_type`     VARCHAR(127)               DEFAULT NULL
  COMMENT 'type of the etl service',
  `cron_expr`       VARCHAR(127)               DEFAULT NULL
  COMMENT 'frequency in crob expression',
  `ref_id`          SMALLINT(6)       NOT NULL
  COMMENT 'id of application/database where etl job runs',
  `timeout`         INT(11)                    DEFAULT NULL
  COMMENT 'timeout in seconds for this etl job',
  `next_run`        INT(10) UNSIGNED           DEFAULT NULL
  COMMENT 'next run time',
  `comments`        VARCHAR(200)               DEFAULT NULL,
  `ref_id_type`     ENUM('APP', 'DB') NOT NULL
  COMMENT 'flag the ref_id is an application or a database',
  `cmd_param`       VARCHAR(500)               DEFAULT ''
  COMMENT 'command line parameters for launch the job',
  `is_active`       CHAR(1)                    DEFAULT 'Y'
  COMMENT 'determine if this job is active or not',
  PRIMARY KEY (`wh_etl_job_id`),
  UNIQUE KEY `etl_unique` (`wh_etl_job_name`, `ref_id`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 18
  DEFAULT CHARSET = utf8
  COMMENT = 'WhereHows ETL jobs table';

CREATE TABLE `wh_etl_job_execution` (
  `wh_etl_exec_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT
  COMMENT 'job execution id',
  `wh_etl_job_id`  INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'id of the etl job',
  `status`         VARCHAR(31)                  DEFAULT NULL
  COMMENT 'status of etl job execution',
  `request_time`     INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'request time of the execution',
  `start_time`     INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'start time of the execution',
  `end_time`       INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'end time of the execution',
  `message`        VARCHAR(1024)                DEFAULT NULL
  COMMENT 'debug information message',
  PRIMARY KEY (`wh_etl_exec_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'WhereHows ETL execution table';

CREATE TABLE `wh_etl_job_property` (
  `id`              INT(10)           NOT NULL AUTO_INCREMENT,
  `wh_etl_job_name` VARCHAR(127)      NOT NULL
  COMMENT 'etl job name',
  `ref_id`          SMALLINT(6)       NOT NULL
  COMMENT 'id of application/database where etl job runs',
  `property_name`   VARCHAR(127)      NOT NULL
  COMMENT 'property name',
  `property_value`  TEXT COMMENT 'property value',
  `is_encrypted`    CHAR(1)                    DEFAULT 'N'
  COMMENT 'whether the value is encrypted',
  `comments`        VARCHAR(100)               DEFAULT NULL,
  `ref_id_type`     ENUM('APP', 'DB') NOT NULL
  COMMENT 'flag the ref_id is an application or a database',
  PRIMARY KEY (`id`),
  UNIQUE KEY `property_unique` (`wh_etl_job_name`, `ref_id`, `property_name`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 91
  DEFAULT CHARSET = utf8
  COMMENT = 'Etl job configuration table';

CREATE TABLE `wh_property` (
  `property_name`  VARCHAR(127) NOT NULL
  COMMENT 'property name',
  `property_value` TEXT COMMENT 'property value',
  `is_encrypted`   CHAR(1)      DEFAULT 'N'
  COMMENT 'whether the value is encrypted',
  `group_name`     VARCHAR(127) DEFAULT NULL
  COMMENT 'group name for the property',
  PRIMARY KEY (`property_name`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COMMENT = 'WhereHows properties table';

CREATE TABLE `cfg_application` (
  `app_id`                  SMALLINT    UNSIGNED NOT NULL,
  `app_code`                VARCHAR(128)         NOT NULL,
  `description`             VARCHAR(512)         NOT NULL,
  `tech_matrix_id`          SMALLINT(5) UNSIGNED DEFAULT '0',
  `doc_url`                 VARCHAR(512)                  DEFAULT NULL,
  `parent_app_id`           INT(11) UNSIGNED     NOT NULL,
  `app_status`              CHAR(1)              NOT NULL,
  `last_modified`           TIMESTAMP            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `is_logical`              CHAR(1)                       DEFAULT NULL,
  `uri_type`                VARCHAR(25)                   DEFAULT NULL,
  `uri`                     VARCHAR(1000)                 DEFAULT NULL,
  `lifecycle_layer_id`      TINYINT(4) UNSIGNED           DEFAULT NULL,
  `short_connection_string` VARCHAR(50)                   DEFAULT NULL,
  PRIMARY KEY (`app_id`),
  KEY `fk_tech_matrix_id` (`tech_matrix_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE `cfg_database` (
  `db_id`                   SMALLINT    UNSIGNED NOT NULL,
  `db_code`                 VARCHAR(30)          NOT NULL,
  `db_type_id`              SMALLINT(6) UNSIGNED NOT NULL,
  `description`             VARCHAR(128)         NOT NULL,
  `is_logical`              CHAR(1)              NOT NULL DEFAULT 'N',
  `cluster_size`            SMALLINT(6) UNSIGNED NOT NULL DEFAULT '1',
  `associated_data_centers` TINYINT(4) UNSIGNED  NOT NULL DEFAULT '1',
  `replication_role`        VARCHAR(10)                   DEFAULT NULL,
  `jdbc_url`                VARCHAR(1000)                 DEFAULT NULL,
  `uri`                     VARCHAR(1000)                 DEFAULT NULL,
  `short_connection_string` VARCHAR(50)                   DEFAULT NULL,
  `last_modified`           TIMESTAMP            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`db_id`),
  UNIQUE KEY `idx_cfg_database_database_code` (`db_code`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
