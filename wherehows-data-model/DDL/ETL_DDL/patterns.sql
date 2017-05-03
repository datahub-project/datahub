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


-- file name pattern to abstract from file level to directory level
CREATE TABLE filename_pattern
(
  filename_pattern_id INT(11) NOT NULL AUTO_INCREMENT,
  regex               VARCHAR(100),
  PRIMARY KEY (filename_pattern_id)
);

-- partitions pattern to abstract from partition level to dataset level
CREATE TABLE `dataset_partition_layout_pattern` (
  `layout_id`               INT(11) NOT NULL AUTO_INCREMENT,
  `regex`                   VARCHAR(50)      DEFAULT NULL,
  `mask`                    VARCHAR(50)      DEFAULT NULL,
  `leading_path_index`      SMALLINT(6)      DEFAULT NULL,
  `partition_index`         SMALLINT(6)      DEFAULT NULL,
  `second_partition_index`  SMALLINT(6)      DEFAULT NULL,
  `sort_id`                 INT(11)          DEFAULT NULL,
  `comments`                VARCHAR(200)     DEFAULT NULL,
  `partition_pattern_group` VARCHAR(50)      DEFAULT NULL,
  PRIMARY KEY (`layout_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- log lineage pattern to extract lineage from logs
CREATE TABLE `log_lineage_pattern` (
  `pattern_id`          INT(11)      NOT NULL AUTO_INCREMENT,
  `pattern_type`        VARCHAR(20)              DEFAULT NULL
  COMMENT 'type of job that have this log pattern',
  `regex`               VARCHAR(200) NOT NULL,
  `database_type`       VARCHAR(20)              DEFAULT NULL
  COMMENT 'database type input by user, e.g. hdfs, voldermont...',
  `database_name_index` INT(11)                  DEFAULT NULL,
  `dataset_index`       INT(11)      NOT NULL
  COMMENT 'the group id of dataset part in the regex',
  `operation_type`      VARCHAR(20)              DEFAULT NULL
  COMMENT 'read/write, input by user',
  `record_count_index`  INT(20)                  DEFAULT NULL
  COMMENT 'all operations count',
  `record_byte_index`   INT(20)                  DEFAULT NULL,
  `insert_count_index`  INT(20)                  DEFAULT NULL,
  `insert_byte_index`   INT(20)                  DEFAULT NULL,
  `delete_count_index`  INT(20)                  DEFAULT NULL,
  `delete_byte_index`   INT(20)                  DEFAULT NULL,
  `update_count_index`  INT(20)                  DEFAULT NULL,
  `update_byte_index`   INT(20)                  DEFAULT NULL,
  `comments`            VARCHAR(200)             DEFAULT NULL,
  `source_target_type`  ENUM('source', 'target') DEFAULT NULL,
  PRIMARY KEY (`pattern_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

-- patterns used to discover the hadoop id inside log
CREATE TABLE `log_reference_job_id_pattern` (
  `pattern_id`             INT(11)      NOT NULL AUTO_INCREMENT,
  `pattern_type`           VARCHAR(20)  DEFAULT NULL
  COMMENT 'type of job that have this log pattern',
  `regex`                  VARCHAR(200) NOT NULL,
  `reference_job_id_index` INT(11)      NOT NULL,
  `is_active`              TINYINT(1)   DEFAULT '0',
  `comments`               VARCHAR(200) DEFAULT NULL,
  PRIMARY KEY (`pattern_id`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;


