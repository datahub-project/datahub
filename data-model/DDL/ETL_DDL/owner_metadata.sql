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


CREATE TABLE dataset_owner (
  `dataset_id` INT UNSIGNED NOT NULL,
  `dataset_urn` VARCHAR(200) NOT NULL,
  `owner_id` VARCHAR(127) NOT NULL,
  `app_id` SMALLINT NOT NULL COMMENT 'application id of the namespace',
  `namespace` VARCHAR(127) COMMENT 'the namespace of the user',
  `owner_type` VARCHAR(127) COMMENT 'Producer, Consumer, Stakeholder',
  `owner_sub_type` VARCHAR(127) COMMENT 'DWH, UMP, BA, etc',
  `owner_id_type` VARCHAR(127) COMMENT 'user, group, service, or urn',
  `owner_source`  VARCHAR(127) COMMENT 'where the owner info is extracted: JIRA,RB,DB,FS,AUDIT',
  `db_ids` VARCHAR(127) COMMENT 'comma separated database ids',
  `is_group` CHAR(1) COMMENT 'if owner is a group',
  `is_active` CHAR(1) COMMENT 'if owner is active',
  `is_deleted` CHAR(1) COMMENT 'if owner has been removed from the dataset',
  `sort_id` SMALLINT COMMENT '0 = primary owner, order by priority/importance',
  `source_time` INT UNSIGNED COMMENT 'the source time in epoch',
  `created_time` INT UNSIGNED COMMENT 'the create time in epoch',
  `modified_time` INT UNSIGNED COMMENT 'the modified time in epoch',
  `confirmed_by` VARCHAR(127) NULL,
  `confirmed_on` INT UNSIGNED,
  wh_etl_exec_id BIGINT COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (`dataset_id`, `owner_id`, `app_id`),
  UNIQUE KEY (`dataset_urn`, `owner_id`, `app_id`)
);

CREATE TABLE stg_dataset_owner (
  `dataset_id` INT COMMENT 'dataset_id',
  `dataset_urn` VARCHAR(200) NOT NULL,
  `owner_id` VARCHAR(127) NOT NULL,
  `sort_id` SMALLINT COMMENT '0 = primary owner, order by priority/importance',
  `app_id` INT COMMENT 'application id of the namesapce',
  `namespace` VARCHAR(127) COMMENT 'the namespace of the user',
  `owner_type` VARCHAR(127) COMMENT 'Producer, Consumer, Stakeholder',
  `owner_sub_type` VARCHAR(127) COMMENT 'DWH, UMP, BA, etc',
  `owner_id_type` VARCHAR(127) COMMENT 'user, group, service, or urn',
  `owner_source`  VARCHAR(127) COMMENT 'where the owner info is extracted: JIRA,RB,DB,FS,AUDIT',
  `is_group` CHAR(1) COMMENT 'if owner is a group',
  `db_name` VARCHAR(127) COMMENT 'database name',
  `db_id` INT COMMENT 'database id',
  `is_active` CHAR(1) COMMENT 'if owner is active',
  `source_time` INT UNSIGNED COMMENT 'the source event time in epoch',
  `is_parent_urn` CHAR(1) DEFAULT 'N' COMMENT 'if the urn is a directory for datasets',
  KEY (dataset_urn, owner_id, namespace, db_name),
  KEY dataset_index (dataset_urn),
  KEY db_name_index (db_name)
);

CREATE TABLE stg_dataset_owner_unmatched (
  `dataset_urn` VARCHAR(200) NOT NULL,
  `owner_id` VARCHAR(127) NOT NULL,
  `sort_id` SMALLINT COMMENT '0 = primary owner, order by priority/importance',
  `app_id` INT COMMENT 'application id of the namesapce',
  `namespace` VARCHAR(127) COMMENT 'the namespace of the user',
  `owner_type` VARCHAR(127) COMMENT 'Producer, Consumer, Stakeholder',
  `owner_sub_type` VARCHAR(127) COMMENT 'DWH, UMP, BA, etc',
  `owner_id_type` VARCHAR(127) COMMENT 'user, group, role, service, or urn',
  `owner_source`  VARCHAR(127) COMMENT 'where the owner info is extracted: JIRA,RB,DB,FS,AUDIT',
  `is_group` CHAR(1) COMMENT 'if owner is a group',
  `db_name` VARCHAR(127) COMMENT 'database name',
  `db_id` INT COMMENT 'database id',
  `is_active` CHAR(1) COMMENT 'if owner is active',
  `source_time` INT UNSIGNED COMMENT 'the source event time in epoch',
  KEY (dataset_urn, owner_id, namespace, db_name),
  KEY dataset_index (dataset_urn),
  KEY db_name_index (db_name)
);

CREATE TABLE `dir_external_user_info` (
  `app_id` smallint(5) unsigned NOT NULL,
  `user_id` varchar(50) NOT NULL,
  `urn` varchar(200) DEFAULT NULL,
  `full_name` varchar(200) DEFAULT NULL,
  `display_name` varchar(200) DEFAULT NULL,
  `title` varchar(200) DEFAULT NULL,
  `employee_number` int(10) unsigned DEFAULT NULL,
  `manager_urn` varchar(200) DEFAULT NULL,
  `manager_user_id` varchar(50) DEFAULT NULL,
  `manager_employee_number` int(10) unsigned DEFAULT NULL,
  `default_group_name` varchar(100) DEFAULT NULL,
  `email` varchar(200) DEFAULT NULL,
  `department_id` int(10) unsigned DEFAULT '0',
  `department_name` varchar(200) DEFAULT NULL,
  `start_date` date DEFAULT NULL,
  `mobile_phone` varchar(50) DEFAULT NULL,
  `is_active` char(1) DEFAULT 'Y',
  `org_hierarchy` varchar(500) DEFAULT NULL,
  `org_hierarchy_depth` tinyint(3) unsigned DEFAULT NULL,
  `created_time` int(10) unsigned DEFAULT NULL COMMENT 'the create time in epoch',
  `modified_time` int(10) unsigned DEFAULT NULL COMMENT 'the modified time in epoch',
  `wh_etl_exec_id` bigint(20) DEFAULT NULL COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (`user_id`,`app_id`),
  KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `dir_external_group_user_map` (
  `app_id` smallint(5) unsigned NOT NULL,
  `group_id` varchar(50) NOT NULL,
  `sort_id` smallint(6) NOT NULL,
  `user_app_id` smallint(5) unsigned NOT NULL,
  `user_id` varchar(50) NOT NULL,
  `created_time` int(10) unsigned DEFAULT NULL COMMENT 'the create time in epoch',
  `modified_time` int(10) unsigned DEFAULT NULL COMMENT 'the modified time in epoch',
  `wh_etl_exec_id` bigint(20) DEFAULT NULL COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (`app_id`,`group_id`,`user_app_id`,`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
