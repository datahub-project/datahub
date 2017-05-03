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
  `ref_id_type`     ENUM('APP', 'DB') NOT NULL
  COMMENT 'flag the ref_id is an application or a database',
  `timeout`         INT(11)                    DEFAULT NULL
  COMMENT 'timeout in seconds for this etl job',
  `next_run`        INT(10) UNSIGNED           DEFAULT NULL
  COMMENT 'next run time',
  `comments`        VARCHAR(200)               DEFAULT NULL,
  `cmd_param`       VARCHAR(500)               DEFAULT ''
  COMMENT 'command line parameters for launch the job',
  `is_active`       CHAR(1)                    DEFAULT 'Y'
  COMMENT 'determine if this job is active or not',
  PRIMARY KEY (`wh_etl_job_id`),
  UNIQUE KEY `etl_unique` (`wh_etl_job_name`, `ref_id`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 1
  DEFAULT CHARSET = utf8
  COMMENT = 'WhereHows ETL jobs table';

CREATE TABLE `wh_etl_job_execution` (
  `wh_etl_exec_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT
  COMMENT 'job execution id',
  `wh_etl_job_id`  INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'id of the etl job',
  `status`         VARCHAR(31)                  DEFAULT NULL
  COMMENT 'status of etl job execution',
  `request_time`   INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'request time of the execution',
  `start_time`     INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'start time of the execution',
  `end_time`       INT(10) UNSIGNED             DEFAULT NULL
  COMMENT 'end time of the execution',
  `message`        VARCHAR(1024)                DEFAULT NULL
  COMMENT 'debug information message',
  `host_name`      VARCHAR(200)                 DEFAULT NULL
  COMMENT 'host machine name of the job execution',
  `process_id`     INT UNSIGNED                 DEFAULT NULL
  COMMENT 'job execution process id',
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
  `ref_id_type`     ENUM('APP', 'DB') NOT NULL
  COMMENT 'flag the ref_id is an application or a database',
  `property_name`   VARCHAR(127)      NOT NULL
  COMMENT 'property name',
  `property_value`  TEXT COMMENT 'property value',
  `is_encrypted`    CHAR(1)                    DEFAULT 'N'
  COMMENT 'whether the value is encrypted',
  `comments`        VARCHAR(100)               DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `property_unique` (`wh_etl_job_name`, `ref_id`, `property_name`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 1
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
  `doc_url`                 VARCHAR(512)         DEFAULT NULL,
  `parent_app_id`           INT(11) UNSIGNED     NOT NULL,
  `app_status`              CHAR(1)              NOT NULL,
  `last_modified`           TIMESTAMP            NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `is_logical`              CHAR(1)                       DEFAULT NULL,
  `uri_type`                VARCHAR(25)                   DEFAULT NULL,
  `uri`                     VARCHAR(1000)                 DEFAULT NULL,
  `lifecycle_layer_id`      TINYINT(4) UNSIGNED           DEFAULT NULL,
  `short_connection_string` VARCHAR(50)                   DEFAULT NULL,
  PRIMARY KEY (`app_id`),
  UNIQUE KEY `idx_cfg_application__appcode` (`app_code`) USING HASH
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE cfg_database  (
	db_id                  	smallint(6) UNSIGNED NOT NULL,
	db_code                	varchar(30) COMMENT 'Unique string without space'  NOT NULL,
	primary_dataset_type    varchar(30) COMMENT 'What type of dataset this DB supports' NOT NULL DEFAULT '*',
	description            	varchar(128) NOT NULL,
	is_logical             	char(1) COMMENT 'Is a group, which contains multiple physical DB(s)'  NOT NULL DEFAULT 'N',
	deployment_tier        	varchar(20) COMMENT 'Lifecycle/FabricGroup: local,dev,sit,ei,qa,canary,preprod,pr'  NULL DEFAULT 'prod',
	data_center            	varchar(200) COMMENT 'Code name of its primary data center. Put * for all data cen'  NULL DEFAULT '*',
	associated_dc_num      	tinyint(4) UNSIGNED COMMENT 'Number of associated data centers'  NOT NULL DEFAULT '1',
	cluster                	varchar(200) COMMENT 'Name of Fleet, Group of Servers or a Server'  NULL DEFAULT '*',
	cluster_size           	smallint(6) COMMENT 'Num of servers in the cluster'  NOT NULL DEFAULT '1',
	extra_deployment_tag1  	varchar(50) COMMENT 'Additional tag. Such as container_group:HIGH'  NULL,
	extra_deployment_tag2  	varchar(50) COMMENT 'Additional tag. Such as slice:i0001'  NULL,
	extra_deployment_tag3  	varchar(50) COMMENT 'Additional tag. Such as region:eu-west-1'  NULL,
	replication_role       	varchar(10) COMMENT 'master or slave or broker'  NULL,
	jdbc_url               	varchar(1000) NULL,
	uri                    	varchar(1000) NULL,
	short_connection_string	varchar(50) COMMENT 'Oracle TNS Name, ODBC DSN, TDPID...' NULL,
  last_modified          	timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY(db_id),
  UNIQUE KEY `uix_cfg_database__dbcode` (db_code) USING HASH
)
ENGINE = InnoDB
DEFAULT CHARSET = utf8
COMMENT = 'Abstract different storage instances as databases' ;


CREATE TABLE stg_cfg_object_name_map  (
	object_type             	varchar(100) NOT NULL,
	object_sub_type         	varchar(100) NULL,
	object_name             	varchar(350) NOT NULL,
	object_urn              	varchar(350) NULL,
	object_dataset_id       	int(11) UNSIGNED NULL,
	map_phrase              	varchar(100) NULL,
	is_identical_map        	char(1) NULL DEFAULT 'N',
	mapped_object_type      	varchar(100) NOT NULL,
	mapped_object_sub_type  	varchar(100) NULL,
	mapped_object_name      	varchar(350) NOT NULL,
	mapped_object_urn       	varchar(350) NULL,
	mapped_object_dataset_id	int(11) UNSIGNED NULL,
	description             	varchar(500) NULL,
	last_modified           	timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY(object_name, mapped_object_name),
  KEY idx_stg_cfg_object_name_map__mappedobjectname (mapped_object_name) USING BTREE
)
ENGINE = InnoDB
CHARACTER SET latin1
COLLATE latin1_swedish_ci
COMMENT = 'Map alias (when is_identical_map=Y) and view dependency' ;

CREATE TABLE cfg_object_name_map  (
  obj_name_map_id         int(11) AUTO_INCREMENT NOT NULL,
  object_type             varchar(100) NOT NULL,
  object_sub_type         varchar(100) NULL,
  object_name             varchar(350) NOT NULL COMMENT 'this is the derived/child object',
  map_phrase              varchar(100) NULL,
  object_dataset_id       int(11) UNSIGNED NULL COMMENT 'can be the abstract dataset id for versioned objects',
  is_identical_map        char(1) NOT NULL DEFAULT 'N' COMMENT 'Y/N',
  mapped_object_type      varchar(100) NOT NULL,
  mapped_object_sub_type  varchar(100) NULL,
  mapped_object_name      varchar(350) NOT NULL COMMENT 'this is the original/parent object',
  mapped_object_dataset_id	int(11) UNSIGNED NULL COMMENT 'can be the abstract dataset id for versioned objects',
  description             varchar(500) NULL,
  last_modified           timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY(obj_name_map_id),
  KEY idx_cfg_object_name_map__mappedobjectname (mapped_object_name) USING BTREE,
  CONSTRAINT uix_cfg_object_name_map__objectname_mappedobjectname UNIQUE (object_name, mapped_object_name)
)
ENGINE = InnoDB
CHARACTER SET latin1
AUTO_INCREMENT = 1
COMMENT = 'Map alias (when is_identical_map=Y) and view dependency. Always map from Derived/Child (object) back to its Original/Parent (mapped_object)' ;


CREATE TABLE cfg_deployment_tier  (
  tier_id      	tinyint(4) NOT NULL,
  tier_code    	varchar(25) COMMENT 'local,dev,test,qa,stg,prod' NOT NULL,
  tier_label    varchar(50) COMMENT 'display full name' NULL,
  sort_id       smallint(6) COMMENT '3-digit for group, 3-digit within group' NOT NULL,
  last_modified timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY(tier_id),
  UNIQUE KEY uix_cfg_deployment_tier__tiercode (tier_code)
)
ENGINE = InnoDB
AUTO_INCREMENT = 0
COMMENT = 'http://en.wikipedia.org/wiki/Deployment_environment';


CREATE TABLE cfg_data_center  (
	data_center_id    	smallint(6) NOT NULL DEFAULT '0',
	data_center_code  	varchar(30) NOT NULL,
	data_center_name  	varchar(50) NOT NULL,
	time_zone         	varchar(50) NOT NULL,
	city              	varchar(50) NOT NULL,
	state             	varchar(25) NULL,
	country           	varchar(50) NOT NULL,
	longtitude        	decimal(10,6) NULL,
	latitude          	decimal(10,6) NULL,
	data_center_status	char(1) COMMENT 'A,D,U' NULL,
	last_modified     	timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY(data_center_id),
  UNIQUE KEY uix_cfg_data_center__datacentercode (data_center_code)
)
ENGINE = InnoDB
AUTO_INCREMENT = 0
COMMENT = 'https://en.wikipedia.org/wiki/Data_center' ;


CREATE TABLE cfg_cluster  (
	cluster_id    	        smallint(6) NOT NULL DEFAULT '0',
	cluster_code  	        varchar(80) NOT NULL,
	cluster_short_name      varchar(50) NOT NULL,
	cluster_type       	varchar(50) NOT NULL,
	deployment_tier_code    varchar(25) NOT NULL,
	data_center_code        varchar(30) NULL,
	description             varchar(200) NULL,
	last_modified     	timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	PRIMARY KEY(cluster_id),
  UNIQUE KEY uix_cfg_cluster__clustercode (cluster_code)
)
COMMENT = 'https://en.wikipedia.org/wiki/Computer_cluster' ;


CREATE TABLE IF NOT EXISTS cfg_search_score_boost (
  `id` INT COMMENT 'dataset id',
  `static_boosting_score` INT COMMENT 'static boosting score for elastic search',
  PRIMARY KEY (`id`)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;
