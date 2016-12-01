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

CREATE TABLE `source_code_commit_info` (
  `app_id`          SMALLINT(5) UNSIGNED DEFAULT NULL,
  `repository_urn`  VARCHAR(300) CHAR SET latin1 NOT NULL COMMENT 'the git repo urn',
  `commit_id`       VARCHAR(50) CHAR SET latin1  NOT NULL COMMENT 'the sha-1 hash of the commit',
  `file_path`       VARCHAR(600) CHAR SET latin1 NOT NULL COMMENT 'the path to the file',
  `file_name`       VARCHAR(127)                 NOT NULL COMMENT 'the file name',
  `commit_time`     INT UNSIGNED COMMENT 'the commit time',
  `committer_name`  VARCHAR(128)                 NOT NULL COMMENT 'name of the committer',
  `committer_email` VARCHAR(128)         DEFAULT NULL COMMENT 'email of the committer',
  `author_name`     VARCHAR(128)                 NOT NULL COMMENT 'name of the author',
  `author_email`    VARCHAR(128)                 NOT NULL COMMENT 'email of the author',
  `message`         VARCHAR(1024)                NOT NULL COMMENT 'message of the commit',
  `created_time`    INT UNSIGNED COMMENT 'wherehows created time',
  `modified_time`   INT UNSIGNED COMMENT 'latest wherehows modified',
  `wh_etl_exec_id`  BIGINT COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (repository_urn, file_path, commit_id),
  KEY (commit_id),
  KEY (repository_urn, file_name, committer_email)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

CREATE TABLE `stg_source_code_commit_info` (
  `app_id`          SMALLINT(5) UNSIGNED DEFAULT NULL,
  `repository_urn`  VARCHAR(300) CHAR SET latin1 NOT NULL COMMENT 'the git repo urn',
  `commit_id`       VARCHAR(50) CHAR SET latin1  NOT NULL COMMENT 'the sha-1 hash of the commit',
  `file_path`       VARCHAR(600) CHAR SET latin1 NOT NULL COMMENT 'the path to the file',
  `file_name`       VARCHAR(127)                 NOT NULL COMMENT 'the file name',
  `commit_time`     INT UNSIGNED COMMENT 'the commit time',
  `committer_name`  VARCHAR(128)                 NOT NULL COMMENT 'name of the committer',
  `committer_email` VARCHAR(128)         DEFAULT NULL COMMENT 'email of the committer',
  `author_name`     VARCHAR(128)                 NOT NULL COMMENT 'name of the author',
  `author_email`    VARCHAR(128)                 NOT NULL COMMENT 'email of the author',
  `message`         VARCHAR(1024)                NOT NULL COMMENT 'message of the commit',
  `wh_etl_exec_id`  BIGINT COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (repository_urn, file_path, commit_id),
  KEY (commit_id),
  KEY (repository_urn, file_name, committer_email)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


CREATE TABLE `stg_git_project` (
  `app_id`          SMALLINT(5) UNSIGNED NOT NULL,
  `wh_etl_exec_id`  BIGINT COMMENT 'wherehows etl execution id that modified this record',
  `project_name`    VARCHAR(100) NOT NULL,
  `scm_type`        VARCHAR(20) NOT NULL COMMENT 'git, svn or other',
  `owner_type`      VARCHAR(50) DEFAULT NULL,
  `owner_name`      VARCHAR(300) DEFAULT NULL COMMENT 'owner names in comma separated list',
  `create_time`     VARCHAR(50) DEFAULT NULL,
  `num_of_repos`    INT UNSIGNED DEFAULT NULL,
  `repos`           MEDIUMTEXT DEFAULT NULL COMMENT 'repo names in comma separated list',
  `license`         VARCHAR(100) DEFAULT NULL,
  `description`     MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  PRIMARY KEY (`project_name`, `scm_type`, `app_id`)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;

CREATE TABLE `stg_product_repo` (
  `app_id`          SMALLINT(5) UNSIGNED NOT NULL,
  `wh_etl_exec_id`  BIGINT COMMENT 'wherehows etl execution id that modified this record',
  `scm_repo_fullname` VARCHAR(100) NOT NULL,
  `scm_type`        VARCHAR(20) NOT NULL,
  `repo_id`         INT UNSIGNED DEFAULT NULL,
  `project`         VARCHAR(100) DEFAULT NULL,
  `dataset_group`   VARCHAR(200) DEFAULT NULL COMMENT 'dataset group name, database name, etc',
  `owner_type`      VARCHAR(50) DEFAULT NULL,
  `owner_name`      VARCHAR(300) DEFAULT NULL COMMENT 'owner names in comma separated list',
  `multiproduct_name` VARCHAR(100) DEFAULT NULL,
  `product_type`    VARCHAR(100) DEFAULT NULL,
  `product_version` VARCHAR(50) DEFAULT NULL,
  `namespace`       VARCHAR(100) DEFAULT NULL,
  PRIMARY KEY (`scm_repo_fullname`, `scm_type`, `app_id`)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;

CREATE TABLE `stg_repo_owner` (
  `app_id`          SMALLINT(5) UNSIGNED NOT NULL,
  `wh_etl_exec_id`  BIGINT COMMENT 'wherehows etl execution id that modified this record',
  `scm_repo_fullname` VARCHAR(100) NOT NULL,
  `scm_type`        VARCHAR(20) NOT NULL,
  `repo_id`         INT DEFAULT NULL,
  `dataset_group`   VARCHAR(200) DEFAULT NULL COMMENT 'dataset group name, database name, etc',
  `owner_type`      VARCHAR(50) DEFAULT NULL COMMENT 'which acl file this owner is in',
  `owner_name`      VARCHAR(50) DEFAULT NULL COMMENT 'one owner name',
  `sort_id`         INT UNSIGNED DEFAULT NULL,
  `paths`           TEXT CHAR SET utf8 DEFAULT NULL COMMENT 'covered paths by this acl',
  PRIMARY KEY (`scm_repo_fullname`, `scm_type`, `owner_type`, `owner_name`, `app_id`)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;


CREATE TABLE stg_database_scm_map (
  `database_name` VARCHAR(100) COMMENT 'database name',
  `database_type` VARCHAR(50) COMMENT 'database type',
  `app_name` VARCHAR(127) COMMENT 'the name of application',
  `scm_type` VARCHAR(50) COMMENT 'scm type',
  `scm_url` VARCHAR(127) COMMENT 'scm url',
  `committers` VARCHAR(500) COMMENT 'committers',
  `filepath` VARCHAR(200) COMMENT 'filepath',
  `app_id` INT COMMENT 'application id of the namesapce',
  `wh_etl_exec_id`  BIGINT COMMENT 'wherehows etl execution id that modified this record',
  PRIMARY KEY (`database_type`,`database_name`,`scm_type`,`app_name`)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;