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

