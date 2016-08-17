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


CREATE TABLE dataset_constraint (
  `dataset_id`            INT UNSIGNED NOT NULL,
  `dataset_urn`           VARCHAR(200) NOT NULL,
  `constraint_type`       VARCHAR(20)  NOT NULL,
  `constraint_sub_type`   VARCHAR(20)  NOT NULL,
  `constraint_name`       VARCHAR(50)  NOT NULL,
  `constraint_expression` TEXT CHAR SET utf8  DEFAULT NULL,
  `enabled`               BOOLEAN      NOT NULL,
  `referred_fields`       TEXT                DEFAULT NULL,
  `additional_reference`  TEXT CHAR SET utf8  DEFAULT NULL,
  `modified_time`         BIGINT(14) UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`, `constraint_type`, `constraint_sub_type`, `constraint_name`),
  UNIQUE KEY (`dataset_urn`, `constraint_type`, `constraint_sub_type`, `constraint_name`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_index (
  `dataset_id`     INT UNSIGNED NOT NULL,
  `dataset_urn`    VARCHAR(200) NOT NULL,
  `index_type`     VARCHAR(20)  NOT NULL,
  `index_name`     VARCHAR(50)  NOT NULL,
  `is_unique`      BOOLEAN      NOT NULL,
  `indexed_fields` TEXT                DEFAULT NULL,
  `modified_time`  BIGINT(14) UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`, `index_name`),
  UNIQUE KEY (`dataset_urn`, `index_name`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_schema_info (
  `dataset_id`                   INT UNSIGNED NOT NULL,
  `dataset_urn`                  VARCHAR(200) NOT NULL,
  `is_latest_revision`           BOOLEAN      NOT NULL,
  `create_time`                  LONG         NOT NULL,
  `revision`                     INT UNSIGNED             DEFAULT NULL,
  `version`                      VARCHAR(20)              DEFAULT NULL,
  `name`                         VARCHAR(100)             DEFAULT NULL,
  `description`                  TEXT CHAR SET utf8       DEFAULT NULL,
  `format`                       VARCHAR(20)              DEFAULT NULL,
  `original_schema`              TEXT                     DEFAULT NULL,
  `original_schema_checksum`     VARCHAR(100)             DEFAULT NULL,
  `key_schema_type`              VARCHAR(20)              DEFAULT NULL,
  `key_schema_format`            VARCHAR(100)             DEFAULT NULL,
  `key_schema`                   MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  `is_field_name_case_sensitive` BOOLEAN                  DEFAULT NULL,
  `field_schema`                 MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  `change_data_capture_fields`   TEXT                     DEFAULT NULL,
  `audit_fields`                 TEXT                     DEFAULT NULL,
  `modified_time`                BIGINT(14) UNSIGNED      DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`),
  UNIQUE KEY (`dataset_urn`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_field_schema (
  `dataset_id`            INT UNSIGNED NOT NULL,
  `position`              INT UNSIGNED NOT NULL,
  `parent_field_position` INT UNSIGNED NOT NULL,
  `field_json_path`       VARCHAR(200)       DEFAULT NULL,
  `field_path`            VARCHAR(200) NOT NULL,
  `label`                 VARCHAR(200)       DEFAULT NULL,
  `aliases`               VARCHAR(200)       DEFAULT NULL,
  `type`                  VARCHAR(20)  NOT NULL,
  `logical_type`          VARCHAR(50)        DEFAULT NULL,
  `semantic_type`         VARCHAR(50)        DEFAULT NULL,
  `abstract_type`         VARCHAR(50)        DEFAULT NULL,
  `description`           TEXT CHAR SET utf8 DEFAULT NULL,
  `nullable`              BOOLEAN      NOT NULL,
  `default_value`         VARCHAR(200)       DEFAULT NULL,
  `max_byte_length`       INT UNSIGNED       DEFAULT NULL,
  `max_char_length`       INT UNSIGNED       DEFAULT NULL,
  `char_type`             VARCHAR(20)        DEFAULT NULL,
  `precision`             INT UNSIGNED       DEFAULT NULL,
  `scale`                 INT UNSIGNED       DEFAULT NULL,
  `confidential_flags`    VARCHAR(100)       DEFAULT NULL,
  `is_recursive`          BOOLEAN            DEFAULT NULL,
  PRIMARY KEY (`dataset_id`, `position`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;
