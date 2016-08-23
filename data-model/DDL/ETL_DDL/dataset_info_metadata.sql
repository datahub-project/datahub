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


CREATE TABLE dataset_deployment (
  `dataset_id`      INT UNSIGNED NOT NULL,
  `dataset_urn`     VARCHAR(200) NOT NULL,
  `deployment_tier` VARCHAR(20)  NOT NULL,
  `datacenter`      VARCHAR(20)  NOT NULL,
  `region`          VARCHAR(50)        DEFAULT NULL,
  `zone`            VARCHAR(50)        DEFAULT NULL,
  `cluster`         VARCHAR(100)       DEFAULT NULL,
  `enabled`         BOOLEAN      NOT NULL,
  `additional_info` TEXT CHAR SET utf8 DEFAULT NULL,
  `modified_time`   INT UNSIGNED       DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`, `deployment_tier`, `datacenter`),
  UNIQUE KEY (`dataset_urn`, `deployment_tier`, `datacenter`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_capacity (
  `dataset_id`    INT UNSIGNED NOT NULL,
  `dataset_urn`   VARCHAR(200) NOT NULL,
  `capacity_name` VARCHAR(100) NOT NULL,
  `capacity_type` VARCHAR(50)  DEFAULT NULL,
  `capacity_unit` VARCHAR(20)  DEFAULT NULL,
  `capacity_low`  BIGINT       DEFAULT NULL,
  `capacity_high` BIGINT       DEFAULT NULL,
  `modified_time` INT UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`, `capacity_name`),
  UNIQUE KEY (`dataset_urn`, `capacity_name`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_tag (
  `dataset_id`    INT UNSIGNED NOT NULL,
  `dataset_urn`   VARCHAR(200) NOT NULL,
  `tag`           VARCHAR(100) NOT NULL,
  `modified_time` INT UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`, `tag`),
  UNIQUE KEY (`dataset_urn`, `tag`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_case_sensitivity (
  `dataset_id`    INT UNSIGNED NOT NULL,
  `dataset_urn`   VARCHAR(200) NOT NULL,
  `dataset_name`  BOOLEAN      NOT NULL,
  `field_name`    BOOLEAN      NOT NULL,
  `data_content`  BOOLEAN      NOT NULL,
  `modified_time` INT UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`),
  UNIQUE KEY (`dataset_urn`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_reference (
  `dataset_id`       INT UNSIGNED       NOT NULL,
  `dataset_urn`      VARCHAR(200)       NOT NULL,
  `reference_type`   VARCHAR(20)        NOT NULL,
  `reference_format` VARCHAR(50)        NOT NULL,
  `reference_list`   TEXT CHAR SET utf8 NOT NULL,
  `modified_time`    INT UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`, `reference_type`, `reference_format`),
  UNIQUE KEY (`dataset_urn`, `reference_type`, `reference_format`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_partition (
  `dataset_id`                INT UNSIGNED NOT NULL,
  `dataset_urn`               VARCHAR(200) NOT NULL,
  `total_partition_level`     SMALLINT UNSIGNED  DEFAULT NULL,
  `partition_spec_text`       TEXT CHAR SET utf8 DEFAULT NULL,
  `has_time_partition`        BOOLEAN            DEFAULT NULL,
  `has_hash_partition`        BOOLEAN            DEFAULT NULL,
  `partition_keys`            TEXT CHAR SET utf8 DEFAULT NULL,
  `time_partition_expression` VARCHAR(100)       DEFAULT NULL,
  `modified_time`             INT UNSIGNED       DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`),
  UNIQUE KEY (`dataset_urn`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_security (
  `dataset_id`          INT UNSIGNED NOT NULL,
  `dataset_urn`         VARCHAR(200) NOT NULL,
  `classification`      VARCHAR(200) DEFAULT NULL,
  `record_owner_type`   VARCHAR(20)  DEFAULT NULL,
  `record_owner`        VARCHAR(200) DEFAULT NULL,
  `compliance_type`     VARCHAR(30)  DEFAULT NULL,
  `retention_policy`    VARCHAR(200) DEFAULT NULL,
  `geographic_affinity` VARCHAR(200) DEFAULT NULL,
  `modified_time`       INT UNSIGNED DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`),
  UNIQUE KEY (`dataset_urn`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;

CREATE TABLE dataset_constraint (
  `dataset_id`            INT UNSIGNED NOT NULL,
  `dataset_urn`           VARCHAR(200) NOT NULL,
  `constraint_type`       VARCHAR(20)  NOT NULL,
  `constraint_sub_type`   VARCHAR(20)  NOT NULL,
  `constraint_name`       VARCHAR(50)  NOT NULL,
  `constraint_expression` TEXT CHAR SET utf8 DEFAULT NULL,
  `enabled`               BOOLEAN      NOT NULL,
  `referred_fields`       TEXT               DEFAULT NULL,
  `additional_reference`  TEXT CHAR SET utf8 DEFAULT NULL,
  `modified_time`         INT UNSIGNED       DEFAULT NULL
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
  `indexed_fields` TEXT         DEFAULT NULL,
  `modified_time`  INT UNSIGNED DEFAULT NULL
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
  `modified_time`                INT UNSIGNED             DEFAULT NULL
  COMMENT 'the modified time in epoch',
  PRIMARY KEY (`dataset_id`),
  UNIQUE KEY (`dataset_urn`)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = latin1;
