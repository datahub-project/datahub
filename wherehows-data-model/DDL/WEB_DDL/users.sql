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

-- create statement for users related tables :
-- users, user_settings, watch

CREATE TABLE users (
  id                       INT(11) AUTO_INCREMENT      NOT NULL,
  name                     VARCHAR(100)                NOT NULL,
  email                    VARCHAR(200)                NOT NULL,
  username                 VARCHAR(20)                 NOT NULL,
  department_number        INT(11)                     NULL,
  password_digest          VARCHAR(256)                NULL,
  password_digest_type     ENUM('SHA1', 'SHA2', 'MD5') NULL DEFAULT 'SHA1',
  ext_directory_ref_app_id SMALLINT UNSIGNED,
  authentication_type      VARCHAR(20),
  PRIMARY KEY (id)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 0
  DEFAULT CHARSET = utf8;

CREATE INDEX idx_users__username USING BTREE ON users(username);

CREATE TABLE user_settings (
  user_id             INT(11)                           NOT NULL,
  detail_default_view VARCHAR(20)                       NULL,
  default_watch       ENUM('monthly', 'weekly', 'daily', 'hourly') NULL DEFAULT 'weekly',
  PRIMARY KEY (user_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE watch (
  id                BIGINT(20) AUTO_INCREMENT                                 NOT NULL,
  user_id           INT(11)                                                   NOT NULL,
  item_id           INT(11)                                                   NULL,
  urn               VARCHAR(200)                                              NULL,
  item_type         ENUM('dataset', 'dataset_field', 'metric', 'flow', 'urn') NOT NULL DEFAULT 'dataset',
  notification_type ENUM('monthly', 'weekly', 'hourly', 'daily')              NULL     DEFAULT 'weekly',
  created           TIMESTAMP                                                 NULL     DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 0
  DEFAULT CHARSET = utf8;

CREATE TABLE favorites (
  user_id    INT(11)   NOT NULL,
  dataset_id INT(11)   NOT NULL,
  created    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id, dataset_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;

CREATE TABLE user_login_history (
  log_id              INT(11) AUTO_INCREMENT NOT NULL,
  username            VARCHAR(20)            NOT NULL,
  authentication_type VARCHAR(20)            NOT NULL,
  `status`            VARCHAR(20)            NOT NULL,
  message             TEXT                            DEFAULT NULL,
  login_time          TIMESTAMP              NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (log_id)
)
  ENGINE = InnoDB
  DEFAULT CHARSET = utf8;
