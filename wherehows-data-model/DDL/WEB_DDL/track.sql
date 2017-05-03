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

-- tracking user access
CREATE TABLE track_object_access_log (
  `access_unixtime` BIGINT(20) UNSIGNED                                                                                                  NOT NULL,
  `login_id`        INT(10) UNSIGNED                                                                                                     NOT NULL,
  `object_type`     ENUM('dataset', 'metric', 'glossary', 'flow', 'lineage:data', 'lineage:flow', 'lineage:metric', 'lineage:metricJob') NOT NULL DEFAULT 'dataset',
  `object_id`       BIGINT(20)                                                                                                           NULL,
  `object_name`     VARCHAR(500)                                                                                                         NULL,
  `parameters`      VARCHAR(500)                                                                                                         NULL,
  PRIMARY KEY (access_unixtime, login_id, object_type)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 0
