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

-- metrics table
CREATE TABLE bus_dashboard_metric  (
  `metric_id`                	SMALLINT(6) UNSIGNED AUTO_INCREMENT NOT NULL,
  `metric_name`              	VARCHAR(200) NOT NULL,
  `metric_description`       	VARCHAR(500) NULL,
  `dashboard_name`           	VARCHAR(100) COMMENT 'Hierarchy Level 1'  NULL,
  `metric_group`             	VARCHAR(100) COMMENT 'Hierarchy Level 2'  NULL,
  `metric_category`          	VARCHAR(100) COMMENT 'Hierarchy Level 3'  NULL,
  `metric_source_type`       	VARCHAR(50) COMMENT 'Table, Cube, File, Web Service'  NULL,
  `metric_source`            	VARCHAR(250) COMMENT 'Table Name, Cube Name, URL'  NULL,
  `metric_ref_id_type`       	VARCHAR(50) COMMENT 'DWH, PLATO, XLNT, MERLIN, CRYSTALBALL, BIZOPS'  NULL,
  `metric_ref_id`            	VARCHAR(100) NULL,
  `metric_additive_type`     	VARCHAR(100) NULL,
  `metric_grain`             	VARCHAR(100) NULL,
  `metric_display_factor`    	DECIMAL(9,2) NULL,
  `metric_display_factor_sym`	VARCHAR(100) NULL,
  `product_page_key_group_sk`	INT(11) NULL,
  `metric_formula`           	TEXT NULL,
  `owner_id`                 	INT(11) NULL,
  `wiki_url_id`              	INT(11) NULL,
  `scm_url_id`               	INT(11) NULL,
  PRIMARY KEY(metric_id),
  KEY `idx_bus_dashboard_metric__ref_id` (`metric_ref_id`) USING BTREE,
  FULLTEXT KEY `fti_bus_dashboard_metric_all` (`metric_name`, `metric_description`, `metric_category`, `metric_group`, `dashboard_name`)
)
  ENGINE = MyISAM
  AUTO_INCREMENT = 0

