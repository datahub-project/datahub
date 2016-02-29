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
CREATE TABLE dict_business_metric  (
  `metric_id`                	SMALLINT(6) UNSIGNED AUTO_INCREMENT NOT NULL,
  `metric_name`              	VARCHAR(200) NOT NULL,
  `metric_description`       	VARCHAR(500) NULL,
  `dashboard_name`           	VARCHAR(100) COMMENT 'Hierarchy Level 1'  NULL,
  `metric_group`             	VARCHAR(100) COMMENT 'Hierarchy Level 2'  NULL,
  `metric_category`          	VARCHAR(100) COMMENT 'Hierarchy Level 3'  NULL,
  `metric_sub_category`         VARCHAR(100) COMMENT 'Additional Classification, such as Product, Line of Business' NULL,
  `metric_level`		VARCHAR(50) COMMENT 'CORE, DEPARTMENT, TEAM, OPERATION, STRATEGIC, TIER1, TIER2' NULL, 
  `metric_source_type`       	VARCHAR(50) COMMENT 'Table, Cube, File, Web Service'  NULL,
  `metric_source`            	VARCHAR(300) CHAR SET latin1 COMMENT 'Table Name, Cube Name, URL'  NULL,
  `metric_source_dataset_id`	INT(11) COMMENT 'If metric_source can be matched in dict_dataset' NULL,
  `metric_ref_id_type`       	VARCHAR(50) CHAR SET latin1 COMMENT 'DWH, ABTEST, FINANCE, SEGMENT, SALESAPP' NULL,
  `metric_ref_id`            	VARCHAR(100) CHAR SET latin1 COMMENT 'ID in the reference system' NULL,
  `metric_type`			VARCHAR(100) COMMENT 'NUMBER, BOOLEAN, LIST' NULL,
  `metric_additive_type`     	VARCHAR(100) COMMENT 'FULL, SEMI, NONE' NULL,
  `metric_grain`             	VARCHAR(100) COMMENT 'DAILY, WEEKLY, UNIQUE, ROLLING 7D, ROLLING 30D' NULL,
  `metric_display_factor`    	DECIMAL(10,4) COMMENT '0.01, 1000, 1000000, 1000000000' NULL,
  `metric_display_factor_sym`	VARCHAR(20) COMMENT '%, (K), (M), (B), (GB), (TB), (PB)' NULL,
  `metric_good_direction`	VARCHAR(20) COMMENT 'UP, DOWN, ZERO, FLAT' NULL,
  `metric_formula`           	TEXT COMMENT 'Expression, Code Snippet or Calculation Logic' NULL,
  `dimensions`			VARCHAR(500) CHAR SET latin1 NULL,
  `owners`                 	VARCHAR(300) NULL,
  `tags`			VARCHAR(300) NULL,
  `urn`                         VARCHAR(300) CHAR SET latin1 NOT NULL,
  `metric_url`			VARCHAR(300) CHAR SET latin1 NULL,
  `wiki_url`              	VARCHAR(300) CHAR SET latin1 NULL,
  `scm_url`               	VARCHAR(300) CHAR SET latin1 NULL,
  PRIMARY KEY(metric_id),
  KEY `idx_dict_business_metric__urn` (`urn`) USING BTREE,
  KEY `idx_dict_business_metric__ref_id` (`metric_ref_id`) USING BTREE,
  FULLTEXT KEY `fti_dict_business_metric_all` (`metric_name`, `metric_description`, `metric_category`, `metric_group`, `dashboard_name`)
)
  ENGINE = InnoDB
  AUTO_INCREMENT = 0
;
