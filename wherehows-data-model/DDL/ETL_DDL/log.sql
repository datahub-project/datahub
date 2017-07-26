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


CREATE TABLE `log_jira__hdfs_directory_to_owner_map` (
  `hdfs_name` varchar(50) NOT NULL,
  `directory_path` varchar(500) NOT NULL,
  `ref_directory_path` varchar(50) DEFAULT NULL,
  `hdfs_owner_id` varchar(50) DEFAULT NULL,
  `total_size_mb` bigint(20) DEFAULT NULL,
  `num_of_files` bigint(20) DEFAULT NULL,
  `earliest_file_creation_date` date DEFAULT NULL,
  `lastest_file_creation_date` date DEFAULT NULL,
  `jira_key` varchar(50) DEFAULT NULL,
  `jira_status` varchar(50) DEFAULT NULL,
  `jira_last_updated_time` bigint(20) DEFAULT NULL,
  `jira_created_time` bigint(20) DEFAULT NULL,
  `prev_jira_status` varchar(50) DEFAULT NULL,
  `prev_jira_status_changed_time` bigint(20) DEFAULT NULL,
  `current_assignee_id` varchar(50) DEFAULT NULL,
  `original_assignee_id` varchar(50) DEFAULT NULL,
  `watcher_id` varchar(50) DEFAULT NULL,
  `ref_manager_ids` varchar(1000) DEFAULT NULL,
  `ref_user_ids` varchar(2000) DEFAULT NULL,
  `modified` timestamp NULL DEFAULT NULL,
  `jira_component` varchar(100) DEFAULT NULL,
  `jira_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`directory_path`,`hdfs_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
