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


-- creation statement for Kafka event related tables
-- Gobblin:
--   + GobblinTrackingEvent: compaction
--   + GobblinTrackingEvent_Distcp_Ng: distcp
--   + GobblinTrackingEvent_Lumos: rdbms/nosql
-- Hive Metastore
--   + MetastoreTableAudit
--   + MetastorePartitionAudit
-- Mapping {Kafka topic => stg table} is loaded in
-- backend-service/app/actors/KafkaConsumerMaster.java
-- Avro schemas of the Kafka event are available in
-- data-model/avro

-- staging table for Gobblin tracking event compaction
CREATE TABLE `stg_kafka_gobblin_compaction` (
  `cluster`         VARCHAR(20) NOT NULL,
  `dataset`         VARCHAR(100) NOT NULL,
  `partition_type`  VARCHAR(20) DEFAULT NULL,
  `partition_name`  VARCHAR(50) DEFAULT NULL,
  `record_count`        BIGINT(20) DEFAULT NULL,
  `late_record_count`   BIGINT(20) DEFAULT NULL,
  `dedupe_status`   VARCHAR(20) DEFAULT NULL,
  `job_context`     VARCHAR(50) DEFAULT NULL,
  `project_name`    VARCHAR(50) DEFAULT NULL,
  `flow_name`       VARCHAR(100) DEFAULT NULL,
  `job_name`        VARCHAR(100) DEFAULT NULL,
  `flow_exec_id`    INT(11) DEFAULT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  PRIMARY KEY (`dataset`,`cluster`,`log_event_time`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- staging table for Gobblin tracking event lumos
CREATE TABLE `stg_kafka_gobblin_lumos` (
  `cluster`     VARCHAR(20) NOT NULL,
  `dataset`     VARCHAR(100) NOT NULL,
  `location`    VARCHAR(200) NOT NULL,
  `partition_type`    VARCHAR(20) DEFAULT NULL,
  `partition_name`    VARCHAR(50) DEFAULT NULL,
  `subpartition_type` VARCHAR(20) DEFAULT NULL,
  `subpartition_name` VARCHAR(50) DEFAULT NULL,
  `max_data_date_epoch3`  BIGINT(20) DEFAULT NULL,
  `max_data_key`          BIGINT(20) DEFAULT NULL,
  `record_count`          BIGINT(20) DEFAULT NULL,
  `source_datacenter`     VARCHAR(10) DEFAULT NULL,
  `source_deployment_env` VARCHAR(10) DEFAULT NULL,
  `source_database` VARCHAR(50) DEFAULT NULL,
  `source_table`    VARCHAR(50) DEFAULT NULL,
  `job_context`     VARCHAR(50) DEFAULT NULL,
  `project_name`    VARCHAR(100) DEFAULT NULL,
  `flow_name`       VARCHAR(100) DEFAULT NULL,
  `job_name`        VARCHAR(100) DEFAULT NULL,
  `flow_exec_id`    INT(11) DEFAULT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  PRIMARY KEY (`dataset`,`cluster`,`log_event_time`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- staging table for Gobblin tracking event distcp_ng
CREATE TABLE `stg_kafka_gobblin_distcp` (
  `cluster`     VARCHAR(20) NOT NULL,
  `dataset`     VARCHAR(100) NOT NULL,
  `partition_type`  VARCHAR(20) DEFAULT NULL,
  `partition_name`  VARCHAR(50) DEFAULT NULL,
  `upsteam_timestamp` BIGINT(20) DEFAULT NULL,
  `origin_timestamp`  BIGINT(20) DEFAULT NULL,
  `source_path`     VARCHAR(200) DEFAULT NULL,
  `target_path`     VARCHAR(200) DEFAULT NULL,
  `job_context`     VARCHAR(50) DEFAULT NULL,
  `project_name`    VARCHAR(100) DEFAULT NULL,
  `flow_name`       VARCHAR(100) DEFAULT NULL,
  `job_name`        VARCHAR(100) DEFAULT NULL,
  `flow_exec_id`    INT(11) DEFAULT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  PRIMARY KEY (`dataset`,`cluster`,`partition_name`,`log_event_time`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- staging table for Metastore Audit Event, include TableAudit / PartitionAudit
CREATE TABLE `stg_kafka_metastore_audit` (
  `server`          VARCHAR(20) NOT NULL,
  `instance`        VARCHAR(20) NOT NULL,
  `app_name`        VARCHAR(50) NOT NULL,
  `event_name`      VARCHAR(50) NOT NULL,
  `event_type`      VARCHAR(30) NOT NULL,
  `log_event_time`  BIGINT(20) NOT NULL,
  `metastore_thrift_uri`  VARCHAR(200) DEFAULT NULL,
  `metastore_version`     VARCHAR(20) DEFAULT NULL,
  `is_successful`         VARCHAR(5) DEFAULT NULL,
  `is_data_deleted`       VARCHAR(5) DEFAULT NULL,
  `db_name`         VARCHAR(100) NOT NULL,
  `table_name`      VARCHAR(100) NOT NULL,
  `time_partition`  VARCHAR(100) NOT NULL,
  `location`        VARCHAR(200) DEFAULT NULL,
  `owner`           VARCHAR(100) DEFAULT NULL,
  `create_time`     BIGINT(20) DEFAULT NULL,
  `last_access_time`  BIGINT(20) DEFAULT NULL,
  `old_info`          MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  `new_info`          MEDIUMTEXT CHAR SET utf8 DEFAULT NULL,
  PRIMARY KEY (`db_name`,`table_name`,`time_partition`,`instance`,`log_event_time`,`event_type`)
)
  ENGINE=InnoDB
  DEFAULT CHARSET=latin1;


-- Combine multiple data log status from Kafka events into a status table
SET TIME_ZONE='US/Pacific';	-- this needs to be customized based on your time zone
SELECT @@session.time_zone, current_timestamp;

CREATE TABLE log_dataset_instance_load_status  (
	dataset_id         	int(11) UNSIGNED NOT NULL DEFAULT '0',
	db_id              	smallint(6) NOT NULL DEFAULT '0',
	dataset_type       	varchar(30) COMMENT 'hive,teradata,oracle,hdfs...'  NOT NULL,
	dataset_native_name	varchar(200) NOT NULL,
	operation_type     	varchar(50) COMMENT 'load, merge, compact, update, delete'  NULL,
	partition_grain    	varchar(30) COMMENT 'snapshot, delta, daily, daily, monthly...'  NOT NULL,
	partition_expr     	varchar(500) COMMENT 'partition name or expression'  NOT NULL,
	data_time_expr     	varchar(20) COMMENT 'datetime literal of the data datetime'  NOT NULL,
	data_time_epoch    	int(11) COMMENT 'epoch second of the data datetime'  NOT NULL,
	record_count       	bigint(20) NULL,
	size_in_byte       	bigint(20) NULL,
	log_time_epoch     	int(11) COMMENT 'When data is loaded or published'  NOT NULL,
	ref_dataset_type   	varchar(30) COMMENT 'Refer to the underlying dataset'  NULL,
	ref_db_id          	int(11) COMMENT 'Refer to db of the underlying dataset'  NULL,
	ref_uri            	varchar(300) COMMENT 'Table name or HDFS location'  NULL,
	last_modified      	timestamp NULL,
	PRIMARY KEY(dataset_id,db_id,data_time_epoch,partition_grain,partition_expr),
	KEY(dataset_native_name),
	KEY(ref_uri)
)
ENGINE = InnoDB
CHARACTER SET latin1
AUTO_INCREMENT = 0
COMMENT = 'Capture the load/publish ops for dataset instance'
PARTITION BY RANGE COLUMNS (data_time_epoch)
( PARTITION P201601 VALUES LESS THAN (unix_timestamp(date'2016-02-01')),
  PARTITION P201602 VALUES LESS THAN (unix_timestamp(date'2016-03-01')),
  PARTITION P201603 VALUES LESS THAN (unix_timestamp(date'2016-04-01')),
  PARTITION P201604 VALUES LESS THAN (unix_timestamp(date'2016-05-01')),
  PARTITION P201605 VALUES LESS THAN (unix_timestamp(date'2016-06-01')),
  PARTITION P201606 VALUES LESS THAN (unix_timestamp(date'2016-07-01')),
  PARTITION P201607 VALUES LESS THAN (unix_timestamp(date'2016-08-01')),
  PARTITION P201608 VALUES LESS THAN (unix_timestamp(date'2016-09-01')),
  PARTITION P201609 VALUES LESS THAN (unix_timestamp(date'2016-10-01')),
  PARTITION P201610 VALUES LESS THAN (unix_timestamp(date'2016-11-01')),
  PARTITION P201611 VALUES LESS THAN (unix_timestamp(date'2016-12-01')),
  PARTITION P201612 VALUES LESS THAN (unix_timestamp(date'2017-01-01')),
  PARTITION P203507 VALUES LESS THAN (unix_timestamp(date'2035-08-01'))
) ;

