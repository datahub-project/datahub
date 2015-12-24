CREATE TABLE `dq_agg_def` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id of the aggregation definition',
  `dataset_name` varchar(255) NOT NULL COMMENT 'dataset name',
  `storage_type` varchar(255) NOT NULL COMMENT 'storage type of the dataset',
  `owner` varchar(255) DEFAULT NULL COMMENT 'owner of this aggregation',
  `tag` varchar(255) DEFAULT NULL COMMENT 'tag of this aggregation',
  `frequency` varchar(127) DEFAULT NULL COMMENT 'interval of running this aggregation',
  `flow_frequency` varchar(127) DEFAULT NULL COMMENT 'interval of the flow which refresh this dataset',
  `next_run` timestamp NULL DEFAULT NULL COMMENT 'timestamp when the next aggregation will run, only apply to scheduled aggregation',
  `time_created` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT 'timestamp when the aggregation definition is created',
  `time_modified` timestamp NULL DEFAULT NULL COMMENT 'timestamp when the aggregation definition modified',
  `update_time_column` varchar(127) DEFAULT NULL COMMENT 'the column name of time dimension',
  `event_time_column` varchar(127) DEFAULT NULL COMMENT 'the column name of time dimension',
  `time_dimension` varchar(127) DEFAULT NULL COMMENT 'the column name of time dimension',
  `time_grain` varchar(127) DEFAULT NULL COMMENT 'the granularity of the query data',
  `time_shift` int(11) DEFAULT NULL COMMENT 'time span in time dimension',
  `comment` varchar(255) DEFAULT NULL COMMENT 'comment of the aggregation',
  `agg_def_json` text COMMENT 'aggregation definition in json format'
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=59 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED COMMENT='Data Quality Aggregation Definition Table';

CREATE TABLE `dq_agg_log` (
  `run_id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'execution id of aggregation',
  `data_time` timestamp NULL DEFAULT NULL COMMENT 'date and time of the data',
  `agg_def_id` int(11) DEFAULT NULL COMMENT 'id of the aggregation definition',
  `status` varchar(127) DEFAULT NULL COMMENT 'status of this run',
  `start_time` timestamp NULL DEFAULT NULL COMMENT 'timestamp when the aggregation run',
  `finish_time` timestamp NULL DEFAULT NULL COMMENT 'timestamp when the aggregation finish',
  `partition_start` timestamp NULL DEFAULT NULL COMMENT 'start partition',
  `partition_end` timestamp NULL DEFAULT NULL COMMENT 'end partition',
  `error_msg` text COMMENT 'error message if run failed',
  `request_time` timestamp NULL DEFAULT NULL COMMENT 'timestamp when the aggregation requested',
  PRIMARY KEY (`run_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6780 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED COMMENT='Data Quality Aggregation Log Table';

CREATE TABLE `dq_agg_comb` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id of the aggregation rule',
  `agg_def_id` int(11) DEFAULT NULL COMMENT 'id of the parent aggregation definition',
  `agg_query_id` int(11) DEFAULT NULL COMMENT 'id of parent query',
  `formula` varchar(255) NOT NULL COMMENT 'formulas for aggregation',
  `data_type` varchar(255) NOT NULL COMMENT 'data type of the formula',
  `dimension` varchar(255) DEFAULT NULL COMMENT 'dimensions in group by',
  `time_dimension` varchar(127) DEFAULT NULL COMMENT 'the column name of time dimension',
  `time_grain` varchar(127) DEFAULT NULL COMMENT 'the granularity of the query data',
  `roll_up_func` varchar(255) DEFAULT NULL COMMENT 'roll up function for outer dimension',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=189 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED COMMENT='Data Quality Aggregation Combination Table';

CREATE TABLE `dq_agg_query` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'id of the aggregation rule',
  `agg_def_id` int(11) NOT NULL COMMENT 'id of parent aggregation definition',
  `dataset_name` varchar(255) NOT NULL COMMENT 'dataset name',
  `storage_type` varchar(255) NOT NULL COMMENT 'storage type of the dataset',
  `generated_query` text COMMENT 'generated query according to agg_def',
  `formula` varchar(255) NOT NULL COMMENT 'formulas for aggregation',
  `dimension` varchar(255) DEFAULT NULL COMMENT 'dimensions in group by',
  `time_dimension` varchar(127) DEFAULT NULL COMMENT 'the column name of time dimension',
  `time_grain` varchar(127) DEFAULT NULL COMMENT 'the granularity of the query data',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=80 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED COMMENT='Data Quality Aggregation Query Table';

CREATE TABLE `dq_agg_query_instance` (
  `run_id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'execution id of aggregation',
  `agg_query_id` int(11) NOT NULL DEFAULT '0' COMMENT 'id of the query',
  `query_string` text COMMENT 'query instance',
  PRIMARY KEY (`run_id`,`agg_query_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6780 DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED COMMENT='Data Quality Aggregation Query Instance Table';


