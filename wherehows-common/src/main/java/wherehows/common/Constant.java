/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package wherehows.common;

/**
 * Created by zsun on 9/29/15.
 */
public class Constant {

  public static final String WH_DB_URL_KEY = "wherehows.db.jdbc.url";
  public static final String WH_DB_USERNAME_KEY = "wherehows.db.username";
  public static final String WH_DB_PASSWORD_KEY = "wherehows.db.password";
  public static final String WH_DB_DRIVER_KEY = "wherehows.db.driver";
  public static final String WH_APP_FOLDER_KEY = "wherehows.app_folder";

  public static final String AZ_DB_URL_KEY = "az.db.jdbc.url";
  public static final String AZ_DB_USERNAME_KEY = "az.db.username";
  public static final String AZ_DB_PASSWORD_KEY = "az.db.password";
  public static final String AZ_DB_DRIVER_KEY = "az.db.driver";
  public static final String AZ_EXEC_ETL_LOOKBACK_MINS_KEY = "az.exec_etl.lookback_period.in.minutes";

  public static final String APP_ID_KEY = "app.id";
  public static final String DB_ID_KEY = "db.id";
  public static final String WH_EXEC_ID_KEY = "wh.exec.id";

  public static final String AZ_HADOOP_JOBHISTORY_KEY = "az.hadoop.jobhistory.server.url";
  public static final String AZ_DEFAULT_HADOOP_DATABASE_ID_KEY = "az.default.hadoop.database.id";
  public static final String AZ_LINEAGE_ETL_LOOKBACK_MINS_KEY = "az.lineage_etl.lookback_period.in.minutes";
  public static final String LINEAGE_ACTOR_TIMEOUT_KEY = "az.lineage.actor.timeout";
  public static final String AZ_LINEAGE_ETL_END_TIMESTAMP_KEY = "az.lineage_etl.end_timestamp";

  public static final String AZ_SERVICE_URL_KEY = "az.server.url";
  public static final String AZ_SERVICE_USERNAME_KEY = "az.server.username";
  public static final String AZ_SERVICE_PASSWORD_KEY = "az.server.password";

  public static final String OZ_DB_URL_KEY = "oz.db.jdbc.url";
  public static final String OZ_DB_USERNAME_KEY = "oz.db.username";
  public static final String OZ_DB_PASSWORD_KEY = "oz.db.password";
  public static final String OZ_DB_DRIVER_KEY = "oz.db.driver";
  public static final String OZ_EXEC_ETL_LOOKBACK_MINS_KEY = "oz.exec_etl.lookback_period.in.minutes";

  // teradata
  public static final String TD_DB_URL_KEY = "teradata.db.jdbc.url";
  public static final String TD_DB_USERNAME_KEY = "teradata.db.username";
  public static final String TD_DB_PASSWORD_KEY = "teradata.db.password";
  public static final String TD_DB_DRIVER_KEY = "teradata.db.driver";
  public static final String TD_METADATA_KEY = "teradata.metadata";
  public static final String TD_FIELD_METADATA_KEY = "teradata.field_metadata";
  public static final String TD_SCHEMA_OUTPUT_KEY = "teradata.schema_output";
  public static final String TD_SAMPLE_OUTPUT_KEY = "teradata.sample_output";
  public static final String TD_LOG_KEY = "teradata.log";
  public static final String TD_TARGET_DATABASES_KEY = "teradata.databases";
  public static final String TD_DEFAULT_DATABASE_KEY = "teradata.default_database";

  // hdfs
  public static final String HDFS_SCHEMA_LOCAL_PATH_KEY = "hdfs.local.raw_metadata";
  public static final String HDFS_SCHEMA_REMOTE_PATH_KEY = "hdfs.remote.raw_metadata";
  public static final String HDFS_SAMPLE_LOCAL_PATH_KEY = "hdfs.local.sample";
  public static final String HDFS_SAMPLE_REMOTE_PATH_KEY = "hdfs.remote.sample";
  public static final String HDFS_CLUSTER_KEY = "hdfs.cluster";
  public static final String HDFS_WHITE_LIST_KEY = "hdfs.white_list";
  public static final String HDFS_NUM_OF_THREAD_KEY = "hdfs.num_of_thread";
  public static final String HDFS_SCHEMA_RESULT_KEY = "hdfs.local.metadata";
  public static final String HDFS_FIELD_RESULT_KEY = "hdfs.local.field_metadata";
  public static final String HDFS_REMOTE_USER_KEY = "hdfs.remote.user";
  public static final String HDFS_REMOTE_MACHINE_KEY = "hdfs.remote.machine";
  public static final String HDFS_PRIVATE_KEY_LOCATION_KEY = "hdfs.private_key_location";
  public static final String HDFS_REMOTE_JAR_KEY = "hdfs.remote.jar";

  // ui
  public static final String DATASET_TREE_FILE_NAME_KEY = "wherehows.ui.tree.dataset.file";
  public static final String FLOW_TREE_FILE_NAME_KEY = "wherehows.ui.tree.flow.file";

  // hive
  public static final String HIVE_METASTORE_JDBC_DRIVER = "hive.metastore.jdbc.driver";
  public static final String HIVE_METASTORE_JDBC_URL = "hive.metastore.jdbc.url";
  public static final String HIVE_METASTORE_USERNAME = "hive.metstore.username";
  public static final String HIVE_METASTORE_PASSWORD = "hive.metastore.password";

  public static final String HIVE_SCHEMA_JSON_FILE_KEY = "hive.schema_json_file";
  // public static final String HIVE_SAMPLE_CSV_FILE_KEY = "hive.sample_csv";
  public static final String HIVE_SCHEMA_CSV_FILE_KEY = "hive.schema_csv_file";
  public static final String HIVE_FIELD_METADATA_KEY = "hive.field_metadata";
}
