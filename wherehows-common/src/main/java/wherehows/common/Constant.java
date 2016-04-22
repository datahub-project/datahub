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

  // For property_name in wh_property table
  /** The property_name field in wh_property table for WhereHows database connection information */
  public static final String WH_DB_URL_KEY = "wherehows.db.jdbc.url";
  public static final String WH_DB_USERNAME_KEY = "wherehows.db.username";
  public static final String WH_DB_PASSWORD_KEY = "wherehows.db.password";
  public static final String WH_DB_DRIVER_KEY = "wherehows.db.driver";

  /** The property_name field in wh_property table. Location of the folder that store interim csv file. */
  public static final String WH_APP_FOLDER_KEY = "wherehows.app_folder";

  // For property_name in wh_etl_job_property table
  // Lineage
  /** The property_name field in wh_etl_job_property table. Azkaban database connection info */
  public static final String AZ_DB_URL_KEY = "az.db.jdbc.url";
  public static final String AZ_DB_USERNAME_KEY = "az.db.username";
  public static final String AZ_DB_PASSWORD_KEY = "az.db.password";
  public static final String AZ_DB_DRIVER_KEY = "az.db.driver";

  /** The property_name field in wh_etl_job_property table. The time period backtrace for execution data ETL */
  public static final String AZ_EXEC_ETL_LOOKBACK_MINS_KEY = "az.exec_etl.lookback_period.in.minutes";

  /** The property_name field in wh_etl_job_property table. Hadoop job history url for retrieving map-reduce job logs */
  public static final String AZ_HADOOP_JOBHISTORY_KEY = "az.hadoop.jobhistory.server.url";
  /** The property_name field in wh_etl_job_property table. Default hadoop database id for this azkaban instance */
  public static final String AZ_DEFAULT_HADOOP_DATABASE_ID_KEY = "az.default.hadoop.database.id";
  /** The property_name field in wh_etl_job_property table. For this period of jobs that finished, we will extract their lineage */
  public static final String AZ_LINEAGE_ETL_LOOKBACK_MINS_KEY = "az.lineage_etl.lookback_period.in.minutes";
  /** The property_name field in wh_etl_job_property table. In lineage ETl, Akka actor time out time */
  public static final String LINEAGE_ACTOR_TIMEOUT_KEY = "az.lineage.actor.timeout";

  public static final String LINEAGE_ACTOR_NUM = "az.lineage.actor.num";
  /** The property_name field in wh_etl_job_property table. Optional property used for debug. Default end timestamp is now */
  public static final String AZ_LINEAGE_ETL_END_TIMESTAMP_KEY = "az.lineage_etl.end_timestamp";
  /** The property_name field in wh_etl_job_property table. Azkaban Server URL (optional way to get azkaban execution log) */
  public static final String AZ_SERVICE_URL_KEY = "az.server.url";
  public static final String AZ_SERVICE_USERNAME_KEY = "az.server.username";
  public static final String AZ_SERVICE_PASSWORD_KEY = "az.server.password";

  //Appworx
  public static final String AW_DB_URL_KEY = "aw.db.jdbc.url";
  public static final String AW_DB_USERNAME_KEY = "aw.db.username";
  public static final String AW_DB_PASSWORD_KEY = "aw.db.password";
  public static final String AW_DB_NAME_KEY = "aw.db.name";
  public static final String AW_DB_DRIVER_KEY = "aw.db.driver";
  public static final String AW_DB_PORT_KEY =  "aw.db.port";
  public static final String AW_ARCHIVE_DIR =  "aw.archive.dir";
  /** The property_name field in wh_etl_job_property table. The time period backtrace for execution data ETL */
  public static final String AW_EXEC_ETL_LOOKBACK_KEY = "aw.exec_etl.lookback_period.in.days";

  // Oozie
  /** The property_name field in wh_etl_job_property table. Oozie database connection info */
  public static final String OZ_DB_URL_KEY = "oz.db.jdbc.url";
  public static final String OZ_DB_USERNAME_KEY = "oz.db.username";
  public static final String OZ_DB_PASSWORD_KEY = "oz.db.password";
  public static final String OZ_DB_DRIVER_KEY = "oz.db.driver";
  /** The property_name field in wh_etl_job_property table. Oozie execution info ETL lookback time */
  public static final String OZ_EXEC_ETL_LOOKBACK_MINS_KEY = "oz.exec_etl.lookback_period.in.minutes";

  /** Optional. The property_name field in wh_etl_job_property table. Set innodb_lock_wait_timeout for mysql */
  public static final String INNODB_LOCK_WAIT_TIMEOUT = "innodb_lock_wait_timeout";

  // Teradata
  /** The property_name field in wh_etl_job_property table. Teradata connection info */
  public static final String TD_DB_URL_KEY = "teradata.db.jdbc.url";
  public static final String TD_DB_USERNAME_KEY = "teradata.db.username";
  public static final String TD_DB_PASSWORD_KEY = "teradata.db.password";
  public static final String TD_DB_DRIVER_KEY = "teradata.db.driver";
  /** The property_name field in wh_etl_job_property table. Teradata metadata raw interim file store location */
  public static final String TD_METADATA_KEY = "teradata.metadata";
  /** The property_name field in wh_etl_job_property table. Teradata field metadata interim file store location */
  public static final String TD_FIELD_METADATA_KEY = "teradata.field_metadata";
  /** The property_name field in wh_etl_job_property table. Teradata schema interim file store location */
  public static final String TD_SCHEMA_OUTPUT_KEY = "teradata.schema_output";
  /** The property_name field in wh_etl_job_property table. Teradata sample data interim file store location */
  public static final String TD_SAMPLE_OUTPUT_KEY = "teradata.sample_output";
  /** The property_name field in wh_etl_job_property table. Teradata log file store location */
  public static final String TD_LOG_KEY = "teradata.log";
  /** The property_name field in wh_etl_job_property table. Teradata databases to collect metadata */
  public static final String TD_TARGET_DATABASES_KEY = "teradata.databases";
  /** The property_name field in wh_etl_job_property table. Used for connecting */
  public static final String TD_DEFAULT_DATABASE_KEY = "teradata.default_database";
  /** Optional. The property_name field in wh_etl_job_property table. Decide whether load sample data or not */
  public static final String TD_LOAD_SAMPLE = "teradata.load_sample";

  // Hdfs
  /** The property_name field in wh_etl_job_property table. Whether using remote mode or not */
  public static final String HDFS_REMOTE = "hdfs.remote.mode";
  /** The property_name field in wh_etl_job_property table. The hfds remote user that run the hadoop job on gateway */
  public static final String HDFS_REMOTE_USER_KEY = "hdfs.remote.user";
  /** The property_name field in wh_etl_job_property table. The gateway machine name*/
  public static final String HDFS_REMOTE_MACHINE_KEY = "hdfs.remote.machine";
  /** The property_name field in wh_etl_job_property table. The private key location of remote user */
  public static final String HDFS_PRIVATE_KEY_LOCATION_KEY = "hdfs.private_key_location";
  /** The property_name field in wh_etl_job_property table. The jar file location that need to run */
  public static final String HDFS_REMOTE_JAR_KEY = "hdfs.remote.jar";
  /** The property_name field in wh_etl_job_property table. The raw hfds metadata file (in json format) location store on local machine */
  public static final String HDFS_SCHEMA_LOCAL_PATH_KEY = "hdfs.local.raw_metadata";
  /** The property_name field in wh_etl_job_property table. The hfds metadata file location store on remote hadoop gateway */
  public static final String HDFS_SCHEMA_REMOTE_PATH_KEY = "hdfs.remote.raw_metadata";
  /** The property_name field in wh_etl_job_property table. The hfds sample data file location store on local machine */
  public static final String HDFS_SAMPLE_LOCAL_PATH_KEY = "hdfs.local.sample";
  /** The property_name field in wh_etl_job_property table. The hfds sample data file location store on remote hadoop gateway */
  public static final String HDFS_SAMPLE_REMOTE_PATH_KEY = "hdfs.remote.sample";
  /** The property_name field in wh_etl_job_property table. Hadoop cluster name */
  public static final String HDFS_CLUSTER_KEY = "hdfs.cluster";
  /** The property_name field in wh_etl_job_property table. The list of directories as a start point to fetch metadata.
   * (include all of their sub directories) */
  public static final String HDFS_WHITE_LIST_KEY = "hdfs.white_list";
  /** The property_name field in wh_etl_job_property table. Number of thread to do the metadata collecting */
  public static final String HDFS_NUM_OF_THREAD_KEY = "hdfs.num_of_thread";
  /** The property_name field in wh_etl_job_property table.  The hfds metadata file (in csv format) location store on local machine */
  public static final String HDFS_SCHEMA_RESULT_KEY = "hdfs.local.metadata";
  /** The property_name field in wh_etl_job_property table.  The field metadata file (in csv format) location store on local machine */
  public static final String HDFS_FIELD_RESULT_KEY = "hdfs.local.field_metadata";
  /** The property_name field in wh_etl_job_property table.  The map of file path regex and dataset source
   * e.g. [{"/data/tracking.*":"Kafka"},{"/data/retail.*":"Teradata"}] */
  public static final String HDFS_FILE_SOURCE_MAP_KEY = "hdfs.file_path_regex_source_map";
  /** The property_name field in wh_etl_job_property table. Keytab file location */
  public static final String HDFS_REMOTE_KEYTAB_LOCATION_KEY = "hdfs.remote.keytab.location";

  /** The property_name field in wh_etl_job_property table. For dataset owner ETL. The hfds location to copy files */
  public static final String HDFS_REMOTE_WORKING_DIR = "hdfs.remote.working.dir";

  // ui
  /** File name of dataset tree that used by front end to show tree */
  public static final String DATASET_TREE_FILE_NAME_KEY = "wherehows.ui.tree.dataset.file";
  /** File name of flow tree that used by front end to show tree */
  public static final String FLOW_TREE_FILE_NAME_KEY = "wherehows.ui.tree.flow.file";

  // hdfs owner
  public static final String HDFS_OWNER_HIVE_QUERY_KEY = "hdfs.owner.hive.query";

  // ldap
  public static final String LDAP_CEO_USER_ID_KEY = "ldap.ceo.user.id";
  public static final String LDAP_CONTEXT_FACTORY_KEY = "ldap.context.factory";
  public static final String LDAP_CONTEXT_PROVIDER_URL_KEY = "ldap.context.provider.url";
  public static final String LDAP_CONTEXT_SECURITY_PRINCIPAL_KEY = "ldap.context.security.principal";
  public static final String LDAP_CONTEXT_SECURITY_CREDENTIALS_KEY = "ldap.context.security.credentials";
  public static final String LDAP_SEARCH_DOMAINS_KEY = "ldap.search.domains";
  public static final String LDAP_INACTIVE_DOMAIN_KEY = "ldap.inactive.domain";
  public static final String LDAP_SEARCH_RETURN_ATTRS_KEY = "ldap.search.return.attributes";
  public static final String LDAP_GROUP_CONTEXT_FACTORY_KEY = "ldap.group.context.factory";
  public static final String LDAP_GROUP_CONTEXT_PROVIDER_URL_KEY = "ldap.group.context.provider.url";
  public static final String LDAP_GROUP_CONTEXT_SECURITY_PRINCIPAL_KEY = "ldap.group.context.security.principal";
  public static final String LDAP_GROUP_CONTEXT_SECURITY_CREDENTIALS_KEY = "ldap.group.context.security.credentials";
  public static final String LDAP_GROUP_APP_ID_KEY = "ldap.group.app.id";
  public static final String LDAP_GROUP_SEARCH_DOMAINS_KEY = "ldap.group.search.domains";
  public static final String LDAP_GROUP_SEARCH_RETURN_ATTRS_KEY = "ldap.group.search.return.attributes";

  // git
  public static final String GIT_HOST_KEY = "git.host";
  public static final String GIT_PROJECT_WHITELIST_KEY = "git.project.whitelist";

  // hive
  public static final String HIVE_METASTORE_JDBC_DRIVER = "hive.metastore.jdbc.driver";
  public static final String HIVE_METASTORE_JDBC_URL = "hive.metastore.jdbc.url";
  public static final String HIVE_METASTORE_USERNAME = "hive.metastore.username";
  public static final String HIVE_METASTORE_PASSWORD = "hive.metastore.password";

  public static final String HIVE_DATABASE_WHITELIST_KEY = "hive.database_white_list";
  public static final String HIVE_SCHEMA_JSON_FILE_KEY = "hive.schema_json_file";
  public static final String HIVE_DEPENDENCY_CSV_FILE_KEY = "hive.dependency_csv_file";
  public static final String HIVE_INSTANCE_CSV_FILE_KEY = "hive.instance_csv_file";
  // public static final String HIVE_SAMPLE_CSV_FILE_KEY = "hive.sample_csv";
  public static final String HIVE_SCHEMA_CSV_FILE_KEY = "hive.schema_csv_file";
  public static final String HIVE_FIELD_METADATA_KEY = "hive.field_metadata";


  /** Property name of app id. For ETL process. ETL process will use this to identify the application */
  public static final String APP_ID_KEY = "app.id";
  /** Property name of database id. ETL process will use this to identify the database */
  public static final String DB_ID_KEY = "db.id";
  /** Property name of wherehows execution id for ETL process. */
  public static final String WH_EXEC_ID_KEY = "wh.exec.id";

  public static final String WH_ELASTICSEARCH_URL_KEY = "wh.elasticsearch.url";
  public static final String WH_ELASTICSEARCH_PORT_KEY = "wh.elasticsearch.port";
  // dali
  public static final String DALI_GIT_URN_KEY = "dali.git.urn";
  public static final String GIT_COMMITTER_BLACKLIST_KEY = "git.committer.blacklist";


}
