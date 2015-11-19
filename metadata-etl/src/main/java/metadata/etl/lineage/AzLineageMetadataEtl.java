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
package metadata.etl.lineage;

import metadata.etl.EtlJob;
import wherehows.common.Constant;

import java.sql.*;
import java.util.Properties;


/**
 * Mining the lineage info from confg/logs
 *
 * Created by zsun on 7/29/15.
 */
public class AzLineageMetadataEtl extends EtlJob {

  public int timeFrame = -1;
  Connection conn;

  /**
   * Assume that all job running on one Azkaban instance will running on the same Hadoop instance
   * @param azkabanInstanceId
   */
  public AzLineageMetadataEtl(int azkabanInstanceId) {
    super(azkabanInstanceId, null, 0L);
    // for default
    try {
      setUp();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Used by backend server
   * @param appId The application id for the target azkaban server
   * @param whExecId
   * @param properties All the properties for this azkaban instance lineage collecting.
   */
  public AzLineageMetadataEtl(int appId, long whExecId, Properties properties) {
    super(appId, null, whExecId, properties);
    this.timeFrame = Integer.valueOf(this.prop.getProperty(Constant.AZ_LINEAGE_ETL_LOOKBACK_MINS_KEY));
    try {
      setUp();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public void setUp()
    throws SQLException {
    String wherehowsHost = this.prop.getProperty(Constant.WH_DB_URL_KEY);
    String wherehowsUserName = this.prop.getProperty(Constant.WH_DB_USERNAME_KEY);
    String wherehowsPassWord = this.prop.getProperty(Constant.WH_DB_PASSWORD_KEY);
    conn =
      DriverManager.getConnection(wherehowsHost + "?" + "user=" + wherehowsUserName + "&password=" + wherehowsPassWord);
  }

  @Override
  public void extract()
    throws Exception {
    logger.info("Azkaban lineage metadata extract for {} begin", this.prop.getProperty(Constant.APP_ID_KEY));
    String emptyStaggingTable =
      "DELETE FROM stg_job_execution_data_lineage WHERE app_id = " + this.prop.getProperty(Constant.APP_ID_KEY);
    conn.createStatement().execute(emptyStaggingTable);
    AzLineageExtractorMaster azLineageExtractorMaster = new AzLineageExtractorMaster(prop);
    // get lineage
    if (timeFrame > 0) {
      azLineageExtractorMaster.run(timeFrame);
    } else {
      azLineageExtractorMaster.run(10);
    }
    logger.info("Azkaban lineage metadata extract for {} end", this.prop.getProperty(Constant.APP_ID_KEY));
  }

  @Override
  public void transform()
    throws Exception {
    // Nothing
    logger.info("Azkaban lineage metadata transform");
  }

  @Override
  public void load()
    throws Exception {
    logger.info("Azkaban lineage metadata load");
    // insert into the final table
    // TODO: need to be insert on duplicate update, so the running flows can be updated
    String insertIntoFinalTable = "INSERT IGNORE INTO job_execution_data_lineage\n"
      + "SELECT app_id, flow_exec_id, job_exec_id, job_exec_uuid, job_name, job_start_unixtime, job_finished_unixtime,\n"
      + "db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,\n"
      + "layout_id, storage_type, source_target_type, srl_no, source_srl_no, operation,\n"
      + "record_count, insert_count, delete_count, update_count, flow_path, UNIX_TIMESTAMP(NOW()), " + this.prop
      .getProperty(Constant.WH_EXEC_ID_KEY) + " FROM stg_job_execution_data_lineage where app_id = " + this.prop
      .getProperty(Constant.APP_ID_KEY);
    logger.info("Azkaban Lineage load cmd :\n" + insertIntoFinalTable);
    conn.createStatement().execute(insertIntoFinalTable);

    logger.info("Azkaban lineage metadata ETL completed");
    if (prop.getProperty(Constant.APP_ID_KEY).equals("32")) {
      logger.info("TEMPORARY load war's data into cmdb database");
      loadIntoOthers();
    }
  }

  /**
   *  temporary solutions, will deprecate later : also insert into cmdb job_attempt_data_lineage table
   */
  @Deprecated
  public void loadIntoOthers()
    throws SQLException {
    String cmdbHost = this.prop.getProperty("cmdb.db.host");
    String cmdbUserName = this.prop.getProperty("cmdb.db.username");
    String cmdbPassWord = this.prop.getProperty("cmdb.db.password");
    Connection cmdbConn =
      DriverManager.getConnection(cmdbHost + "?" + "user=" + cmdbUserName + "&password=" + cmdbPassWord);

    String queryCmd =
      "SELECT app_id, flow_exec_id, job_exec_id, job_exec_uuid, job_name, job_start_unixtime, job_finished_unixtime,\n"
        + "db_id, abstracted_object_name, full_object_name, partition_start, partition_end, partition_type,\n"
        + "layout_id, storage_type, source_target_type, srl_no, source_srl_no, operation,\n"
        + "record_count, insert_count, delete_count, update_count, created_date, wh_etl_exec_id \n"
        + "FROM stg_job_execution_data_lineage";
    ResultSet resultSet = conn.createStatement().executeQuery(queryCmd);

    Statement cmdbStatement = cmdbConn.createStatement();
    while (resultSet.next()) {

      String insertCmd =
        "INSERT IGNORE INTO job_attempt_data_lineage (" + "application_id," + " job_id, " + "srl_no," + "database_id,"
          + "source_target_type," + "object_type," + "abstracted_object_name," + "full_object_name,"
          + "partition_start," + "partition_end," + "partition_type," + "storage_type," + "operation," + "record_count,"
          + "insert_count," + "delete_count," + "update_count," + "created_date," + "parent_srl_no," + "flow_exec_id,"
          + "job_start_unixtime," + "job_finished_unixtime" + ") VALUES (" + resultSet.getString("app_id") + ","
          + resultSet.getString("job_exec_id") + "," + resultSet.getString("srl_no") + "," + resultSet
          .getString("db_id") + ",'" + resultSet.getString("source_target_type") + "','" + "hdfs" + "','"
          + resultSet.getString("abstracted_object_name") + "','" + resultSet.getString("full_object_name") + "'," + (
          resultSet.getString("partition_start") != null ? "'" + resultSet.getString("partition_start") + "'" : "null")
          + "," + (resultSet.getString("partition_end") != null ? "'" + resultSet.getString("partition_end") + "'"
          : "null") + "," + (resultSet.getString("partition_type") != null ? "'" + resultSet.getString("partition_type")
          + "'" : "null") + ",'" + resultSet.getString("storage_type") + "','" + resultSet.getString("operation") + "',"
          + resultSet.getString("record_count") + "," + resultSet.getString("insert_count") + "," + resultSet
          .getString("delete_count") + "," + resultSet.getString("update_count") + "," + "CURRENT_TIME() ,"
          // TODO convert it to UTC?
          + resultSet.getString("source_srl_no") + "," + resultSet.getString("flow_exec_id") + "," + resultSet
          .getString("job_start_unixtime") + "," + resultSet.getString("job_finished_unixtime") + ")";

      logger.debug(insertCmd);

      cmdbStatement.execute(insertCmd);
    }
    cmdbConn.close();
  }

  @Override
  public void close()
    throws SQLException {
    this.conn.close();
  }
}
