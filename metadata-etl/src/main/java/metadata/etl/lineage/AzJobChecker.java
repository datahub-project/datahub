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

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.Constant;
import wherehows.common.schemas.AzkabanJobExecRecord;
import wherehows.common.utils.AzkabanJobExecUtil;


/**
 * Created by zsun on 9/8/15.
 */
public class AzJobChecker {

  final int DEFAULT_LOOK_BACK_TIME_MINUTES = 10;
  int appId;
  Connection conn = null;
  private static final Logger logger = LoggerFactory.getLogger(AzJobChecker.class);

  public AzJobChecker(Properties prop)
    throws SQLException {
    appId = Integer.valueOf(prop.getProperty(Constant.APP_ID_KEY));
    String host = prop.getProperty(Constant.AZ_DB_URL_KEY);
    String userName = prop.getProperty(Constant.AZ_DB_USERNAME_KEY);
    String passWord = prop.getProperty(Constant.AZ_DB_PASSWORD_KEY);
    // set up connections
    conn = DriverManager.getConnection(host + "?" + "user=" + userName + "&password=" + passWord);
  }

  /**
   * Default 10 minutes
   * @return
   * @throws IOException
   * @throws SQLException
   */
  public List<AzkabanJobExecRecord> getRecentFinishedJobFromFlow()
    throws IOException, SQLException {
    return getRecentFinishedJobFromFlow(DEFAULT_LOOK_BACK_TIME_MINUTES);
  }

  /**
   * Get recent finished job from flow level
   * This way will have 2 benefits:
   * 1. Query flow level table, faster than query job level table
   * 2. We can calculate the job execution UUID by retrieving the whole flow info.
   * @param timeFrameMinutes the time in minutes that we used to search recent finished jobs
   * @return
   */
  public List<AzkabanJobExecRecord> getRecentFinishedJobFromFlow(int timeFrameMinutes)
    throws IOException, SQLException {
    long currentTimeStamp = System.currentTimeMillis();
    long oneHourAgo = currentTimeStamp - 1000 * 60 * timeFrameMinutes;
    return getRecentFinishedJobFromFlow(oneHourAgo);
  }

  /**
   * Read the blob from "flow_data", do a topological sort on the nodes. Give them the sort id.
   * @param timestamp the beginning timestamp
   * @return
   */
  public List<AzkabanJobExecRecord> getRecentFinishedJobFromFlow(long timestamp)
    throws SQLException, IOException {

    logger.info("Get the jobs from time : {}", timestamp);
    List<AzkabanJobExecRecord> results = new ArrayList<>();
    Statement stmt = conn.createStatement();
    final String cmd =
      "select exec_id, flow_id, status, submit_user, flow_data from execution_flows where end_time > " + timestamp;
    final ResultSet rs = stmt.executeQuery(cmd); // this sql take 3 second to execute

    while (rs.next()) {
      long execId = rs.getLong("exec_id");
      Blob flowBlob = rs.getBlob("flow_data");
      GZIPInputStream gzip = new GZIPInputStream(flowBlob.getBinaryStream());
      String flowJson = IOUtils.toString(gzip, "UTF-8");
      results.addAll(parseJson(flowJson, execId));
    }
    return results;
  }

  public List<AzkabanJobExecRecord> parseJson(String flowJson, long flowExecId)
    throws IOException {
    List<AzkabanJobExecRecord> results = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode wholeFlow = mapper.readTree(flowJson);
    JsonNode allJobs = wholeFlow.get("nodes");
    String flowPath = wholeFlow.get("projectName").asText() + ":" + wholeFlow.get("flowId").asText();
    for (JsonNode oneJob : allJobs) {
      String jobName = oneJob.get("id").asText();
      long startTime = oneJob.get("startTime").asLong();
      long endTime = oneJob.get("endTime").asLong();
      String status = oneJob.get("status").asText();
      AzkabanJobExecRecord azkabanJobExecRecord =
        new AzkabanJobExecRecord(appId, jobName, flowExecId, (int) (startTime / 1000), (int) (endTime / 1000), status,
          flowPath);
      results.add(azkabanJobExecRecord);
    }
    AzkabanJobExecUtil.sortAndSet(results);
    return results;
  }

  public void close()
    throws SQLException {
    conn.close();
  }
}
