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
package models.daos;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import metadata.etl.models.EtlJobName;
import metadata.etl.models.EtlJobStatus;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.KeyHolder;
import play.libs.Time;
import utils.JdbcUtil;
import utils.JsonUtil;

import java.sql.SQLException;


/**
 * Created by zechen on 9/25/15.
 */
public class EtlJobDao {
  public static final String GET_ETL_JOB_BY_ID = "SELECT * FROM wh_etl_job where wh_etl_job_id = :id";

  public static final String INSERT_ETL_JOB =
    "INSERT INTO wh_etl_job (wh_etl_job_name, wh_etl_type, cron_expr, ref_id, timeout, next_run, comments, ref_id_type) "
      + " VALUES (:whEtlJobName, :whEtlType, :cronExpr, :refId, :timeout, :nextRun, :comments, :refIdType)";

  public static final String GET_DUE_JOBS =
    "SELECT * FROM wh_etl_job WHERE next_run <= :currentTime and is_active = 'Y'";

  public static final String GET_ALL_JOBS =
    "SELECT * FROM wh_etl_job";

  public static final String UPDATE_NEXT_RUN =
    "UPDATE wh_etl_job SET next_run = :nextRun WHERE wh_etl_job_id = :whEtlJobId";

  public static final String INSERT_NEW_RUN = "INSERT INTO wh_etl_job_execution(wh_etl_job_id, status, request_time) "
    + "VALUES (:whEtlJobId, :status, :requestTime)";

  public static final String START_RUN =
    "UPDATE wh_etl_job_execution set status = :status, message = :message, start_time = :startTime where wh_etl_exec_id = :whEtlExecId";

  public static final String END_RUN =
    "UPDATE wh_etl_job_execution set status = :status, message = :message, end_time = :endTime where wh_etl_exec_id = :whEtlExecId";

  public static final String UPDATE_JOB_PROCESS_ID_AND_HOSTNAME =
    "UPDATE wh_etl_job_execution SET process_id=?, host_name=? WHERE wh_etl_exec_id =?";

  public static final String UPDATE_JOB_STATUS =
    "UPDATE wh_etl_job SET is_active = :isActive WHERE wh_etl_job_name = :whEtlJobName and ref_id = :refId";

  public static final String UPDATE_JOB_SCHEDULE =
    "UPDATE wh_etl_job SET cron_expr = :cronExpr WHERE wh_etl_job_name = :whEtlJobName and ref_id = :refId";

  public static final String DELETE_JOB =
    "DELETE FROM wh_etl_job WHERE wh_etl_job_name = :whEtlJobName and ref_id = :refId";
  public static final String DELETE_JOB_PROPERTIES =
    "DELETE FROM wh_etl_job_property WHERE wh_etl_job_name = :whEtlJobName and ref_id = :refId";


  public static List<Map<String, Object>> getAllJobs() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_JOBS);
  }

  public static Map<String, Object> getEtlJobById(int id)
    throws SQLException {
    Map<String, Object> params = new HashMap<>();
    params.put("id", id);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForMap(GET_ETL_JOB_BY_ID, params);
  }

  public static int insertEtlJob(JsonNode etlJob)
    throws Exception {
    Map<String, Object> params = new HashMap<>();

    params.put("whEtlJobName", JsonUtil.getJsonValue(etlJob, "wh_etl_job_name", String.class));
    EtlJobName whEtlJobName = EtlJobName.valueOf((String) params.get("whEtlJobName"));
    params.put("whEtlType", whEtlJobName.getEtlType().toString());
    params.put("refIdType", whEtlJobName.getRefIdType().toString());
    params.put("refId", JsonUtil.getJsonValue(etlJob, "ref_id", Integer.class));
    params.put("cronExpr", JsonUtil.getJsonValue(etlJob, "cron_expr", String.class));
    params.put("timeout", JsonUtil.getJsonValue(etlJob, "timeout", Integer.class, null));
    params.put("nextRun", JsonUtil.getJsonValue(etlJob, "next_run", Integer.class, System.currentTimeMillis() / 1000));
    params.put("comments", JsonUtil.getJsonValue(etlJob, "comments", String.class, null));


    ObjectMapper om = new ObjectMapper();
    Map<String, String> properties = om.convertValue(etlJob.findPath("properties"),
      om.getTypeFactory().constructMapType(HashMap.class, String.class, String.class));
    if (properties == null) {
      properties = new HashMap<>();
    }

    Set<String> encryptedPropertyKeys = om.convertValue(etlJob.findPath("encrypted_property_keys"),
      om.getTypeFactory().constructCollectionType(HashSet.class, String.class));
    if (encryptedPropertyKeys == null) {
      encryptedPropertyKeys = new HashSet<>();
    }

    if (!properties.keySet().containsAll(encryptedPropertyKeys)) {
      throw new IllegalArgumentException("Some encrypted keys are not in properties");
    }

    for (String propertyKey : properties.keySet()) {
      EtlJobPropertyDao.insertJobProperty(whEtlJobName, (int) params.get("refId"), propertyKey, properties.get(propertyKey),
        encryptedPropertyKeys.contains(propertyKey));
    }

    KeyHolder kh = JdbcUtil.insertRow(INSERT_ETL_JOB, params);
    return kh.getKey().intValue();
  }

  public static void updateJobStatus(JsonNode jobStatus)
    throws Exception {
    EtlJobName whEtlJobName = EtlJobName.valueOf((String) JsonUtil.getJsonValue(jobStatus, "wh_etl_job_name", String.class));
    int refId = (Integer) JsonUtil.getJsonValue(jobStatus, "ref_id", Integer.class);
    String control = (String) JsonUtil.getJsonValue(jobStatus, "control", String.class);

    if (control.toLowerCase().equals("activate")) {
      EtlJobDao.updateJobStatus(whEtlJobName, refId, true);
    }

    if (control.toLowerCase().equals("deactivate")) {
      EtlJobDao.updateJobStatus(whEtlJobName, refId, false);
    }

    if (control.toLowerCase().equals("delete")) {
      EtlJobDao.deleteJob(whEtlJobName, refId);
    }
  }

  public static void updateJobStatus(EtlJobName whEtlJobName, int refId, boolean active)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlJobName", whEtlJobName.toString());
    params.put("refId", refId);
    params.put("isActive", active ? "Y" : "N");
    JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_JOB_STATUS, params);
  }

  public static void updateJobSchedule(JsonNode jobSchedule)
    throws Exception {
    EtlJobName whEtlJobName = EtlJobName.valueOf((String) JsonUtil.getJsonValue(jobSchedule, "wh_etl_job_name", String.class));
    int refId = (Integer) JsonUtil.getJsonValue(jobSchedule, "ref_id", Integer.class);
    String cronExpr = (String) JsonUtil.getJsonValue(jobSchedule, "cron_expr", String.class);
    updateJobSchedule(whEtlJobName, refId, cronExpr);
  }

  public static void updateJobSchedule(EtlJobName whEtlJobName, int refId, String cronExpr)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlJobName", whEtlJobName.toString());
    params.put("refId", refId);
    if (!Time.CronExpression.isValidExpression(cronExpr)) {
      throw new IllegalArgumentException("Invalid cron expression, please refer to quartz document");
    }
    params.put("cronExpr", cronExpr);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_JOB_SCHEDULE, params);
  }

  public static void deleteJob(EtlJobName whEtlJobName, int refId)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlJobName", whEtlJobName.toString());
    params.put("refId", refId);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(DELETE_JOB, params);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(DELETE_JOB_PROPERTIES, params);
  }

  /**
   * Update the next run time for the etl job using Quartz cron expression
   * @param etlJobId
   * @param cronExprStr
   * @param startTime
   * @throws Exception
   */
  public static void updateNextRun(int etlJobId, String cronExprStr, Date startTime)
    throws Exception {
    Time.CronExpression cronExpression = new Time.CronExpression(cronExprStr);
    Date nextTime = cronExpression.getNextValidTimeAfter(startTime);
    updateNextRun(etlJobId, nextTime);
  }

  public static void updateNextRun(int etlJobId, Date nextTime)
    throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("nextRun", String.valueOf(nextTime.getTime() / 1000));
    params.put("whEtlJobId", etlJobId);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_NEXT_RUN, params);
  }

  public static List<Map<String, Object>> getDueJobs() {
    Map<String, Object> params = new HashMap<>();
    params.put("currentTime", System.currentTimeMillis() / 1000);
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(GET_DUE_JOBS, params);
  }

  public static long insertNewRun(int whEtlJobId) {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlJobId", whEtlJobId);
    params.put("status", EtlJobStatus.REQUESTED.toString());
    params.put("requestTime", System.currentTimeMillis() / 1000);
    KeyHolder keyHolder = JdbcUtil.insertRow(INSERT_NEW_RUN, params);
    return (Long) keyHolder.getKey();
  }

  public static void startRun(long whEtlExecId, String message) {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlExecId", whEtlExecId);
    params.put("status", EtlJobStatus.STARTED.toString());
    params.put("startTime", System.currentTimeMillis() / 1000);
    params.put("message", message);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(START_RUN, params);
  }

  public static void endRun(long whEtlExecId, EtlJobStatus status, String message) {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlExecId", whEtlExecId);
    params.put("status", status.toString());
    params.put("endTime", System.currentTimeMillis() / 1000);
    params.put("message", message);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(END_RUN, params);
  }

  public static void updateJobProcessInfo(long whEtlExecId, int processId, String hostname)
      throws DataAccessException {
    JdbcUtil.wherehowsJdbcTemplate.update(UPDATE_JOB_PROCESS_ID_AND_HOSTNAME, processId, hostname, whEtlExecId);
  }
}
