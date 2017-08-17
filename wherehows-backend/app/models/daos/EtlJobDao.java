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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import wherehows.common.jobs.JobStatus;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.KeyHolder;
import play.libs.Time;
import utils.JdbcUtil;


/**
 * Created by zechen on 9/25/15.
 */
public class EtlJobDao {

  public static final String GET_ALL_SCHEDULED_JOBS =
      "SELECT wh_etl_job_name, enabled, next_run FROM wh_etl_job_schedule";

  public static final String UPDATE_NEXT_RUN =
      "INSERT INTO wh_etl_job_schedule (wh_etl_job_name, enabled, next_run) VALUES (:whEtlJobName, :enabled, :nextRun) "
          + "ON DUPLICATE KEY UPDATE next_run = :nextRun";

  public static final String INSERT_NEW_RUN = "INSERT INTO wh_etl_job_history(wh_etl_job_name, status, request_time) "
      + "VALUES (:whEtlJobName, :status, :requestTime)";

  public static final String START_RUN =
      "UPDATE wh_etl_job_history set status = :status, message = :message, start_time = :startTime where wh_etl_exec_id = :whEtlExecId";

  public static final String END_RUN =
      "UPDATE wh_etl_job_history set status = :status, message = :message, end_time = :endTime where wh_etl_exec_id = :whEtlExecId";

  public static final String UPDATE_JOB_PROCESS_ID_AND_HOSTNAME =
      "UPDATE wh_etl_job_history SET process_id=?, host_name=? WHERE wh_etl_exec_id =?";

  /**
   * Get all scheduled jobs from DB
   */
  public static List<Map<String, Object>> getAllScheduledJobs() throws Exception {
    return JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_ALL_SCHEDULED_JOBS);
  }

  /**
   * Update the next run time for the etl job using Quartz cron expression
   */
  public static void updateNextRun(String etlJobName, String cronExprStr, Date startTime) throws Exception {
    Time.CronExpression cronExpression = new Time.CronExpression(cronExprStr);
    Date nextTime = cronExpression.getNextValidTimeAfter(startTime);
    updateNextRun(etlJobName, nextTime);
  }

  public static void updateNextRun(String etlJobName, Date nextTime) throws Exception {
    Map<String, Object> params = new HashMap<>();
    params.put("nextRun", String.valueOf(nextTime.getTime() / 1000));
    params.put("enabled", true);
    params.put("whEtlJobName", etlJobName);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(UPDATE_NEXT_RUN, params);
  }

  public static long insertNewRun(String etlJobName) {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlJobName", etlJobName);
    params.put("status", JobStatus.REQUESTED.toString());
    params.put("requestTime", System.currentTimeMillis() / 1000);
    KeyHolder keyHolder = JdbcUtil.insertRow(INSERT_NEW_RUN, params);
    return (Long) keyHolder.getKey();
  }

  public static void startRun(long whEtlExecId, String message) {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlExecId", whEtlExecId);
    params.put("status", JobStatus.STARTED.toString());
    params.put("startTime", System.currentTimeMillis() / 1000);
    params.put("message", message);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(START_RUN, params);
  }

  public static void endRun(long whEtlExecId, JobStatus status, String message) {
    Map<String, Object> params = new HashMap<>();
    params.put("whEtlExecId", whEtlExecId);
    params.put("status", status.toString());
    params.put("endTime", System.currentTimeMillis() / 1000);
    params.put("message", message);
    JdbcUtil.wherehowsNamedJdbcTemplate.update(END_RUN, params);
  }

  public static void updateJobProcessInfo(long whEtlExecId, int processId, String hostname) throws DataAccessException {
    JdbcUtil.wherehowsJdbcTemplate.update(UPDATE_JOB_PROCESS_ID_AND_HOSTNAME, processId, hostname, whEtlExecId);
  }
}
