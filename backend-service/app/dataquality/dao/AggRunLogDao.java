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
package dataquality.dao;

import dataquality.models.TimeRange;
import dataquality.models.enums.JobStatus;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.support.KeyHolder;
import utils.JdbcUtil;


/**
 * Created by zechen on 12/23/15.
 */
public class AggRunLogDao extends AbstractDao {

  public static final String INSERT_RUN_LOG = "INSERT INTO dq_agg_log(agg_def_id, status, request_time, data_time) VALUES (:aggDefId, :status, :requestTime, :dataTime)";

  public static final String UPDATE_PARTITION = " update dq_agg_log set partition_start = :minPartiton, partition_end = :maxPartition where run_id = :runId";

  public static final String UPDATE_RUN_STATUS = " update dq_agg_log set status = :status, error_msg = :message where run_id = :runId";

  public final static String LAST_TWO_RUN =
      "select distinct data_time, partition_start, partition_end from dq_agg_log where agg_def_id = :aggDefId order by data_time desc limit 2";

  public static Long insertNewAggRunLog(Integer aggDefId, Timestamp requestTime, Timestamp dataTime) {
    Map<String, Object> params = new HashMap<>();
    params.put("aggDefId", aggDefId);
    params.put("requestTime", requestTime);
    params.put("dataTime", dataTime);
    params.put("status", JobStatus.REQUESTED.toString());
    KeyHolder keyHolder = JdbcUtil.insertRow(INSERT_RUN_LOG, params, dqNamedJdbc);
    return keyHolder.getKey().longValue();
  }


  public static void updatePartitions(Timestamp maxPartition, Timestamp minPartition, Long runId) {
    Map<String, Object> params = new HashMap<>();
    params.put("maxPartition", maxPartition);
    params.put("minPartition", minPartition);
    params.put("runId", runId);
    dqNamedJdbc.update(UPDATE_PARTITION, params);
  }

  public static void updateRunStatus(JobStatus status, String message, Long runId) {
    Map<String, Object> params = new HashMap<>();
    params.put("status", status.toString());
    params.put("runId", runId);
    params.put("message", message);
    dqNamedJdbc.update(UPDATE_RUN_STATUS, params);
  }

  public static List<TimeRange> getLastTwoRuns(int aggDefId) {
    Map<String, Object> params = new HashMap<>();
    params.put("aggDefId", aggDefId);
    List<Map<String, Object>> runs = dqNamedJdbc.queryForList(LAST_TWO_RUN, params);
    List<TimeRange> ret = new ArrayList<>();
    for (Map<String, Object> run : runs) {
      Timestamp start, end;
      if (run.get("partition_start") != null) {
        start = (Timestamp) run.get("partition_start");
        end = (Timestamp) run.get("partition_end");
      } else {
        start = (Timestamp) run.get("data_time");
        end = (Timestamp) run.get("data_time");
      }
      ret.add(new TimeRange(start, end));
    }
    return ret;
  }

}
