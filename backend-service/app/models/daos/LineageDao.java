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
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import models.utils.Urn;
import utils.JdbcUtil;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.PartitionPatternMatcher;
import wherehows.common.writers.DatabaseWriter;


/**
 * Find jobs that use the urn as a source in recent 'period' days. Only find Azkaban jobs.
 * Created by zsun on 4/5/15.
 * Modified by zechen on 10/12/15.
 */
public class LineageDao {
  public static final String FIND_JOBS_BY_DATASET =
    " select distinct ca.short_connection_string, f.flow_group, f.flow_name, jedl.job_name "
    + " from job_execution_data_lineage jedl "
    + " join flow_execution fe on jedl.app_id = fe.app_id and jedl.flow_exec_id = fe.flow_exec_id "
    + " join flow f on fe.app_id = f.app_id and fe.flow_id = f.flow_id "
    + " join cfg_application ca on ca.app_id = jedl.app_id "
    + " join cfg_database cd on cd.db_id = jedl.db_id "
    + " where source_target_type = :source_target_type "
    + " and jedl.abstracted_object_name = :abstracted_object_name "
    + " and jedl.job_finished_unixtime > UNIX_TIMESTAMP(subdate(current_date, :period)) "
    + " and ca.short_connection_string like :instance "
    + " and cd.short_connection_string like :cluster ";

  public static final String FIND_DATASETS_BY_JOB =
    " select distinct jedl.abstracted_object_name, jedl.db_id, jedl.partition_start, jedl.partition_end, "
      + " jedl.storage_type, jedl.record_count, jedl.insert_count, jedl.update_count, jedl.delete_count "
      + " from job_execution_data_lineage jedl join "
      + " (select jedl.app_id, max(jedl.job_exec_id) job_exec_id from job_execution_data_lineage jedl "
      + " where jedl.job_name = :job_name "
      + " and jedl.flow_path = :flow_path "
      + " group by app_id) a "
      + " on jedl.app_id = a.app_id and jedl.job_exec_id = a.job_exec_id "
      + " join cfg_application ca on ca.app_id = jedl.app_id "
      + " where jedl.source_target_type = :source_target_type "
      + " and ca.short_connection_string like :instance ";


  public static final String FIND_DATASETS_BY_FLOW_EXEC =
    " select distinct jedl.abstracted_object_name, jedl.db_id, jedl.partition_start, jedl.partition_end, "
      + " jedl.storage_type, jedl.record_count, jedl.insert_count, jedl.update_count, jedl.delete_count "
      + " from job_execution_data_lineage jedl "
      + " join cfg_application ca on ca.app_id = jedl.app_id "
      + " where jedl.job_name = :job_name "
      + " and jedl.flow_exec_id = :flow_exec_id "
      + " and jedl.source_target_type = :source_target_type "
      + " and ca.short_connection_string like :instance ";


  public static final String FIND_DATASETS_BY_JOB_EXEC =
    " select distinct jedl.abstracted_object_name, jedl.db_id, jedl.partition_start, jedl.partition_end, "
      + " jedl.storage_type, jedl.record_count, jedl.insert_count, jedl.update_count, jedl.delete_count "
      + " from job_execution_data_lineage jedl "
      + " join cfg_application ca on ca.app_id = jedl.app_id "
      + " where jedl.job_exec_id = :job_exec_id "
      + " and jedl.source_target_type = :source_target_type "
      + " and ca.short_connection_string like :instance ";


  public static List<Map<String, Object>> getJobsByDataset(String urn, String period, String cluster, String instance, String sourceTargetType)
    throws SQLException {
    Urn u = new Urn(urn);
    Map<String, Object> params = new HashMap<>();
    params.put("abstracted_object_name", u.abstractObjectName);
    params.put("period", period);
    params.put("source_target_type", sourceTargetType);

    if (cluster == null || cluster.isEmpty()) {
      params.put("cluster", "%");
    } else {
      params.put("cluster", cluster);
    }
    if (instance == null || instance.isEmpty()) {
      params.put("instance", "%");
    } else {
      params.put("instance", instance);
    }
    List<Map<String, Object>> jobs = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_JOBS_BY_DATASET, params);
    return jobs;
  }

  public static List<Map<String, Object>> getDatasetsByJob(String flowPath, String jobName, String instance, String sourceTargetType) {
    Map<String, Object> params = new HashMap<>();
    params.put("flow_path", flowPath);
    params.put("job_name", jobName);
    params.put("source_target_type", sourceTargetType);
    if (instance == null || instance.isEmpty()) {
      params.put("instance", "%");
    } else {
      params.put("instance", instance);
    }
    List<Map<String, Object>> datasets = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_DATASETS_BY_JOB, params);
    return datasets;
  }

  public static List<Map<String, Object>> getDatasetsByFlowExec(Long flowExecId, String jobName, String instance, String sourceTargetType) {
    Map<String, Object> params = new HashMap<>();
    params.put("flow_exec_id", flowExecId);
    params.put("job_name", jobName);
    params.put("source_target_type", sourceTargetType);
    if (instance == null || instance.isEmpty()) {
      params.put("instance", "%");
    } else {
      params.put("instance", instance);
    }
    List<Map<String, Object>> datasets = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_DATASETS_BY_FLOW_EXEC, params);
    return datasets;
  }

  public static List<Map<String, Object>> getDatasetsByJobExec(Long jobExecId, String instance, String sourceTargetType) {
    Map<String, Object> params = new HashMap<>();
    params.put("job_exec_id", jobExecId);
    params.put("source_target_type", sourceTargetType);
    if (instance == null || instance.isEmpty()) {
      params.put("instance", "%");
    } else {
      params.put("instance", instance);
    }
    List<Map<String, Object>> datasets = JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_DATASETS_BY_JOB_EXEC, params);
    return datasets;
  }

  public static void insertLineage(JsonNode lineage) throws Exception {
    TreeMap<String, LineageRecord> records = new TreeMap<>();
    Map<String, String> refSourceMap = new HashMap<>();
    Integer appId = lineage.findPath("app_id").asInt();
    String appName = lineage.findPath("app_name").asText();
    // Set application id if app id is not set or equals to 0
    if (appId == 0) {
      appId = (Integer) CfgDao.getAppByName(appName).get("id");
    }

    Long flowExecId = lineage.findPath("flow_exec_id").asLong();
    Long jobExecId = lineage.findPath("job_exec_id").asLong();
    String jobExecUuid = lineage.findPath("job_exec_uuid").asText();
    String jobName = lineage.findPath("job_name").asText();
    Integer jobStartTime = lineage.findPath("job_start_unixtime").asInt();
    Integer jobEndTime = lineage.findPath("job_end_unixtime").asInt();
    String flowPath = lineage.findPath("flow_path").asText();

    JsonNode nodes = lineage.findPath("lineages");
    if (nodes.isArray()) {
      PartitionPatternMatcher ppm = null;
      for (JsonNode node : nodes) {
        Integer databaseId = node.findPath("db_id").asInt();
        String databaseName = node.findPath("database_name").asText();
        // Set application id if app id is not set or equals to 0
        if (databaseId == 0) {
          databaseId = (Integer) CfgDao.getDbByName(databaseName).get("id");
        }

        String abstractedObjectName = node.findPath("abstracted_object_name").asText();
        String fullObjectName = node.findPath("full_object_name").asText();
        String storageType = node.findPath("storage_type").asText();
        String partitionStart = node.findPath("partition_start").asText();
        String partitionEnd = node.findPath("partition_end").asText();
        String partitionType = node.findPath("partition_type").asText();
        Integer layoutId = node.findPath("layout_id").asInt();
        // Get layout id if layout id is not set or equals to 0
        if (layoutId == 0) {
          if (ppm == null) {
            ppm = new PartitionPatternMatcher(PartitionLayoutDao.getPartitionLayouts());
          }
          layoutId = ppm.analyze(fullObjectName);
        }

        String sourceTargetType = node.findPath("source_target_type").textValue();
        String operation = node.findPath("operation").textValue();
        Long recordCount = node.findPath("record_count").longValue();
        Long insertCount = node.findPath("insert_count").longValue();
        Long deleteCount = node.findPath("delete_count").longValue();
        Long updateCount = node.findPath("update_count").longValue();

        LineageRecord record = new LineageRecord(appId, flowExecId, jobName, jobExecId);
        record.setJobExecUUID(jobExecUuid);
        record.setJobStartTime(jobStartTime);
        record.setJobEndTime(jobEndTime);
        record.setFlowPath(flowPath);
        record.setDatabaseId(databaseId);
        record.setAbstractObjectName(abstractedObjectName);
        record.setFullObjectName(fullObjectName);
        record.setStorageType(storageType);
        record.setPartitionStart(partitionStart);
        record.setPartitionEnd(partitionEnd);
        record.setPartitionType(partitionType);
        record.setLayoutId(layoutId);
        record.setSourceTargetType(sourceTargetType);
        record.setOperation(operation);
        record.setRecordCount(recordCount);
        record.setInsertCount(insertCount);
        record.setDeleteCount(deleteCount);
        record.setUpdateCount(updateCount);
        records.put(record.getLineageRecordKey(), record);

        JsonNode sourceName = node.findPath("ref_source_object_name");
        JsonNode sourceDb = node.findPath("ref_source_db_id");
        if (sourceTargetType.equals("target") &&
          !sourceName.isMissingNode() && !sourceDb.isMissingNode()) {
          refSourceMap.put(record.getLineageRecordKey(), "source-" + sourceDb.intValue() + "-" + sourceName.textValue());
        }
      }

      int srlNo = 0;
      for (LineageRecord r : records.values()) {
        r.setSrlNo(srlNo++);
      }

      for (String target : refSourceMap.keySet()) {
        String sourceKey = refSourceMap.get(target);
        if (records.containsKey(sourceKey)) {
          LineageRecord r = records.get(target);
          r.setRelatedSrlNo(records.get(sourceKey).getSrlNo());
        }
      }
    }

    DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, "job_execution_data_lineage");
    try {
      for (LineageRecord record : records.values()) {
        dw.append(record);
      }
      dw.flush();
    } catch (IOException | SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        dw.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

  }
}
