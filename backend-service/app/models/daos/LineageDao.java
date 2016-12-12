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
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.dao.DataAccessException;
import play.Logger;
import utils.Urn;
import utils.JdbcUtil;
import wherehows.common.DatasetPath;
import wherehows.common.LineageCombiner;
import wherehows.common.schemas.ApplicationRecord;
import wherehows.common.schemas.JobExecutionRecord;
import wherehows.common.schemas.LineageDatasetRecord;
import wherehows.common.schemas.LineageRecord;
import wherehows.common.utils.PartitionPatternMatcher;
import wherehows.common.utils.PreparedStatementUtil;
import wherehows.common.writers.DatabaseWriter;


/**
 * Find jobs that use the urn as a source in recent 'period' days. Only find Azkaban jobs.
 * Created by zsun on 4/5/15.
 * Modified by zechen on 10/12/15.
 */
public class LineageDao {

  private static final String JOB_EXECUTION_TABLE = "job_execution";
  private static final String JOB_EXECUTION_DATA_LINEAGE_TABLE = "job_execution_data_lineage";

  private static final DatabaseWriter JOB_EXECUTION_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, JOB_EXECUTION_TABLE);

  private static final DatabaseWriter JOB_EXECUTION_DATA_LINEAGE_WRITER =
      new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, JOB_EXECUTION_DATA_LINEAGE_TABLE);

  public static final String FIND_JOBS_BY_DATASET =
    " select distinct ca.short_connection_string, jedl.job_name, jedl.flow_path "
    + " from job_execution_data_lineage jedl "
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

  public static final String INSERT_JOB_EXECUTION_RECORD =
      PreparedStatementUtil.prepareInsertTemplateWithColumn("REPLACE", JOB_EXECUTION_TABLE,
          JobExecutionRecord.dbColumns());

  public static final String INSERT_JOB_EXECUTION_DATA_LINEAGE_RECORD =
      PreparedStatementUtil.prepareInsertTemplateWithColumn("REPLACE", JOB_EXECUTION_DATA_LINEAGE_TABLE,
          LineageRecord.dbColumns());

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
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_JOBS_BY_DATASET, params);
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
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_DATASETS_BY_JOB, params);
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
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_DATASETS_BY_FLOW_EXEC, params);
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
    return JdbcUtil.wherehowsNamedJdbcTemplate.queryForList(FIND_DATASETS_BY_JOB_EXEC, params);
  }

  public static void insertLineage(JsonNode lineage) throws Exception {
    TreeMap<String, LineageRecord> records = new TreeMap<>();
    Map<String, String> refSourceMap = new HashMap<>();
    Integer appId = lineage.findPath("app_id").asInt();
    String appName = lineage.findPath("app_name").asText();
    // Set application id if app id is not set or equals to 0
    if (appId == 0) {
      appId = (Integer) CfgDao.getAppByName(appName).get("app_id");
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
          databaseId = (Integer) CfgDao.getDbByName(databaseName).get("db_id");
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

  public static void updateJobExecutionLineage(JsonNode root)
      throws Exception {
    final ObjectMapper om = new ObjectMapper();
    // get Application
    final JsonNode application = root.path("application");
    final JsonNode jobExecution = root.path("jobExecution");
    final JsonNode inputDatasetList = root.path("inputDatasetList");
    final JsonNode outputDatasetList = root.path("outputDatasetList");
    if (application.isMissingNode() || jobExecution.isMissingNode() || !inputDatasetList.isArray()
        || !outputDatasetList.isArray()) {
      throw new IllegalArgumentException(
          "Job Execution Lineage info update error, missing necessary fields: " + root.toString());
    }

    ApplicationRecord appRecord = om.convertValue(application, ApplicationRecord.class);
    // match app id from cfg_application
    Integer appId;
    try {
      appId = (Integer) CfgDao.getAppByAppCode(appRecord.getName()).get("app_id");
    } catch (Exception ex) {
      Logger.error("Can't find application by app_code: " + application.toString(), ex);
      throw ex;
    }

    // process job execution info
    JobExecutionRecord jobExecRecord = om.convertValue(jobExecution, JobExecutionRecord.class);
    // TODO generate flow_id, job_id if not provided
    jobExecRecord.setAppId(appId);
    jobExecRecord.setLogTime(System.currentTimeMillis());

    JOB_EXECUTION_WRITER.execute(INSERT_JOB_EXECUTION_RECORD, jobExecRecord.dbValues());

    // process job data lineage info
    final List<LineageRecord> lineageRecords = new ArrayList<>();

    for (final JsonNode inputDataset : inputDatasetList) {
      LineageDatasetRecord lineageDataset = om.convertValue(inputDataset, LineageDatasetRecord.class);
      lineageDataset.setSourceTargetType("source");

      LineageRecord record = convertLineageDataset(lineageDataset, jobExecRecord);
      if (record != null) {
        lineageRecords.add(record);
      }
    }

    for (final JsonNode outputDataset : outputDatasetList) {
      LineageDatasetRecord lineageDataset = om.convertValue(outputDataset, LineageDatasetRecord.class);
      lineageDataset.setSourceTargetType("target");

      LineageRecord record = convertLineageDataset(lineageDataset, jobExecRecord);
      if (record != null) {
        lineageRecords.add(record);
      }
    }

    // combine partitions
    final LineageCombiner lineageCombiner = new LineageCombiner(null);
    List<LineageRecord> combinedLineageRecords;
    try {
      lineageCombiner.addAllWoPartitionUpdate(lineageRecords);
      combinedLineageRecords = lineageCombiner.getCombinedLineage();
    } catch (Exception ex) {
      Logger.error("Lineage records combine error: ", ex);
      throw ex;
    }

    // generate srl_no
    int srlNumber = 0;
    for (LineageRecord record : combinedLineageRecords) {
      record.setSrlNo(srlNumber);
      srlNumber++;
    }

    // store data lineage info
    for (LineageRecord record : combinedLineageRecords) {
      try {
        JOB_EXECUTION_DATA_LINEAGE_WRITER.execute(INSERT_JOB_EXECUTION_DATA_LINEAGE_RECORD, record.dbValues());
      } catch (DataAccessException ex) {
        Logger.error("Data Lineage input error: ", ex);
      }
    }
  }

  // convert LineageDatasetRecord and JobExecutionRecord into LineageRecord
  private static LineageRecord convertLineageDataset(LineageDatasetRecord lineageDataset, JobExecutionRecord jobExec)
      throws Exception {
    final LineageRecord record = new LineageRecord(jobExec.getAppId(), jobExec.getFlowExecutionId(), jobExec.getName(),
        jobExec.getExecutionId());

    record.setFlowPath(jobExec.getTopLevelFlowName());
    record.setJobExecUUID(jobExec.getExecutionGuid());
    record.setSourceTargetType(lineageDataset.getSourceTargetType());
    record.setOperation(lineageDataset.getOperation());
    record.setJobStartTime((int) (jobExec.getStartTime() / 1000));
    record.setJobEndTime((int) (jobExec.getEndTime() / 1000));

    if (lineageDataset.getPartition() != null) {
      record.setPartitionStart(lineageDataset.getPartition().getMinPartitionValue());
      record.setPartitionEnd(lineageDataset.getPartition().getMaxPartitionValue());
      record.setPartitionType(lineageDataset.getPartition().getPartitionType());
    }

    if (lineageDataset.getDatasetUrn() != null) {
      record.setFullObjectName(lineageDataset.getDatasetUrn());
    } else if (lineageDataset.getDatasetProperties() != null
        && lineageDataset.getDatasetProperties().getUri() != null) {
      record.setFullObjectName(lineageDataset.getDatasetProperties().getUri());
    }
    if (record.getFullObjectName() != null) {
      List<String> abstractPaths = DatasetPath.separatedDataset(record.getFullObjectName());
      if (abstractPaths.size() > 0) {
        record.setAbstractObjectName(abstractPaths.get(0));
      }
    }

    return record;
  }
}
