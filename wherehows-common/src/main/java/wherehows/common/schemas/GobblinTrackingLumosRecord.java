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
package wherehows.common.schemas;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


/**
 * Data record model for Gobblin tracking event lumos
 *
 */
public class GobblinTrackingLumosRecord extends AbstractRecord {

  String cluster;
  String dataset;
  String location;
  String partitionType;
  String partitionName;
  String subpartitionType;
  String subpartitionName;
  long maxDataDateEpoch3;
  long maxDataKey;
  long recordCount;
  String sourceDatacenter;
  String sourceDeploymentEnv;
  String sourceDatabase;
  String sourceTable;
  String jobContext;
  String projectName;
  String flowName;
  String jobName;
  int flowExecId;
  long logEventTime;

  @Override
  public String[] getDbColumnNames() {
    final String[] columnNames = {"cluster", "dataset", "location", "partition_type", "partition_name",
        "subpartition_type", "subpartition_name", "max_data_date_epoch3", "max_data_key", "record_count",
        "source_datacenter", "source_deployment_env", "source_database", "source_table",
        "job_context", "project_name", "flow_name", "job_name", "flow_exec_id", "log_event_time"};
    return columnNames;
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public GobblinTrackingLumosRecord() {
  }

  public GobblinTrackingLumosRecord(long timestamp, String cluster, String jobContext,
      String projectName, String flowId, String jobId, int execId) {
    this.logEventTime = timestamp;
    this.cluster = cluster;
    this.jobContext = jobContext;
    this.projectName = projectName;
    this.flowName = flowId;
    this.jobName = jobId;
    this.flowExecId = execId;
  }

  public void setDatasetUrn(String dataset, String location, String partitionType, String partitionName,
      String subpartitionType, String subpartitionName) {
    this.dataset = dataset;
    this.location = location;
    this.partitionType = partitionType;
    this.partitionName = partitionName;
    this.subpartitionType = subpartitionType;
    this.subpartitionName = subpartitionName;
  }

  public void setMaxDataDate(Long maxDataDateEpoch3, Long maxDataKey) {
    this.maxDataDateEpoch3 = maxDataDateEpoch3;
    this.maxDataKey = maxDataKey;
  }

  public void setSource(String sourceDatacenter, String sourceDeploymentEnv,
      String sourceDatabase, String sourceTable) {
    this.sourceDatacenter = sourceDatacenter;
    this.sourceDeploymentEnv = sourceDeploymentEnv;
    this.sourceDatabase = sourceDatabase;
    this.sourceTable = sourceTable;
  }

  public String getCluster() {
    return cluster;
  }

  public void setCluster(String cluster) {
    this.cluster = cluster;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public String getPartitionType() {
    return partitionType;
  }

  public void setPartitionType(String partitionType) {
    this.partitionType = partitionType;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public String getSubpartitionType() {
    return subpartitionType;
  }

  public void setSubpartitionType(String subpartitionType) {
    this.subpartitionType = subpartitionType;
  }

  public String getSubpartitionName() {
    return subpartitionName;
  }

  public void setSubpartitionName(String subpartitionName) {
    this.subpartitionName = subpartitionName;
  }

  public Long getMaxDataDateEpoch3() {
    return maxDataDateEpoch3;
  }

  public void setMaxDataDateEpoch3(Long maxDataDateEpoch3) {
    this.maxDataDateEpoch3 = maxDataDateEpoch3;
  }

  public Long getMaxDataKey() {
    return maxDataKey;
  }

  public void setMaxDataKey(Long maxDataKey) {
    this.maxDataKey = maxDataKey;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  public String getSourceDatacenter() {
    return sourceDatacenter;
  }

  public void setSourceDatacenter(String sourceDatacenter) {
    this.sourceDatacenter = sourceDatacenter;
  }

  public String getSourceDeploymentEnv() {
    return sourceDeploymentEnv;
  }

  public void setSourceDeploymentEnv(String sourceDeploymentEnv) {
    this.sourceDeploymentEnv = sourceDeploymentEnv;
  }

  public String getSourceDatabase() {
    return sourceDatabase;
  }

  public void setSourceDatabase(String sourceDatabase) {
    this.sourceDatabase = sourceDatabase;
  }

  public String getSourceTable() {
    return sourceTable;
  }

  public void setSourceTable(String sourceTable) {
    this.sourceTable = sourceTable;
  }

  public String getJobContext() {
    return jobContext;
  }

  public void setJobContext(String jobContext) {
    this.jobContext = jobContext;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public int getFlowExecId() {
    return flowExecId;
  }

  public void setFlowExecId(int flowExecId) {
    this.flowExecId = flowExecId;
  }

  public long getLogEventTime() {
    return logEventTime;
  }

  public void setLogEventTime(long logEventTime) {
    this.logEventTime = logEventTime;
  }
}
