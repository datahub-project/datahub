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

import wherehows.common.DatasetPath;
import wherehows.common.utils.StringUtil;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by zsun on 8/20/15.
 */
public class LineageRecord implements Record, Comparable<LineageRecord> {

  Integer appId;
  String appName;
  Long flowExecId;
  Long jobExecId;
  String jobExecUUID;
  String jobName;
  Integer jobStartTime;
  Integer jobEndTime;
  Integer databaseId;
  String databaseName;
  String abstractObjectName;
  String fullObjectName;
  String partitionStart;
  String partitionEnd;
  String partitionType;
  Integer layoutId;
  String storageType;
  String sourceTargetType;
  Integer srlNo;
  Integer relatedSrlNo;
  String operation;
  Long recordCount;
  Long insertCount;
  Long deleteCount;
  Long updateCount;
  String flowPath;
  char SEPR = 0x001A;
  List<Object> allFields;

  public LineageRecord(Integer appId, Long flowExecId, String jobName, Long jobExecId) {
    this.appId = appId;
    this.flowExecId = flowExecId;
    this.jobName = jobName;
    this.jobExecId = jobExecId;
  }

  public String getFullObjectName() {
    return fullObjectName;
  }

  /**
   * Set the dataset raw info
   * @param databaseId
   * @param fullObjectName
   * @param storageType
   */
  public void setDatasetInfo(Integer databaseId, String fullObjectName, String storageType) {
    this.databaseId = databaseId;
    this.fullObjectName = fullObjectName;
    this.storageType = storageType;
  }

  public void setOperationInfo(String sourceTargetType, String operation, Long recordCount, Long insertCount,
    Long deleteCount, Long updateCount, Integer jobStartTime, Integer jobEndTime, String flowPath) {
    this.sourceTargetType = sourceTargetType;
    this.operation = operation;
    this.recordCount = recordCount;
    this.insertCount = insertCount;
    this.deleteCount = deleteCount;
    this.updateCount = updateCount;
    this.jobStartTime = jobStartTime;
    this.jobEndTime = jobEndTime;
    this.flowPath = flowPath;
  }

  @Override
  public String toCsvString() {
    return null;
  }

  public String toDatabaseValue() {
    allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowExecId);
    allFields.add(jobExecId);
    allFields.add(jobExecUUID);
    allFields.add(jobName);
    allFields.add(jobStartTime);
    allFields.add(jobEndTime);
    allFields.add(databaseId);
    allFields.add(abstractObjectName);
    allFields.add(fullObjectName);
    allFields.add(partitionStart);
    allFields.add(partitionEnd);
    allFields.add(partitionType);
    allFields.add(layoutId);
    allFields.add(storageType);
    allFields.add(sourceTargetType);
    allFields.add(srlNo);
    allFields.add(relatedSrlNo);
    allFields.add(operation);
    allFields.add(recordCount);
    allFields.add(insertCount);
    allFields.add(deleteCount);
    allFields.add(updateCount);
    allFields.add(flowPath);

    // add the created_date and wh_etl_exec_id
    allFields.add(System.currentTimeMillis() / 1000);
    allFields.add(null);
    StringBuilder sb = new StringBuilder();
    for (Object o : allFields) {
      sb.append(StringUtil.toDbString(o));
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public static String[] dbColumns() {
    return new String[]{"app_id", "flow_exec_id", "job_exec_id", "job_exec_uuid", "flow_path", "job_name",
        "job_start_unixtime", "job_finished_unixtime", "abstracted_object_name", "full_object_name", "partition_start",
        "partition_end", "partition_type", "layout_id", "storage_type", "source_target_type",
        "srl_no", "source_srl_no", "operation", "created_date"};
  }

  public Object[] dbValues() {
    return new Object[]{appId, flowExecId, jobExecId, jobExecUUID, flowPath, jobName, jobStartTime, jobEndTime,
        abstractObjectName, fullObjectName, partitionStart, partitionEnd, partitionType, layoutId, storageType,
        sourceTargetType, srlNo, relatedSrlNo, operation, System.currentTimeMillis() / 1000};
  }

  /**
   * After analyze, the path need to update to abstract format
   * @param datasetPath
   */
  public void updateDataset(DatasetPath datasetPath) {
    this.abstractObjectName = datasetPath.abstractPath;
    this.partitionStart = datasetPath.partitionStart;
    this.partitionEnd = datasetPath.partitionEnd;
    this.partitionType = datasetPath.partitionType;
    this.layoutId = datasetPath.layoutId;
  }

  public String getLineageRecordKey() {
    return this.sourceTargetType + '-' + this.databaseId + '-' + this.abstractObjectName;
  }

  public void merge(LineageRecord theOtherlr) {
    assert (theOtherlr.abstractObjectName
      .equals(this.abstractObjectName)); // abstract path must be the same, otherwise can't merge
    if (this.partitionStart != null && theOtherlr.partitionStart != null) {
      this.partitionStart = ((this.partitionStart.compareTo(theOtherlr.partitionStart)) < 0) ? this.partitionStart
        : theOtherlr.partitionStart;
    } else {
      this.partitionStart = (this.partitionStart == null) ? theOtherlr.partitionStart : this.partitionStart;
    }

    if (this.partitionEnd != null && theOtherlr.partitionEnd != null) {
      this.partitionEnd =
        ((this.partitionEnd.compareTo(theOtherlr.partitionEnd)) > 0) ? this.partitionEnd : theOtherlr.partitionEnd;
    } else {
      this.partitionEnd = (this.partitionEnd == null) ? theOtherlr.partitionEnd : this.partitionEnd;
    }
  }

  @Override
  public int compareTo(LineageRecord o) {
    // used when need to compare to get the srl_no in one job's lineage
    return this.getLineageRecordKey().compareTo(o.getLineageRecordKey());
  }

  public void setAppId(Integer appId) {
    this.appId = appId;
  }

  public void setFlowExecId(Long flowExecId) {
    this.flowExecId = flowExecId;
  }

  public void setJobExecId(Long jobExecId) {
    this.jobExecId = jobExecId;
  }

  public void setJobExecUUID(String jobExecUUID) {
    this.jobExecUUID = jobExecUUID;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public void setJobStartTime(Integer jobStartTime) {
    this.jobStartTime = jobStartTime;
  }

  public void setJobEndTime(Integer jobEndTime) {
    this.jobEndTime = jobEndTime;
  }

  public void setDatabaseId(Integer databaseId) {
    this.databaseId = databaseId;
  }

  public void setAbstractObjectName(String abstractObjectName) {
    this.abstractObjectName = abstractObjectName;
  }

  public void setFullObjectName(String fullObjectName) {
    this.fullObjectName = fullObjectName;
  }

  public String getPartitionStart() {
    return partitionStart;
  }

  public void setPartitionStart(String partitionStart) {
    this.partitionStart = partitionStart;
  }

  public String getPartitionEnd() {
    return partitionEnd;
  }

  public void setPartitionEnd(String partitionEnd) {
    this.partitionEnd = partitionEnd;
  }

  public String getPartitionType() {
    return partitionType;
  }

  public void setPartitionType(String partitionType) {
    this.partitionType = partitionType;
  }

  public void setLayoutId(Integer layoutId) {
    this.layoutId = layoutId;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public void setSourceTargetType(String sourceTargetType) {
    this.sourceTargetType = sourceTargetType;
  }

  public Integer getSrlNo() {
    return srlNo;
  }

  public void setSrlNo(Integer srlNo) {
    this.srlNo = srlNo;
  }

  public void setRelatedSrlNo(Integer relatedSrlNo) {
    this.relatedSrlNo = relatedSrlNo;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setRecordCount(Long recordCount) {
    this.recordCount = recordCount;
  }

  public void setInsertCount(Long insertCount) {
    this.insertCount = insertCount;
  }

  public void setDeleteCount(Long deleteCount) {
    this.deleteCount = deleteCount;
  }

  public void setUpdateCount(Long updateCount) {
    this.updateCount = updateCount;
  }

  public void setFlowPath(String flowPath) {
    this.flowPath = flowPath;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }
}
