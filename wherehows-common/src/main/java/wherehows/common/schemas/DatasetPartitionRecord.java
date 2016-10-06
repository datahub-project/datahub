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

import java.util.List;


public class DatasetPartitionRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  Integer totalPartitionLevel;
  String partitionSpecText;
  Boolean hasTimePartition;
  Boolean hasHashPartition;
  List<DatasetPartitionKeyRecord> partitionKeys;
  String timePartitionExpression;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "total_partition_level", "partition_spec_text", "has_time_partition",
        "has_hash_partition", "partition_keys", "time_partition_expression", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetPartitionRecord() {
  }

  public Integer getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(Integer datasetId) {
    this.datasetId = datasetId;
  }

  public String getDatasetUrn() {
    return datasetUrn;
  }

  public void setDatasetUrn(String datasetUrn) {
    this.datasetUrn = datasetUrn;
  }

  public Integer getTotalPartitionLevel() {
    return totalPartitionLevel;
  }

  public void setTotalPartitionLevel(Integer totalPartitionLevel) {
    this.totalPartitionLevel = totalPartitionLevel;
  }

  public String getPartitionSpecText() {
    return partitionSpecText;
  }

  public void setPartitionSpecText(String partitionSpecText) {
    this.partitionSpecText = partitionSpecText;
  }

  public Boolean getHasTimePartition() {
    return hasTimePartition;
  }

  public void setHasTimePartition(Boolean hasTimePartition) {
    this.hasTimePartition = hasTimePartition;
  }

  public Boolean getHasHashPartition() {
    return hasHashPartition;
  }

  public void setHasHashPartition(Boolean hasHashPartition) {
    this.hasHashPartition = hasHashPartition;
  }

  public List<DatasetPartitionKeyRecord> getPartitionKeys() {
    return partitionKeys;
  }

  public void setPartitionKeys(List<DatasetPartitionKeyRecord> partitionKeys) {
    this.partitionKeys = partitionKeys;
  }

  public String getTimePartitionExpression() {
    return timePartitionExpression;
  }

  public void setTimePartitionExpression(String timePartitionExpression) {
    this.timePartitionExpression = timePartitionExpression;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
