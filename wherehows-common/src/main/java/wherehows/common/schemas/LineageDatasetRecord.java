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
import java.util.Map;


public class LineageDatasetRecord extends AbstractRecord {

  Integer datasetId;
  DatasetIdentifier datasetIdentifier;
  String datasetUrn;
  DatasetPropertyRecord datasetProperties;
  DatasetPartitionRangeRecord partition;
  String[] qualifiedValues;
  LineageDatasetMapRecord detailLineageMap;
  String operation;
  String sourceTargetType;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public LineageDatasetRecord() {
  }

  public Integer getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(Integer datasetId) {
    this.datasetId = datasetId;
  }

  public DatasetIdentifier getDatasetIdentifier() {
    return datasetIdentifier;
  }

  public void setDatasetIdentifier(DatasetIdentifier datasetIdentifier) {
    this.datasetIdentifier = datasetIdentifier;
  }

  public String getDatasetUrn() {
    return datasetUrn;
  }

  public void setDatasetUrn(String datasetUrn) {
    this.datasetUrn = datasetUrn;
  }

  public DatasetPropertyRecord getDatasetProperties() {
    return datasetProperties;
  }

  public void setDatasetProperties(DatasetPropertyRecord datasetProperties) {
    this.datasetProperties = datasetProperties;
  }

  public DatasetPartitionRangeRecord getPartition() {
    return partition;
  }

  public void setPartition(DatasetPartitionRangeRecord partition) {
    this.partition = partition;
  }

  public String[] getQualifiedValues() {
    return qualifiedValues;
  }

  public void setQualifiedValues(String[] qualifiedValues) {
    this.qualifiedValues = qualifiedValues;
  }

  public LineageDatasetMapRecord getDetailLineageMap() {
    return detailLineageMap;
  }

  public void setDetailLineageMap(LineageDatasetMapRecord detailLineageMap) {
    this.detailLineageMap = detailLineageMap;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getSourceTargetType() {
    return sourceTargetType;
  }

  public void setSourceTargetType(String sourceTargetType) {
    this.sourceTargetType = sourceTargetType;
  }
}
