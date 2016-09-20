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


public class DatasetSecurityRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  Map<String, Integer> classification;
  String recordOwnerType;
  List<String> recordOwner;
  String complianceType;
  DatasetRetentionRecord retentionPolicy;
  DatasetGeographicAffinityRecord geographicAffinity;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "classification", "record_owner_type", "record_owner",
        "compliance_type", "retention_policy", "geographic_affinity", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetSecurityRecord() {
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

  public Map<String, Integer> getClassification() {
    return classification;
  }

  public void setClassification(Map<String, Integer> classification) {
    this.classification = classification;
  }

  public String getRecordOwnerType() {
    return recordOwnerType;
  }

  public void setRecordOwnerType(String recordOwnerType) {
    this.recordOwnerType = recordOwnerType;
  }

  public List<String> getRecordOwner() {
    return recordOwner;
  }

  public void setRecordOwner(List<String> recordOwner) {
    this.recordOwner = recordOwner;
  }

  public String getComplianceType() {
    return complianceType;
  }

  public void setComplianceType(String complianceType) {
    this.complianceType = complianceType;
  }

  public DatasetRetentionRecord getRetentionPolicy() {
    return retentionPolicy;
  }

  public void setRetentionPolicy(DatasetRetentionRecord retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
  }

  public DatasetGeographicAffinityRecord getGeographicAffinity() {
    return geographicAffinity;
  }

  public void setGeographicAffinity(DatasetGeographicAffinityRecord geographicAffinity) {
    this.geographicAffinity = geographicAffinity;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
