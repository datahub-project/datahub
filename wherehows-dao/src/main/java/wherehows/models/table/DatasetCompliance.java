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
package wherehows.models.table;

import java.util.List;
import java.util.Map;


public class DatasetCompliance {

  private Integer datasetId;
  private String datasetUrn;
  private String complianceType;
  private List<DatasetFieldEntity> complianceEntities;
  private String confidentiality;
  private Map<String, Boolean> datasetClassification;
  private Map<String, String> fieldClassification;
  private String recordOwnerType;
  private Map<String, Object> retentionPolicy;
  private Map<String, Object> geographicAffinity;
  private String modifiedBy;
  private Long modifiedTime;

  public DatasetCompliance() {
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

  public String getComplianceType() {
    return complianceType;
  }

  public void setComplianceType(String complianceType) {
    this.complianceType = complianceType;
  }

  public List<DatasetFieldEntity> getComplianceEntities() {
    return complianceEntities;
  }

  public void setComplianceEntities(List<DatasetFieldEntity> complianceEntities) {
    this.complianceEntities = complianceEntities;
  }

  public String getConfidentiality() {
    return confidentiality;
  }

  public void setConfidentiality(String confidentiality) {
    this.confidentiality = confidentiality;
  }

  public Map<String, Boolean> getDatasetClassification() {
    return datasetClassification;
  }

  public void setDatasetClassification(Map<String, Boolean> datasetClassification) {
    this.datasetClassification = datasetClassification;
  }

  public Map<String, String> getFieldClassification() {
    return fieldClassification;
  }

  public void setFieldClassification(Map<String, String> fieldClassification) {
    this.fieldClassification = fieldClassification;
  }

  public String getRecordOwnerType() {
    return recordOwnerType;
  }

  public void setRecordOwnerType(String recordOwnerType) {
    this.recordOwnerType = recordOwnerType;
  }

  public Map<String, Object> getRetentionPolicy() {
    return retentionPolicy;
  }

  public void setRetentionPolicy(Map<String, Object> retentionPolicy) {
    this.retentionPolicy = retentionPolicy;
  }

  public Map<String, Object> getGeographicAffinity() {
    return geographicAffinity;
  }

  public void setGeographicAffinity(Map<String, Object> geographicAffinity) {
    this.geographicAffinity = geographicAffinity;
  }

  public String getModifiedBy() {
    return modifiedBy;
  }

  public void setModifiedBy(String modifiedBy) {
    this.modifiedBy = modifiedBy;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
