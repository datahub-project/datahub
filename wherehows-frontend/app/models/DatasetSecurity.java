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
package models;

import java.util.List;
import java.util.Map;


public class DatasetSecurity {

  private Integer datasetId;
  private String datasetUrn;
  private Map<String, Object> datasetClassification;
  private String confidentiality;
  private Map<String, String> classification;
  private String recordOwnerType;
  private Map<String, Object> retentionPolicy;
  private Map<String, Object> geographicAffinity;
  private Long modifiedTime;

  public DatasetSecurity() {
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

  public Map<String, Object> getDatasetClassification() {
    return datasetClassification;
  }

  public void setDatasetClassification(Map<String, Object> datasetClassification) {
    this.datasetClassification = datasetClassification;
  }

  public String getConfidentiality() {
    return confidentiality;
  }

  public void setConfidentiality(String confidentiality) {
    this.confidentiality = confidentiality;
  }

  public Map<String, String> getClassification() {
    return classification;
  }

  public void setClassification(Map<String, String> classification) {
    this.classification = classification;
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

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
