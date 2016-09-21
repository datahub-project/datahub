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


public class DatasetReferenceRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  String referenceType;
  String referenceFormat;
  List<String> referenceList;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "reference_type", "reference_format", "reference_list",
        "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetReferenceRecord() {
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

  public String getReferenceType() {
    return referenceType;
  }

  public void setReferenceType(String referenceType) {
    this.referenceType = referenceType;
  }

  public String getReferenceFormat() {
    return referenceFormat;
  }

  public void setReferenceFormat(String referenceFormat) {
    this.referenceFormat = referenceFormat;
  }

  public List<String> getReferenceList() {
    return referenceList;
  }

  public void setReferenceList(List<String> referenceList) {
    this.referenceList = referenceList;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
