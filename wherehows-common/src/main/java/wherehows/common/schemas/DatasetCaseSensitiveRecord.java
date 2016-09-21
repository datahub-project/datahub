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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DatasetCaseSensitiveRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  Boolean datasetName;
  Boolean fieldName;
  Boolean dataContent;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "dataset_name", "field_name", "data_content", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetCaseSensitiveRecord() {
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

  public Boolean getDatasetName() {
    return datasetName;
  }

  public void setDatasetName(Boolean datasetName) {
    this.datasetName = datasetName;
  }

  public Boolean getFieldName() {
    return fieldName;
  }

  public void setFieldName(Boolean fieldName) {
    this.fieldName = fieldName;
  }

  public Boolean getDataContent() {
    return dataContent;
  }

  public void setDataContent(Boolean dataContent) {
    this.dataContent = dataContent;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
