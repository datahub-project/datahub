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


public class DatasetIndexRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  String indexType;
  String indexName;
  Boolean isUnique;
  List<DatasetFieldIndexRecord> indexedFields;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "index_type", "index_name", "is_unique", "indexed_fields",
        "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetIndexRecord() {
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

  public String getIndexType() {
    return indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public Boolean getIsUnique() {
    return isUnique;
  }

  public void setIsUnique(Boolean isUnique) {
    this.isUnique = isUnique;
  }

  public List<DatasetFieldIndexRecord> getIndexedFields() {
    return indexedFields;
  }

  public void setIndexedFields(List<DatasetFieldIndexRecord> indexedFields) {
    this.indexedFields = indexedFields;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
