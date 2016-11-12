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


public class DatasetSchemaInfoRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  Boolean isBackwardCompatible;
  Boolean isFieldNameCaseSensitive;
  Long createTime;
  Integer revision;
  String version;
  String name;
  String description;
  DatasetOriginalSchemaRecord originalSchema;
  Map<String, String> keySchema;
  List<DatasetFieldSchemaRecord> fieldSchema;
  List<DatasetFieldPathRecord> changeDataCaptureFields;
  List<DatasetFieldPathRecord> auditFields;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "is_backward_compatible", "is_field_name_case_sensitive",
        "create_time", "revision", "version", "name", "description", "original_schema", "key_schema", "field_schema",
        "change_data_capture_fields", "audit_fields", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetSchemaInfoRecord() {
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

  public Boolean getIsBackwardCompatible() {
    return isBackwardCompatible;
  }

  public void setIsBackwardCompatible(Boolean backwardCompatible) {
    isBackwardCompatible = backwardCompatible;
  }

  public Long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Long createTime) {
    this.createTime = createTime;
  }

  public Integer getRevision() {
    return revision;
  }

  public void setRevision(Integer revision) {
    this.revision = revision;
  }

  public String getVersion() {
    return version;
  }

  public void setVersion(String version) {
    this.version = version;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public DatasetOriginalSchemaRecord getOriginalSchema() {
    return originalSchema;
  }

  public void setOriginalSchema(DatasetOriginalSchemaRecord originalSchema) {
    this.originalSchema = originalSchema;
  }

  public Map<String, String> getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(Map<String, String> keySchema) {
    this.keySchema = keySchema;
  }

  public Boolean getIsFieldNameCaseSensitive() {
    return isFieldNameCaseSensitive;
  }

  public void setIsFieldNameCaseSensitive(Boolean isFieldNameCaseSensitive) {
    this.isFieldNameCaseSensitive = isFieldNameCaseSensitive;
  }

  public List<DatasetFieldSchemaRecord> getFieldSchema() {
    return fieldSchema;
  }

  public void setFieldSchema(List<DatasetFieldSchemaRecord> fieldSchema) {
    this.fieldSchema = fieldSchema;
  }

  public List<DatasetFieldPathRecord> getChangeDataCaptureFields() {
    return changeDataCaptureFields;
  }

  public void setChangeDataCaptureFields(List<DatasetFieldPathRecord> changeDataCaptureFields) {
    this.changeDataCaptureFields = changeDataCaptureFields;
  }

  public List<DatasetFieldPathRecord> getAuditFields() {
    return auditFields;
  }

  public void setAuditFields(List<DatasetFieldPathRecord> auditFields) {
    this.auditFields = auditFields;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
