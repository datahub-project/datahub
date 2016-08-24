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
  Boolean isLatestRevision;
  Long createTime;
  Integer revision;
  String version;
  String name;
  String description;
  String format;
  String originalSchema;
  Map<String, String> originalSchemaChecksum;
  String keySchemaType;
  String keySchemaFormat;
  String keySchema;
  Boolean isFieldNameCaseSensitive;
  List<DatasetFieldSchemaRecord> fieldSchema;
  List<DatasetFieldPathRecord> changeDataCaptureFields;
  List<DatasetFieldPathRecord> auditFields;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "is_latest_revision", "create_time", "revision", "version", "name",
        "description", "format", "original_schema", "original_schema_checksum", "key_schema_type", "key_schema_format",
        "key_schema", "is_field_name_case_sensitive", "field_schema", "change_data_capture_fields", "audit_fields",
        "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetSchemaInfoRecord() {
  }

  public void setDataset(Integer datasetId, String datasetUrn) {
    this.datasetId = datasetId;
    this.datasetUrn = datasetUrn;
  }

  public Boolean getIsLatestRevision() {
    return isLatestRevision;
  }

  public void setIsLatestRevision(Boolean isLatestRevision) {
    this.isLatestRevision = isLatestRevision;
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

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getOriginalSchema() {
    return originalSchema;
  }

  public void setOriginalSchema(String originalSchema) {
    this.originalSchema = originalSchema;
  }

  public Map<String, String> getOriginalSchemaChecksum() {
    return originalSchemaChecksum;
  }

  public void setOriginalSchemaChecksum(Map<String, String> originalSchemaChecksum) {
    this.originalSchemaChecksum = originalSchemaChecksum;
  }

  public String getKeySchemaType() {
    return keySchemaType;
  }

  public void setKeySchemaType(String keySchemaType) {
    this.keySchemaType = keySchemaType;
  }

  public String getKeySchemaFormat() {
    return keySchemaFormat;
  }

  public void setKeySchemaFormat(String keySchemaFormat) {
    this.keySchemaFormat = keySchemaFormat;
  }

  public String getKeySchema() {
    return keySchema;
  }

  public void setKeySchema(String keySchema) {
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
