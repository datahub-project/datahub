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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;


public class DatasetFieldSchemaRecord extends AbstractRecord {

  Integer datasetId;
  Integer position;
  Integer parentFieldPosition;
  String fieldJsonPath;
  String fieldPath;
  String parentPath;
  String fieldName;
  String label;
  List<String> aliases;
  String type;
  String logicalType;
  String semanticType;
  String abstractType;
  String description;
  Boolean nullable;
  String defaultValue;
  Integer maxByteLength;
  Integer maxCharLength;
  String charType;
  Integer precision;
  Integer scale;
  Boolean isRecursive;
  Boolean partitioned;
  Boolean indexed;
  String namespace;
  Integer defaultCommentId;
  String commentIds;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "position", "parent_field_position", "field_json_path", "field_path", "parent_path",
        "field_name", "label", "aliases", "type", "logical_type", "semantic_type", "abstract_type", "description",
        "nullable", "default_value", "max_byte_length", "max_char_length", "char_type", "precision", "scale",
        "is_recursive", "partitioned", "indexed", "namespace", "default_comment_id", "comment_ids"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  @JsonIgnore
  public String[] getFieldDetailColumns() {
    return new String[]{"dataset_id", "sort_id", "parent_sort_id", "parent_path", "field_name", "fields_layout_id",
        "field_label", "data_type", "data_size", "data_precision", "data_fraction", "is_nullable", "is_indexed",
        "is_partitioned", "is_recursive", "default_value", "namespace", "default_comment_id", "comment_ids"};
  }

  @JsonIgnore
  public Object[] getFieldDetailValues() {
    return new Object[]{datasetId, position, parentFieldPosition, parentPath, fieldName, 0, label, type, maxCharLength,
        precision, scale, nullable != null && nullable ? "Y" : "N", indexed != null && indexed ? "Y" : "N",
        partitioned != null && partitioned ? "Y" : "N", isRecursive != null && isRecursive ? "Y" : "N",
        defaultValue, namespace, defaultCommentId, commentIds};
  }

  public DatasetFieldSchemaRecord() {
  }

  public Integer getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(Integer datasetId) {
    this.datasetId = datasetId;
  }

  public Integer getPosition() {
    return position;
  }

  public void setPosition(Integer position) {
    this.position = position;
  }

  public Integer getParentFieldPosition() {
    return parentFieldPosition;
  }

  public void setParentFieldPosition(Integer parentFieldPosition) {
    this.parentFieldPosition = parentFieldPosition;
  }

  public String getFieldJsonPath() {
    return fieldJsonPath;
  }

  public void setFieldJsonPath(String fieldJsonPath) {
    this.fieldJsonPath = fieldJsonPath;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public void setFieldPath(String fieldPath) {
    this.fieldPath = fieldPath;
  }

  public String getParentPath() {
    return parentPath;
  }

  public void setParentPath(String parentPath) {
    this.parentPath = parentPath;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  public List<String> getAliases() {
    return aliases;
  }

  public void setAliases(List<String> aliases) {
    this.aliases = aliases;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public void setLogicalType(String logicalType) {
    this.logicalType = logicalType;
  }

  public String getSemanticType() {
    return semanticType;
  }

  public void setSemanticType(String semanticType) {
    this.semanticType = semanticType;
  }

  public String getAbstractType() {
    return abstractType;
  }

  public void setAbstractType(String abstractType) {
    this.abstractType = abstractType;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Boolean getNullable() {
    return nullable;
  }

  public void setNullable(Boolean nullable) {
    this.nullable = nullable;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  public Integer getMaxByteLength() {
    return maxByteLength;
  }

  public void setMaxByteLength(Integer maxByteLength) {
    this.maxByteLength = maxByteLength;
  }

  public Integer getMaxCharLength() {
    return maxCharLength;
  }

  public void setMaxCharLength(Integer maxCharLength) {
    this.maxCharLength = maxCharLength;
  }

  public String getCharType() {
    return charType;
  }

  public void setCharType(String charType) {
    this.charType = charType;
  }

  public Integer getPrecision() {
    return precision;
  }

  public void setPrecision(Integer precision) {
    this.precision = precision;
  }

  public Integer getScale() {
    return scale;
  }

  public void setScale(Integer scale) {
    this.scale = scale;
  }

  public Boolean getRecursive() {
    return isRecursive;
  }

  public void setRecursive(Boolean recursive) {
    isRecursive = recursive;
  }

  public Boolean getPartitioned() {
    return partitioned;
  }

  public void setPartitioned(Boolean partitioned) {
    this.partitioned = partitioned;
  }

  public Boolean getIndexed() {
    return indexed;
  }

  public void setIndexed(Boolean indexed) {
    this.indexed = indexed;
  }

  public Boolean getIsRecursive() {
    return isRecursive;
  }

  public void setIsRecursive(Boolean isRecursive) {
    this.isRecursive = isRecursive;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public Integer getDefaultCommentId() {
    return defaultCommentId;
  }

  public void setDefaultCommentId(Integer defaultCommentId) {
    this.defaultCommentId = defaultCommentId;
  }

  public String getCommentIds() {
    return commentIds;
  }

  public void setCommentIds(String commentIds) {
    this.commentIds = commentIds;
  }
}
