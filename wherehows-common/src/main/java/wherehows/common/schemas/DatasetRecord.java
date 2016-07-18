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

import java.util.ArrayList;
import java.util.List;
import wherehows.common.utils.StringUtil;


/**
 * Created by zechen on 10/16/15.
 */
public class DatasetRecord extends AbstractRecord {
  Integer id;
  String name;
  String schema;
  String schemaType;
  String properties;
  String fields;
  String urn;
  String source;
  String sourceCreatedTime;
  String sourceModifiedTime;
  String locationPrefix;
  String refDatasetUrn;
  Integer refDatasetId;
  Integer statusId;
  Character isPartitioned;
  String samplePartitionFullPath;
  Integer partitionLayoutPatternId;
  String parentName;
  String storageType;
  String datasetType;
  String hiveSerdesClass;

  public DatasetRecord() {
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(id);
    allFields.add(name);
    allFields.add(schema);
    allFields.add(schemaType);
    allFields.add(properties);
    allFields.add(fields);
    allFields.add(urn);
    allFields.add(source);
    allFields.add(locationPrefix);
    allFields.add(parentName);
    allFields.add(storageType);
    allFields.add(refDatasetId);
    allFields.add(statusId);
    allFields.add(datasetType);
    allFields.add(hiveSerdesClass);
    allFields.add(isPartitioned);
    allFields.add(partitionLayoutPatternId);
    allFields.add(samplePartitionFullPath);
    allFields.add(sourceCreatedTime);
    allFields.add(sourceModifiedTime);
    // add the created_date, modified_date and wh_etl_exec_id
    allFields.add(System.currentTimeMillis()/1000);
    allFields.add(null);
    allFields.add(null);
    return allFields;
  }
  public List<String> fillAllFieldNames() {
    List<String> allFieldNames = new ArrayList<>();
    allFieldNames.add("id");
    allFieldNames.add("name");
    allFieldNames.add("schema");
    allFieldNames.add("schema_type");
    allFieldNames.add("properties");
    allFieldNames.add("fields");
    allFieldNames.add("urn");
    allFieldNames.add("source");
    allFieldNames.add("location_prefix");
    allFieldNames.add("parent_name");
    allFieldNames.add("storage_type");
    allFieldNames.add("ref_dataset_id");
    allFieldNames.add("status_id");
    allFieldNames.add("dataset_type");
    allFieldNames.add("hive_serdes_class");
    allFieldNames.add("is_partitioned");
    allFieldNames.add("partition_layout_pattern_id");
    allFieldNames.add("sample_partition_full_path");
    allFieldNames.add("source_created_time");
    allFieldNames.add("source_modified_time");
    // add the created_date, modified_date and wh_etl_exec_id
    allFieldNames.add("created_time");
    allFieldNames.add("modified_time");
    allFieldNames.add("wh_etl_exec_id");
    return allFieldNames;
  }

  public String toUpdateDatabaseValue() {
    List<Object> allFields = fillAllFields();
    List<String> allFieldNames = fillAllFieldNames();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < allFields.size(); i++) {
      Object o = allFields.get(i);
      if (o != null)   {
        String fieldName = allFieldNames.get(i);
        sb.append("`"+fieldName+"`");
        sb.append("=");
        sb.append(StringUtil.toDbString(o));
        sb.append(",");
      }
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();

  }


  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getSchemaType() {
    return schemaType;
  }

  public void setSchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  public String getProperties() {
    return properties;
  }

  public void setProperties(String properties) {
    this.properties = properties;
  }

  public String getFields() {
    return fields;
  }

  public void setFields(String fields) {
    this.fields = fields;
  }

  public String getUrn() {
    return urn;
  }

  public void setUrn(String urn) {
    this.urn = urn;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getSourceCreatedTime() {
    return sourceCreatedTime;
  }

  public void setSourceCreatedTime(String sourceCreatedTime) {
    this.sourceCreatedTime = sourceCreatedTime;
  }

  public String getSourceModifiedTime() {
    return sourceModifiedTime;
  }

  public void setSourceModifiedTime(String sourceModifiedTime) {
    this.sourceModifiedTime = sourceModifiedTime;
  }

  public String getLocationPrefix() {
    return locationPrefix;
  }

  public void setLocationPrefix(String locationPrefix) {
    this.locationPrefix = locationPrefix;
  }

  public String getRefDatasetUrn() {
    return refDatasetUrn;
  }

  public void setRefDatasetUrn(String refDatasetUrn) {
    this.refDatasetUrn = refDatasetUrn;
  }

  public Character getIsPartitioned() {
    return isPartitioned;
  }

  public void setIsPartitioned(Character isPartitioned) {
    this.isPartitioned = isPartitioned;
  }

  public String getSamplePartitionFullPath() {
    return samplePartitionFullPath;
  }

  public void setSamplePartitionFullPath(String samplePartitionFullPath) {
    this.samplePartitionFullPath = samplePartitionFullPath;
  }

  public String getParentName() {
    return parentName;
  }

  public void setParentName(String parentName) {
    this.parentName = parentName;
  }

  public String getStorageType() {
    return storageType;
  }

  public void setStorageType(String storageType) {
    this.storageType = storageType;
  }

  public String getDatasetType() {
    return datasetType;
  }

  public void setDatasetType(String datasetType) {
    this.datasetType = datasetType;
  }

  public String getHiveSerdesClass() {
    return hiveSerdesClass;
  }

  public void setHiveSerdesClass(String hiveSerdesClass) {
    this.hiveSerdesClass = hiveSerdesClass;
  }

  public Integer getRefDatasetId() {
    return refDatasetId;
  }

  public void setRefDatasetId(Integer refDatasetId) {
    this.refDatasetId = refDatasetId;
  }

  public Integer getStatusId() {
    return statusId;
  }

  public void setStatusId(Integer statusId) {
    this.statusId = statusId;
  }

  public Integer getPartitionLayoutPatternId() {
    return partitionLayoutPatternId;
  }

  public void setPartitionLayoutPatternId(Integer partitionLayoutPatternId) {
    this.partitionLayoutPatternId = partitionLayoutPatternId;
  }
}
