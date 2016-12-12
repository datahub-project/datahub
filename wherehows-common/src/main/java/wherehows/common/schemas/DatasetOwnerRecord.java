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
import java.util.ArrayList;
import java.util.List;


public class DatasetOwnerRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  Integer appId;
  String ownerCategory;
  String owner;
  String ownerType;
  String isGroup;
  String isActive;
  Integer sortId;
  String namespace;
  String ownerSource;
  String dbIds;
  Long sourceTime;
  Long createdTime;
  Long modifiedTime;
  String confirmedBy;
  Long confirmedOn;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "app_id", "owner_type", "owner_id",
        "owner_id_type", "is_group", "is_active", "sort_id", "namespace", "owner_source", "db_ids",
        "source_time", "created_time", "modified_time", "confirmed_by", "confirmed_on"};
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(datasetUrn);
    allFields.add(owner);
    allFields.add(sortId);
    allFields.add(namespace);
    allFields.add(dbIds);
    allFields.add(sourceTime);
    return allFields;
  }

  @JsonIgnore
  public String[] getDbColumnForUnmatchedOwner() {
    return new String[]{"dataset_urn", "app_id", "owner_type", "owner_id", "owner_id_type",
        "is_group", "is_active", "sort_id", "namespace", "owner_source", "db_name", "db_id", "source_time"};
  }

  @JsonIgnore
  public Object[] getValuesForUnmatchedOwner() {
    return new Object[]{datasetUrn, appId, ownerCategory, owner, ownerType, isGroup,
        isActive, sortId, namespace, ownerSource, "N/A", dbIds, sourceTime};
  }

  public DatasetOwnerRecord() {
  }

  public DatasetOwnerRecord(String datasetUrn, String ownerId, Integer sortId, String namespace, String dbName,
      Long sourceTime) {
    this.datasetUrn = datasetUrn;
    this.owner = ownerId;
    this.sortId = sortId;
    this.namespace = namespace;
    this.dbIds = dbName;
    this.sourceTime = sourceTime;
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

  public Integer getAppId() {
    return appId;
  }

  public void setAppId(Integer appId) {
    this.appId = appId;
  }

  public String getOwnerCategory() {
    return ownerCategory;
  }

  public void setOwnerCategory(String ownerCategory) {
    this.ownerCategory = ownerCategory;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getOwnerType() {
    return ownerType;
  }

  public void setOwnerType(String ownerType) {
    this.ownerType = ownerType;
  }

  public String getIsGroup() {
    return isGroup;
  }

  public void setIsGroup(String isGroup) {
    this.isGroup = isGroup;
  }

  public String getIsActive() {
    return isActive;
  }

  public void setIsActive(String isActive) {
    this.isActive = isActive;
  }

  public Integer getSortId() {
    return sortId;
  }

  public void setSortId(Integer sortId) {
    this.sortId = sortId;
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public String getOwnerSource() {
    return ownerSource;
  }

  public void setOwnerSource(String ownerSource) {
    this.ownerSource = ownerSource;
  }

  public String getDbIds() {
    return dbIds;
  }

  public void setDbIds(String dbIds) {
    this.dbIds = dbIds;
  }

  public Long getSourceTime() {
    return sourceTime;
  }

  public void setSourceTime(Long sourceTime) {
    this.sourceTime = sourceTime;
  }

  public Long getCreatedTime() {
    return createdTime;
  }

  public void setCreatedTime(Long createdTime) {
    this.createdTime = createdTime;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }

  public Long getConfirmedOn() {
    return confirmedOn;
  }

  public void setConfirmedOn(Long confirmedOn) {
    this.confirmedOn = confirmedOn;
  }

  public String getConfirmedBy() {
    return confirmedBy;
  }

  public void setConfirmedBy(String confirmedBy) {
    this.confirmedBy = confirmedBy;
  }
}
