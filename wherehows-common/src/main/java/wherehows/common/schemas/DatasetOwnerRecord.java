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


public class DatasetOwnerRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  String ownerCategory;
  String ownerSubCategory;
  String owner;
  String ownerType;
  String ownerSource;
  Long modifiedTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "owner_category", "owner_sub_category", "owner_id",
        "owner_type", "owner_source", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetOwnerRecord() {
  }

  public void setDataset(Integer datasetId, String datasetUrn) {
    this.datasetId = datasetId;
    this.datasetUrn = datasetUrn;
  }

  public String getOwnerCategory() {
    return ownerCategory;
  }

  public void setOwnerCategory(String ownerCategory) {
    this.ownerCategory = ownerCategory;
  }

  public String getOwnerSubCategory() {
    return ownerSubCategory;
  }

  public void setOwnerSubCategory(String ownerSubCategory) {
    this.ownerSubCategory = ownerSubCategory;
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

  public String getOwnerSource() {
    return ownerSource;
  }

  public void setOwnerSource(String ownerSource) {
    this.ownerSource = ownerSource;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
