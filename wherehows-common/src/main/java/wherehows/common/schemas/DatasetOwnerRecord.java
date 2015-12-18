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


/**
 * Created by zechen on 12/8/15.
 */
public class DatasetOwnerRecord extends AbstractRecord {
  String datasetUrn;
  String ownerId;
  Integer sortId;
  String namespace;
  String dbName;
  Integer sourceTime;

  public DatasetOwnerRecord() {
  }

  public DatasetOwnerRecord(String datasetUrn, String ownerId, Integer sortId, String namespace, String dbName,
      Integer sourceTime) {
    this.datasetUrn = datasetUrn;
    this.ownerId = ownerId;
    this.sortId = sortId;
    this.namespace = namespace;
    this.dbName = dbName;
    this.sourceTime = sourceTime;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(datasetUrn);
    allFields.add(ownerId);
    allFields.add(sortId);
    allFields.add(namespace);
    allFields.add(dbName);
    allFields.add(sourceTime);
    return allFields;
  }

  public String getDatasetUrn() {
    return datasetUrn;
  }

  public void setDatasetUrn(String datasetUrn) {
    this.datasetUrn = datasetUrn;
  }

  public String getOwnerId() {
    return ownerId;
  }

  public void setOwnerId(String ownerId) {
    this.ownerId = ownerId;
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

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public Integer getSourceTime() {
    return sourceTime;
  }

  public void setSourceTime(Integer sourceTime) {
    this.sourceTime = sourceTime;
  }
}
