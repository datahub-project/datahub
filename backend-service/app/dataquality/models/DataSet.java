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
package dataquality.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import dataquality.models.enums.StorageType;


/**
 * Model for describe a dataset
 *
 * Created by zechen on 4/20/15.
 */
public class DataSet {

  // Dataset name
  private String name;

  // Data storage type
  private StorageType storageType;

  // Dataset alias (i.e. use for as ... in sql)
  private String alias;

  public DataSet() {
  }

  public DataSet(String name, StorageType storageType, String alias) {
    this.name = name;
    this.storageType = storageType;
    this.alias = alias;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name.toLowerCase();
  }

  public StorageType getStorageType() {
    return storageType;
  }

  @JsonIgnore
  public void setStorageType(StorageType storageType) {
    this.storageType = storageType;
  }

  @JsonProperty
  public void setStorageType(String storageType) {
    String storageTypeUppder = storageType.trim().toUpperCase();
    if (storageTypeUppder.isEmpty()) {
      setStorageType((StorageType) null);
      return;
    }
    setStorageType(StorageType.valueOf(storageTypeUppder));
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }
}
