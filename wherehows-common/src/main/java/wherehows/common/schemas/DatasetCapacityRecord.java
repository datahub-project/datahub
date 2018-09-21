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


public class DatasetCapacityRecord extends AbstractRecord {

  Integer datasetId;
  String datasetUrn;
  String capacityName;
  String capacityType;
  String capacityUnit;
  Double capacityLow;
  Double capacityHigh;
  Long modifiedTime;

  public DatasetCapacityRecord() {
  }

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"dataset_id", "dataset_urn", "capacity_name", "capacity_type", "capacity_unit", "capacity_low",
        "capacity_high", "modified_time"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
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

  public String getCapacityName() {
    return capacityName;
  }

  public void setCapacityName(String capacityName) {
    this.capacityName = capacityName;
  }

  public String getCapacityType() {
    return capacityType;
  }

  public void setCapacityType(String capacityType) {
    this.capacityType = capacityType;
  }

  public String getCapacityUnit() {
    return capacityUnit;
  }

  public void setCapacityUnit(String capacityUnit) {
    this.capacityUnit = capacityUnit;
  }

  public Double getCapacityLow() {
    return capacityLow;
  }

  public void setCapacityLow(Double capacityLow) {
    this.capacityLow = capacityLow;
  }

  public Double getCapacityHigh() {
    return capacityHigh;
  }

  public void setCapacityHigh(Double capacityHigh) {
    this.capacityHigh = capacityHigh;
  }

  public Long getModifiedTime() {
    return modifiedTime;
  }

  public void setModifiedTime(Long modifiedTime) {
    this.modifiedTime = modifiedTime;
  }
}
