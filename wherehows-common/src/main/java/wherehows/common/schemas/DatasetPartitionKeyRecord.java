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


public class DatasetPartitionKeyRecord extends AbstractRecord {

  String partitionLevel;
  String partitionType;
  String timeFormat;
  String granularity;
  List<String> fieldNames;
  Integer numberOfHashBuckets;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"partition_level", "partition_type", "time_format", "granularity", "field_names",
        "number_of_hash_buckets"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetPartitionKeyRecord() {
  }

  @Override
  public String toString() {
    try {
      return this.getFieldValueMap().toString();
    } catch (IllegalAccessException ex) {
      return null;
    }
  }

  public String getPartitionLevel() {
    return partitionLevel;
  }

  public void setPartitionLevel(String partitionLevel) {
    this.partitionLevel = partitionLevel;
  }

  public String getPartitionType() {
    return partitionType;
  }

  public void setPartitionType(String partitionType) {
    this.partitionType = partitionType;
  }

  public String getTimeFormat() {
    return timeFormat;
  }

  public void setTimeFormat(String timeFormat) {
    this.timeFormat = timeFormat;
  }

  public String getGranularity() {
    return granularity;
  }

  public void setGranularity(String granularity) {
    this.granularity = granularity;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
  }

  public Integer getNumberOfHashBuckets() {
    return numberOfHashBuckets;
  }

  public void setNumberOfHashBuckets(Integer numberOfHashBuckets) {
    this.numberOfHashBuckets = numberOfHashBuckets;
  }
}
