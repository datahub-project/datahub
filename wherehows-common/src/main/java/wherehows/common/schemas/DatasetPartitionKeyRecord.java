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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;


public class DatasetPartitionKeyRecord {

  String partitionLevel;
  String partitionType;
  String timeFormat;
  String timeGranularity;
  List<String> fieldNames;
  List<String> partitionValues;
  Integer numberOfHashBuckets;

  public DatasetPartitionKeyRecord() {
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

  public String getTimeGranularity() {
    return timeGranularity;
  }

  public void setTimeGranularity(String timeGranularity) {
    this.timeGranularity = timeGranularity;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
  }

  public List<String> getPartitionValues() {
    return partitionValues;
  }

  public void setPartitionValues(List<String> partitionValues) {
    this.partitionValues = partitionValues;
  }

  public Integer getNumberOfHashBuckets() {
    return numberOfHashBuckets;
  }

  public void setNumberOfHashBuckets(Integer numberOfHashBuckets) {
    this.numberOfHashBuckets = numberOfHashBuckets;
  }
}
