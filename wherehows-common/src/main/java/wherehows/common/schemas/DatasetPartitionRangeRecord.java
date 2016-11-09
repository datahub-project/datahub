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


public class DatasetPartitionRangeRecord extends AbstractRecord {

  String partitionType;
  String minPartitionValue;
  String maxPartitionValue;
  String listPartitionValue;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetPartitionRangeRecord() {
  }

  public String getPartitionType() {
    return partitionType;
  }

  public void setPartitionType(String partitionType) {
    this.partitionType = partitionType;
  }

  public String getMinPartitionValue() {
    return minPartitionValue;
  }

  public void setMinPartitionValue(String minPartitionValue) {
    this.minPartitionValue = minPartitionValue;
  }

  public String getMaxPartitionValue() {
    return maxPartitionValue;
  }

  public void setMaxPartitionValue(String maxPartitionValue) {
    this.maxPartitionValue = maxPartitionValue;
  }

  public String getListPartitionValue() {
    return listPartitionValue;
  }

  public void setListPartitionValue(String listPartitionValue) {
    this.listPartitionValue = listPartitionValue;
  }
}
