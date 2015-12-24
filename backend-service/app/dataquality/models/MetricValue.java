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

import dataquality.models.enums.TimeGrain;
import dataquality.utils.TimeUtil;
import java.io.Serializable;
import java.sql.Timestamp;


/**
 * Created by zechen on 8/4/15.
 */
public class MetricValue implements Serializable {
  Timestamp dataTime;
  TimeGrain timeGrain;
  String dimension;
  Double value;


  public MetricValue(Timestamp dataTime, Double value) {
    this.dataTime = dataTime;
    this.value = value;
  }

  public MetricValue(Timestamp dataTime, TimeGrain timeGrain, Double value) {
    this.dataTime = dataTime;
    this.timeGrain = timeGrain;
    this.value = value;
  }

  public MetricValue(Timestamp dataTime, String dimension, Double value) {
    this.dataTime = dataTime;
    this.dimension = dimension;
    this.value = value;
  }

  public Timestamp getDataTime() {
    return dataTime;
  }

  public void setDataTime(Timestamp dataTime) {
    this.dataTime = dataTime;
  }

  public String getDimension() {
    return dimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }

  public Double getValue() {
    return value;
  }

  public void setValue(Double value) {
    this.value = value;
  }

  public TimeGrain getTimeGrain() {
    return timeGrain;
  }

  public void setTimeGrain(TimeGrain timeGrain) {
    this.timeGrain = timeGrain;
  }

  public String toString() {
    if (dimension != null) {
      return dimension + " at " + TimeUtil.getSimpleDateFormat(timeGrain).format(dataTime) + " has value " + value;
    } else {
      return TimeUtil.getSimpleDateFormat(timeGrain).format(dataTime) + " has value " + value;
    }
  }
}
