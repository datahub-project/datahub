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
import dataquality.models.enums.DataType;
import dataquality.models.enums.TimeGrain;
import dataquality.models.enums.TimeZone;


/**
 * Created by zechen on 6/8/15.
 */
public class TimeColumn {

  String columnName;

  DataType dataType;

  TimeZone dataTimezone;

  TimeGrain dataGrain;

  TimeZone aggTimezone;

  TimeGrain aggGrain;

  String format;

  int shift;

  public TimeColumn() {
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public DataType getDataType() {
    return dataType;
  }

  @JsonIgnore
  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  @JsonProperty
  public void setDataType(String dataType) {
    String dataTypeUpper = dataType.trim().toUpperCase();
    if (dataTypeUpper.isEmpty()) {
      setDataType((DataType) null);
      return;
    }
    setDataType(DataType.valueOf(dataTypeUpper));
  }

  public TimeZone getDataTimezone() {
    return dataTimezone;
  }

  @JsonIgnore
  public void setDataTimezone(TimeZone dataTimezone) {
    this.dataTimezone = dataTimezone;
  }

  @JsonProperty
  public void setDataTimezone(String dataTimezone) {
    String dataTimezoneUpper = dataTimezone.trim().toUpperCase();
    if (dataTimezoneUpper.isEmpty()) {
      setDataTimezone((TimeZone) null);
      return;
    }
    setDataTimezone(TimeZone.valueOf(dataTimezoneUpper));
  }

  public TimeGrain getDataGrain() {
    return dataGrain;
  }

  @JsonIgnore
  public void setDataGrain(TimeGrain dataGrain) {
    this.dataGrain = dataGrain;
  }

  @JsonProperty
  public void setDataGrain(String dataGrain) {
    String dataGrainUpper = dataGrain.trim().toUpperCase();
    if (dataGrainUpper.isEmpty()) {
      setDataGrain((TimeGrain) null);
      return;
    }
    setDataGrain(TimeGrain.valueOf(dataGrainUpper));
  }

  public TimeZone getAggTimezone() {
    return aggTimezone;
  }

  @JsonIgnore
  public void setAggTimezone(TimeZone aggTimezone) {
    this.aggTimezone = aggTimezone;
  }

  @JsonProperty
  public void setAggTimezone(String aggTimezone) {
    String aggTimezoneUpper = aggTimezone.trim().toUpperCase();
    if (aggTimezoneUpper.isEmpty()) {
      setAggTimezone((TimeZone) null);
      return;
    }
    setAggTimezone(TimeZone.valueOf(aggTimezoneUpper));
  }

  public TimeGrain getAggGrain() {
    return aggGrain;
  }

  @JsonIgnore
  public void setAggGrain(TimeGrain aggGrain) {
    this.aggGrain = aggGrain;
  }

  @JsonProperty
  public void setAggGrain(String aggGrain) {
    String aggGrainUpper = aggGrain.trim().toUpperCase();
    if (aggGrainUpper.isEmpty()) {
      setAggGrain((TimeGrain) null);
      return;
    }
    setAggGrain(TimeGrain.valueOf(aggGrainUpper));
  }

  public int getShift() {
    return shift;
  }

  public void setShift(int shift) {
    this.shift = shift;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getConvertedTime() {
    String ret = null;
    switch (dataType) {
      case INT:
      case INTEGER:
      case LONG:
        ret = "cast(cast(" + columnName + " as VARCHAR(19)) as timestamp format '" + format + "')";
        break;
      case STRING:
        ret = "cast(" + columnName + " as timestamp format '" + format + "')";
        break;
      case DATE:
        ret = "cast(" + columnName + " as timestamp)";
        break;
      case DATETIME:
        ret = "cast(" + columnName + " as timestamp)";
        break;
      case TIMESTAMP:
        ret = columnName;
        break;
    }

    return ret;
  }

  public String getTruncTime() {
    String ret = null;
    switch (aggGrain) {
      case HOUR:
        ret = "cast(cast(" + columnName + " as char(13)) as timestamp format 'YYYY-MM-DDBHH')";
        break;
      case DAY:
        ret = "cast(cast(" + columnName + " as char(10)) as timestamp format 'YYYY-MM-DD')";
        break;
      case MONTH:
        ret = "cast(cast(" + columnName + " as char(7)) as timestamp format 'YYYY-MM')";
        break;
      case YEAR:
        ret = "cast(cast("+ columnName +" as char(4)) as timestamp format 'YYYY')";
        break;
    }

    return ret;
  }

}
