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
import dataquality.models.enums.TimeZone;


/**
 * Created by zechen on 7/22/15.
 */
public class TimeDimension {
  String columnName;

  String castFormat;

  TimeZone timezone;

  TimeGrain grain;

  int shift;

  public TimeDimension(String columnName, String castFormat, TimeZone timezone, TimeGrain grain, int shift) {
    this.columnName = columnName;
    this.castFormat = castFormat;
    this.timezone = timezone;
    this.grain = grain;
    this.shift = shift;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getCastFormat() {
    return castFormat;
  }

  public void setCastFormat(String castFormat) {
    this.castFormat = castFormat;
  }

  public TimeZone getTimezone() {
    return timezone;
  }

  public void setTimezone(TimeZone timezone) {
    this.timezone = timezone;
  }

  public TimeGrain getGrain() {
    return grain;
  }

  public void setGrain(TimeGrain grain) {
    this.grain = grain;
  }

  public int getShift() {
    return shift;
  }

  public void setShift(int shift) {
    this.shift = shift;
  }
}
