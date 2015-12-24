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

import dataquality.models.enums.Frequency;
import java.sql.Timestamp;


/**
 * Created by zechen on 7/7/15.
 */
public class AggValue {
  Integer aggCombId;
  String dimension;
  String value;
  Frequency frequency;
  Timestamp createTs;
  Timestamp etlTs;
  Timestamp dataTs;

  public AggValue(Integer aggCombId, Timestamp createTs, String value, Frequency frequency) {
    this.aggCombId = aggCombId;
    this.createTs = createTs;
    this.value = value;
    this.frequency = frequency;
  }

  public AggValue(Integer aggCombId, Timestamp createTs, String value, Frequency frequency, String dimension) {
    this.aggCombId = aggCombId;
    this.createTs = createTs;
    this.value = value;
    this.frequency = frequency;
    this.dimension = dimension;
  }

  public String getOldValue() {
    return getOldValue(-1);
  }

  public String getOldValue(int unit) {

    return value + 0.02;
  }

  public Integer getAggCombId() {
    return aggCombId;
  }

  public void setAggCombId(Integer aggCombId) {
    this.aggCombId = aggCombId;
  }

  public String getDimension() {
    return dimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public Timestamp getCreateTs() {
    return createTs;
  }

  public void setCreateTs(Timestamp createTs) {
    this.createTs = createTs;
  }

  public Frequency getFrequency() {
    return frequency;
  }

  public void setFrequency(Frequency frequency) {
    this.frequency = frequency;
  }


}
