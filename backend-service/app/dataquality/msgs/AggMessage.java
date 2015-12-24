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
package dataquality.msgs;

import dataquality.models.enums.Frequency;
import dataquality.models.enums.TimeGrain;
import java.sql.Timestamp;


/**
 * Created by zechen on 5/15/15.
 */
public class AggMessage {

  private int aggDefId;
  private Timestamp aggTime;
  private Frequency frequency;
  private String timeDimension;
  private TimeGrain grain;
  private Integer shift;
  private Long aggRunId;

  public AggMessage(int aggDefId) {
    this.aggDefId = aggDefId;
  }

  public AggMessage(int aggDefId, Timestamp aggTime, String timeDimension, TimeGrain grain, Integer shift) {
    this.aggDefId = aggDefId;
    this.aggTime = aggTime;
    this.timeDimension = timeDimension;
    this.grain = grain;
    this.shift = shift;
  }

  public AggMessage(int aggDefId, Timestamp aggTime, Frequency frequency, String timeDimension, TimeGrain grain,
      Integer shift) {
    this.aggDefId = aggDefId;
    this.aggTime = aggTime;
    this.frequency = frequency;
    this.timeDimension = timeDimension;
    this.grain = grain;
    this.shift = shift;
  }

  public int getAggDefId() {
    return aggDefId;
  }

  public void setAggDefId(int aggDefId) {
    this.aggDefId = aggDefId;
  }

  public Timestamp getAggTime() {
    return aggTime;
  }

  public void setAggTime(Timestamp aggTime) {
    this.aggTime = aggTime;
  }

  public TimeGrain getGrain() {
    return grain;
  }

  public void setGrain(TimeGrain grain) {
    this.grain = grain;
  }

  public String getTimeDimension() {
    return timeDimension;
  }

  public void setTimeDimension(String timeDimension) {
    this.timeDimension = timeDimension;
  }

  public Integer getShift() {
    return shift;
  }

  public void setShift(Integer shift) {
    this.shift = shift;
  }

  public Frequency getFrequency() {
    return frequency;
  }

  public void setFrequency(Frequency frequency) {
    this.frequency = frequency;
  }

  public Long getAggRunId() {
    return aggRunId;
  }

  public void setAggRunId(Long aggRunId) {
    this.aggRunId = aggRunId;
  }
}
