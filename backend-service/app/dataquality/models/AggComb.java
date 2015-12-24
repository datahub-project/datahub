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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import dataquality.models.enums.DataType;
import dataquality.models.enums.TimeGrain;


/**
 * Created by zechen on 7/28/15.
 */
public class AggComb {

  Integer id;

  Integer aggDefId;

  @JsonIgnore
  Integer aggQueryId;

  String formula;

  DataType dataType;

  String dimension;

  @JsonSerialize(include= JsonSerialize.Inclusion.NON_NULL)
  String timeDimension;

  TimeGrain timeGrain;

  @JsonIgnore
  String rollUpFunc;

  public AggComb() {

  }

  public AggComb(Integer id, Integer aggDefId, Integer aggQueryId, String formula, DataType dataType, String dimension, String timeDimension,
      TimeGrain timeGrain, String rollUpFunc) {
    this.id = id;
    this.aggDefId = aggDefId;
    this.aggQueryId = aggQueryId;
    this.formula = formula;
    this.dataType = dataType;
    this.dimension = dimension;
    this.timeDimension = timeDimension;
    this.timeGrain = timeGrain;
    this.rollUpFunc = rollUpFunc;
  }
  
  public String generatedName() {
    StringBuilder sb = new StringBuilder("");
    if (this.getDimension() != null) {
      sb.append(this.getFormula());
      sb.append(" in ");
      sb.append(this.getDimension());
    } else {
      sb.append("overall ");
      sb.append(this.getFormula());
    }

    return sb.toString();
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getAggDefId() {
    return aggDefId;
  }

  public void setAggDefId(Integer aggDefId) {
    this.aggDefId = aggDefId;
  }

  public Integer getAggQueryId() {
    return aggQueryId;
  }

  public void setAggQueryId(Integer aggQueryId) {
    this.aggQueryId = aggQueryId;
  }

  public String getFormula() {
    return formula;
  }

  public void setFormula(String formula) {
    this.formula = formula;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public String getDimension() {
    return dimension;
  }

  public TimeGrain getTimeGrain() {
    return timeGrain;
  }

  public void setTimeGrain(TimeGrain timeGrain) {
    this.timeGrain = timeGrain;
  }

  public String getRollUpFunc() {
    return rollUpFunc;
  }

  public void setRollUpFunc(String rollUpFunc) {
    this.rollUpFunc = rollUpFunc;
  }

  public String getTimeDimension() {
    return timeDimension;
  }

  public void setTimeDimension(String timeDimension) {
    this.timeDimension = timeDimension;
  }

  public void setDimension(String dimension) {
    this.dimension = dimension;
  }
}
