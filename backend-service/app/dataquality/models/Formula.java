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
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Model for aggregation formula
 *
 * Created by zechen on 6/5/15.
 */
public class Formula {

  // expression of the aggregation
  private String expr;

  // return data type of this expression
  private DataType dataType;

  @JsonProperty(required = true)
  // alias of this formula
  private String alias;

  // overall aggregation flag
  boolean overall;

  public Formula() {

  }

  public Formula(String expr, DataType dataType, String alias, boolean overall) {
    this.expr = expr;
    this.dataType = dataType;
    this.alias = alias;
    this.overall = overall;
  }

  public boolean isAdditive() {
    Pattern p = Pattern.compile("(.*\\bdistinct\\b.*)|(.*\\bcase\\b.*)");
    Matcher m = p.matcher(expr.toLowerCase());
    return !m.matches();
  }

  public String getFunc() {
    Pattern p = Pattern.compile("((\\w+)\\s*\\()");
    Matcher m = p.matcher(expr.toLowerCase());
    if (m.find()) {
      return m.group(2);
    }

    return null;
  }

  public String getRollupFunc() {
    if (!isAdditive()) {
      return null;
    } else {
      if (getFunc() != null && getFunc().toLowerCase().equals("count")) {
        return "sum";
      } else {
        return getFunc();
      }
    }
  }

  public String getExpr() {
    return expr;
  }

  public void setExpr(String expr) {
    this.expr = expr;
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

  public String getAlias() {
    if (alias != null && !alias.isEmpty()) {
      return alias;
    } else {
      String generatedAlias = expr.replaceAll("[^A-Za-z0-9]+", "_");
      if (generatedAlias.length() > 127) {
        return generatedAlias.substring(0, 127);
      } else {
        return generatedAlias;
      }
    }
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public boolean isOverall() {
    return overall;
  }

  public void setOverall(boolean overall) {
    this.overall = overall;
  }
}
