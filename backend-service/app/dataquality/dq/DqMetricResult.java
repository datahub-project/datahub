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
package dataquality.dq;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;


/**
 * Created by zechen on 8/12/15.
 */

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DqMetricResult {
  int metricId;
  int aggDefId;
  String metricName;
  DqStatus status;
  List<DqRuleResult> ruleResults;

  public DqMetricResult() {
  }

  public int getAggDefId() {
    return aggDefId;
  }

  public void setAggDefId(int aggDefId) {
    this.aggDefId = aggDefId;
  }

  public DqStatus getStatus() {
    return status;
  }

  public void setStatus(DqStatus status) {
    this.status = status;
  }

  public List<DqRuleResult> getRuleResults() {
    return ruleResults;
  }

  public void setRuleResults(List<DqRuleResult> ruleResults) {
    this.ruleResults = ruleResults;
  }

  public int getMetricId() {
    return metricId;
  }

  public void setMetricId(int metricId) {
    this.metricId = metricId;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }
}
