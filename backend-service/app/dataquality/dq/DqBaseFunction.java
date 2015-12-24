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

import dataquality.models.MetricValue;
import dataquality.models.TimeRange;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zechen on 8/10/15.
 */
public class DqBaseFunction implements DqFunction {
  Map<String, Object> params;
  String description;
  TimeRange lastRun;


  public DqBaseFunction(Map<String, Object> params) {
    this.params = params;
    this.description = "Latest value";
  }

  @Override
  public List<TimeRange> getTimeRanges(List<TimeRange> lastTwoRun) {
    List<TimeRange> ret = new ArrayList<>();
    if (lastTwoRun.size() < 1) {
      return null;
    } else {
      lastRun = lastTwoRun.get(0);
      ret.add(lastTwoRun.get(0));
    }

    return ret;
  }

  @Override
  public DqRuleResult computeAndValidate(List<MetricValue> values, DqCriteria criteria) {
    DqRuleResult ret = new DqRuleResult();
    ret.setDescription(description);

    Map<MetricValue, String> failedValues = new HashMap<>();
    if (values == null || values.size() == 0) {
      ret.setStatus(DqStatus.NO_ENOUGH_DATA);
      return ret;
    }

    for (MetricValue v : values) {
      if (!criteria.check(v.getValue())) {
        ret.setStatus(DqStatus.FAILED);
        failedValues.put(v, "Reason - " + criteria.getDetail());
      }
    }

    if (DqStatus.FAILED.equals(ret.getStatus())) {
      ret.setFailedValues(failedValues);
    } else {
      ret.setStatus(DqStatus.PASSED);
    }


    return ret;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public void setParams(Map<String, Object> params) {
    this.params = params;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
