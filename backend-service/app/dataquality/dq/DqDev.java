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
import dataquality.utils.TimeUtil;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;



/**
 * Created by zechen on 8/9/15.
 */
public class DqDev extends DqBaseFunction {
  TimeOption range;

  public DqDev(Map<String, Object> params) {
    super(params);
    range = TimeOption.valueOf(params.get("range").toString());
    description = "Derivative within " + range.toString();
  }

  @Override
  public List<TimeRange> getTimeRanges(List<TimeRange> lastTwoRun) {
    if (lastTwoRun.size() < 1) {
      return null;
    }

    lastRun = lastTwoRun.get(0);
    List<TimeRange> ret = new ArrayList<>();
    Timestamp start = null;
    switch (range) {
      case RUN:
        ret.add(lastRun);
        return ret;
      case HOUR:
        start = TimeUtil.adjust(lastRun.getEnd(), TimeOption.HOUR);
        break;
      case DAY:
        start = TimeUtil.adjust(lastRun.getEnd(), TimeOption.DAY);
        break;
      case WEEK:
        start = TimeUtil.adjust(lastRun.getEnd(), TimeOption.WEEK);
        break;
      case MONTH:
        start = TimeUtil.adjust(lastRun.getEnd(), TimeOption.MONTH);
        break;
      case QUARTER:
        start = TimeUtil.adjust(lastRun.getEnd(), TimeOption.QUARTER);
        break;
      case YEAR:
        start = TimeUtil.adjust(lastRun.getEnd(), TimeOption.YEAR);
        break;
    }
    TimeRange timeRange = new TimeRange();
    timeRange.setStart(start);
    timeRange.setEnd(lastRun.getEnd());
    timeRange.setExclusiveStart(true);
    ret.add(timeRange);
    return ret;
  }

  @Override
  public DqRuleResult computeAndValidate(List<MetricValue> values, DqCriteria criteria) {
    DqRuleResult ret = new DqRuleResult();
    ret.setDescription(description);

    int count = values.size();

    Double total = 0.0;
    for (MetricValue value : values) {
      total += value.getValue();
    }

    Double avg = total / count;

    Double var = 0.0;

    for (MetricValue value : values) {
      var += (value.getValue() - avg) * (value.getValue() - avg);
    }

    Double dev = Math.sqrt(var);

    if (!criteria.check(dev)) {
      ret.setStatus(DqStatus.FAILED);
      ret.addMessage("Reason - deviation is  " + dev + " - " + criteria.getDetail());
    } else {
      ret.setStatus(DqStatus.PASSED);
    }


    return ret;
  }
}
