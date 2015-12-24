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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;



/**
 * Created by zechen on 8/7/15.
 */
public class DqDiff extends DqBaseFunction {

  TimeOption compare;

  public DqDiff(Map<String, Object> params) {
    super(params);
    compare = TimeOption.valueOf(params.get("compare").toString());
    description = "Difference between " + compare.toString();
  }

  @Override
  public List<TimeRange> getTimeRanges(List<TimeRange> lastTwoRun) {
    if (lastTwoRun.size() < 1) {
      return null;
    }
    lastRun = lastTwoRun.get(0);
    List<TimeRange> ret = new ArrayList<>();
    switch (compare) {
      case RUN:
        if (lastTwoRun.size() < 2 || !lastRun.getStart().equals(lastRun.getEnd())
            || !lastTwoRun.get(1).getStart().equals(lastTwoRun.get(1).getEnd())) {
          return null;
        } else {
          return lastTwoRun;
        }
      case HOUR:
        ret.add(TimeUtil.adjust(lastRun, TimeOption.HOUR));
        break;
      case DAY:
        ret.add(TimeUtil.adjust(lastRun, TimeOption.DAY));
        break;
      case WEEK:
        ret.add(TimeUtil.adjust(lastRun, TimeOption.WEEK));
        break;
      case MONTH:
        ret.add(TimeUtil.adjust(lastRun, TimeOption.MONTH));
        break;
      case QUARTER:
        ret.add(TimeUtil.adjust(lastRun, TimeOption.QUARTER));
        break;
      case YEAR:
        ret.add(TimeUtil.adjust(lastRun, TimeOption.YEAR));
        break;
    }
    ret.add(lastRun);

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

    TreeMap<Timestamp, Map<String, MetricValue>> treeMap = new TreeMap<>(new Comparator<Timestamp>() {
      @Override
      public int compare(Timestamp o1, Timestamp o2) {
        return -o1.compareTo(o2);
      }
    });

    for (MetricValue v : values) {
      if (!treeMap.containsKey(v.getDataTime())) {
        treeMap.put(v.getDataTime(), new HashMap<>());
      }
      Map<String, MetricValue> map = treeMap.get(v.getDataTime());
      map.put(v.getDimension(), v);
    }

    Set entries = treeMap.entrySet();
    Iterator iterator = entries.iterator();

    while (iterator.hasNext()) {
      Map.Entry<Timestamp, Map<String, MetricValue>> entry =
          (Map.Entry<Timestamp, Map<String, MetricValue>>) iterator.next();
      Timestamp t0 = entry.getKey();
      Map<String, MetricValue> v0 = entry.getValue();

      if (t0.before(lastRun.getStart())) {
        break;
      }

      Timestamp t1 = null;
      Map<String, MetricValue> v1 = null;

      if (compare.equals(TimeOption.RUN)) {
        if (iterator.hasNext()) {
          Map.Entry<Timestamp, Map<String, MetricValue>> nextEntry =
              (Map.Entry<Timestamp, Map<String, MetricValue>>) iterator.next();
          t1 = nextEntry.getKey();
          v1 = nextEntry.getValue();
        } else {
          ret.setStatus(DqStatus.NO_ENOUGH_DATA);
          return ret;
        }
      } else {
        t1 = TimeUtil.adjust(t0, compare);
        if (treeMap.containsKey(t1)) {
          v1 = treeMap.get(t1);
        } else {
          ret.setStatus(DqStatus.NO_ENOUGH_DATA);
          return ret;
        }
      }

      if (t1 != null && v1 != null) {
        for (String dim : v0.keySet()) {
          MetricValue newValue = v0.get(dim);
          if (v1.containsKey(dim)) {
            MetricValue oldValue = v1.get(dim);
            if (!criteria.check(computeDiff(newValue, oldValue))) {
              ret.setStatus(DqStatus.FAILED);
              failedValues.put(newValue, "Reason - compare with " + oldValue.toString() + " - " + criteria.getDetail());
            }
          } else {
            ret.addMessage("Dimension: " + dim + " does not exist at " + TimeUtil.getSimpleDateFormat(newValue.getTimeGrain()).format(t1));
          }
        }
      }
    }

    if (DqStatus.FAILED.equals(ret.getStatus())) {
      ret.setFailedValues(failedValues);
    } else {
      ret.setStatus(DqStatus.PASSED);
    }


    return ret;
  }

  public Double computeDiff(MetricValue newValue, MetricValue oldValue) {
    return newValue.getValue() - oldValue.getValue();
  }
}
