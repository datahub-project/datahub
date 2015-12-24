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
import java.util.Map;


/**
 * Created by zechen on 8/7/15.
 */
public class DqDiffPct extends DqDiff{

  public DqDiffPct(Map<String, Object> params) {
    super(params);
    description = "Difference in percentage between " + compare.toString();
  }

  @Override
  public Double computeDiff(MetricValue newValue, MetricValue oldValue) {
    if (oldValue.getValue().equals(0.0)) {
      return 0.0;
    }

    return (newValue.getValue() - oldValue.getValue()) * 100.0 / oldValue.getValue();
  }
}
