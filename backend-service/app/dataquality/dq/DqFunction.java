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
import java.util.List;


/**
 * Created by zechen on 8/6/15.
 */
public interface DqFunction {

  List<TimeRange> getTimeRanges(List<TimeRange> lastTwoRun);

  DqRuleResult computeAndValidate(List<MetricValue> values, DqCriteria criteria);

}
