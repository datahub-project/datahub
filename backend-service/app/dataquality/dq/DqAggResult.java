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

import java.util.List;


/**
 * Created by zechen on 8/12/15.
 */
public class DqAggResult {
  DqStatus status;
  List<DqMetricResult> metricResults;

  public DqAggResult() {
  }

  public DqStatus getStatus() {
    return status;
  }

  public void setStatus(DqStatus status) {
    this.status = status;
  }

  public List<DqMetricResult> getMetricResults() {
    return metricResults;
  }

  public void setMetricResults(List<DqMetricResult> metricResults) {
    this.metricResults = metricResults;
  }
}
