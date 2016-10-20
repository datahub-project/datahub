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
package wherehows.common.schemas;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by zechen on 9/16/15.
 */
public class AzkabanFlowScheduleRecord extends AbstractRecord {
  Integer appId;
  String flowPath;
  String unit;
  Integer frequency;
  String cronExpression;
  Long effectiveStartTime;
  Long effectiveEndTime;
  String refId;
  Long whExecId;

  public AzkabanFlowScheduleRecord(Integer appId, String flowPath, String unit, Integer frequency,
      String cronExpression, Long effectiveStartTime, Long effectiveEndTime, String refId, Long whExecId) {
    this.appId = appId;
    this.flowPath = flowPath;
    this.unit = unit;
    this.frequency = frequency;
    this.cronExpression = cronExpression;
    this.effectiveStartTime = effectiveStartTime;
    this.effectiveEndTime = effectiveEndTime;
    this.refId = refId;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowPath);
    allFields.add(unit);
    allFields.add(frequency);
    allFields.add(cronExpression);
    allFields.add(effectiveStartTime);
    allFields.add(effectiveEndTime);
    allFields.add(refId);
    allFields.add(whExecId);
    return allFields;
  }
}
