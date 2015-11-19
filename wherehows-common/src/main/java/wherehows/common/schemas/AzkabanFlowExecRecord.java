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
public class AzkabanFlowExecRecord extends AbstractRecord {
  Integer appId;
  String flowName;
  String flowPath;
  Integer sourceVersion;
  Integer flowExecId;
  String flowExecStatus;
  Integer attemptId;
  String executedBy;
  Long startTime;
  Long endTime;
  Long whExecId;

  public AzkabanFlowExecRecord(Integer appId, String flowName, String flowPath, Integer sourceVersion, Integer flowExecId,
    String flowExecStatus, Integer attemptId, String executedBy, Long startTime, Long endTime, Long whExecId) {
    this.appId = appId;
    this.flowName = flowName;
    this.flowPath = flowPath;
    this.sourceVersion = sourceVersion;
    this.flowExecId = flowExecId;
    this.flowExecStatus = flowExecStatus;
    this.attemptId = attemptId;
    this.executedBy = executedBy;
    this.startTime = startTime;
    this.endTime = endTime;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowName);
    allFields.add(flowPath);
    allFields.add(sourceVersion);
    allFields.add(flowExecId);
    allFields.add(flowExecStatus);
    allFields.add(attemptId);
    allFields.add(executedBy);
    allFields.add(startTime);
    allFields.add(endTime);
    allFields.add(whExecId);
    return allFields;
  }
}
