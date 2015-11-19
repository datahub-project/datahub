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
 * Created by zechen on 10/3/15.
 */
public class OozieFlowExecRecord extends AbstractRecord {
  Integer appId;
  String flowName;
  String flowPath;
  String flowExecUuid;
  String sourceVersion;
  String flowExecStatus;
  Integer attemptId;
  String executedBy;
  Long startTime;
  Long endTime;
  Long whExecId;

  public OozieFlowExecRecord(Integer appId, String flowName, String flowPath, String flowExecUuid, String sourceVersion,
    String flowExecStatus, Integer attemptId, String executedBy, Long startTime, Long endTime, Long whExecId) {
    this.appId = appId;
    this.flowName = flowName;
    this.flowPath = flowPath;
    this.flowExecUuid = flowExecUuid;
    this.sourceVersion = sourceVersion;
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
    allFields.add(flowExecUuid);
    allFields.add(sourceVersion);
    allFields.add(flowExecStatus);
    allFields.add(attemptId);
    allFields.add(executedBy);
    allFields.add(startTime);
    allFields.add(endTime);
    allFields.add(whExecId);
    return allFields;
  }
}
