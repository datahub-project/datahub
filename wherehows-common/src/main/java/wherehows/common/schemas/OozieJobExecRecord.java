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
public class OozieJobExecRecord extends AbstractRecord {
  Integer appId;
  String flowPath;
  String flowExecUuid;
  String sourceVersion;
  String jobName;
  String jobPath;
  String jobExecUuid;
  String jobExecStatus;
  Integer attemptId;
  Integer startTime;
  Integer endTime;
  Long whExecId;

  public OozieJobExecRecord(Integer appId, String jobName, String flowExecUuid, Integer startTime, Integer endTime,
    String jobExecStatus, String flowPath) {
    this.appId = appId;
    this.jobName = jobName;
    this.flowExecUuid = flowExecUuid;
    this.startTime = startTime;
    this.endTime = endTime;
    this.jobExecStatus = jobExecStatus;
    this.flowPath = flowPath;
  }

  public OozieJobExecRecord(Integer appId, String flowPath, String flowExecUuid,
    String sourceVersion, String jobName, String jobPath, String jobExecUuid,  String jobExecStatus,
    Integer attemptId, Integer startTime, Integer endTime, Long whExecId) {
    this.appId = appId;
    this.flowPath = flowPath;
    this.flowExecUuid = flowExecUuid;
    this.sourceVersion = sourceVersion;
    this.jobName = jobName;
    this.jobPath = jobPath;
    this.jobExecUuid = jobExecUuid;
    this.jobExecStatus = jobExecStatus;
    this.attemptId = attemptId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowPath);
    allFields.add(flowExecUuid);
    allFields.add(sourceVersion);
    allFields.add(jobName);
    allFields.add(jobPath);
    allFields.add(jobExecUuid);
    allFields.add(jobExecStatus);
    allFields.add(attemptId);
    allFields.add(startTime);
    allFields.add(endTime);
    allFields.add(whExecId);
    return allFields;
  }

  public Integer getAppId() {
    return appId;
  }

  public String getJobName() {
    return jobName;
  }

  public String getJobExecStatus() {
    return jobExecStatus;
  }

  public Integer getStartTime() {
    return startTime;
  }

  public Integer getEndTime() {
    return endTime;
  }

  public String getFlowPath() {
    return flowPath;
  }

  public String getJobExecUuid() {
    return jobExecUuid;
  }

  public String getFlowExecUuid() {
    return flowExecUuid;
  }
}

