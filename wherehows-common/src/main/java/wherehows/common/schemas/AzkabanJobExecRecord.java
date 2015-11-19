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
public class AzkabanJobExecRecord extends AbstractRecord {
  Integer appId;
  String flowPath;
  Integer sourceVersion;
  Long flowExecId;
  String jobName;
  String jobPath;
  Long jobExecId;
  String jobExecStatus;
  Integer attemptId;
  Integer startTime;
  Integer endTime;
  Long whExecId;

  public AzkabanJobExecRecord(Integer appId, String jobName, Long flowExecId, Integer startTime, Integer endTime,
    String jobExecStatus, String flowPath) {
    this.appId = appId;
    this.jobName = jobName;
    this.flowExecId = flowExecId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.jobExecStatus = jobExecStatus;
    this.flowPath = flowPath;
  }

  public AzkabanJobExecRecord(Integer appId, String flowPath, Integer sourceVersion, Long flowExecId,
    String jobName, String jobPath, Long jobExecId,  String jobExecStatus,
    Integer attemptId, Integer startTime, Integer endTime, Long whExecId) {
    this.appId = appId;
    this.flowPath = flowPath;
    this.sourceVersion = sourceVersion;
    this.flowExecId = flowExecId;
    this.jobName = jobName;
    this.jobPath = jobPath;
    this.jobExecId = jobExecId;
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
    allFields.add(sourceVersion);
    allFields.add(flowExecId);
    allFields.add(jobName);
    allFields.add(jobPath);
    allFields.add(jobExecId);
    allFields.add(jobExecStatus);
    allFields.add(attemptId);
    allFields.add(startTime);
    allFields.add(endTime);
    allFields.add(whExecId);
    return allFields;
  }

  public void setJobExecId(Long jobExecId) {
    this.jobExecId = jobExecId;
  }

  public Integer getAppId() {
    return appId;
  }
  public Long getFlowExecId() {
    return flowExecId;
  }

  public String getJobName() {
    return jobName;
  }

  public Long getJobExecId() {
    return jobExecId;
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

  /**
   * For debugging print out
   * @return
   */
  public String toString() {
    return "appId:" + this.appId + "\tflowPath:" + this.flowPath + "\tflowExecId:" + this.flowExecId + "\tjobname:" + this.jobName;
  }
}
