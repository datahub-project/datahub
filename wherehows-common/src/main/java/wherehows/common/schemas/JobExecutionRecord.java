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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;


public class JobExecutionRecord extends AbstractRecord {

  Integer appId;
  String name;
  String node; //
  String server; //
  Long definitionId;
  Long executionId;
  String executionGuid;
  String topLevelFlowName; //
  Long flowDefinitionId;
  Long flowExecutionId;
  String flowExecutionGuid;
  Integer attempt;
  Long startTime;
  Long suspendTime;
  Long endTime;
  String state;
  Long logTime;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"app_id", "job_name", "node", "server", "job_id", "job_exec_id", "job_exec_uuid", "flow_name",
        "flow_id", "flow_exec_id", "flow_exec_uuid", "attempt_id", "start_time", "end_time", "job_exec_status",
        "created_time"};
  }

  @JsonIgnore
  public static String[] dbColumns() {
    return new String[]{"app_id", "job_name", "job_id", "job_exec_id", "job_exec_uuid", "flow_id", "flow_exec_id",
        "attempt_id", "start_time", "end_time", "job_exec_status", "created_time"};
  }

  @JsonIgnore
  public Object[] dbValues() {
    return new Object[]{appId, name, definitionId, executionId, executionGuid, flowDefinitionId, flowExecutionId,
        attempt, (int) (startTime / 1000), (int) (endTime / 1000), state, (int) (logTime / 1000)};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public JobExecutionRecord() {
  }

  public Integer getAppId() {
    return appId;
  }

  public void setAppId(Integer appId) {
    this.appId = appId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getNode() {
    return node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public Long getDefinitionId() {
    return definitionId;
  }

  public void setDefinitionId(Long definitionId) {
    this.definitionId = definitionId;
  }

  public Long getExecutionId() {
    return executionId;
  }

  public void setExecutionId(Long executionId) {
    this.executionId = executionId;
  }

  public String getExecutionGuid() {
    return executionGuid;
  }

  public void setExecutionGuid(String executionGuid) {
    this.executionGuid = executionGuid;
  }

  public String getTopLevelFlowName() {
    return topLevelFlowName;
  }

  public void setTopLevelFlowName(String topLevelFlowName) {
    this.topLevelFlowName = topLevelFlowName;
  }

  public Long getFlowDefinitionId() {
    return flowDefinitionId;
  }

  public void setFlowDefinitionId(Long flowDefinitionId) {
    this.flowDefinitionId = flowDefinitionId;
  }

  public Long getFlowExecutionId() {
    return flowExecutionId;
  }

  public void setFlowExecutionId(Long flowExecutionId) {
    this.flowExecutionId = flowExecutionId;
  }

  public String getFlowExecutionGuid() {
    return flowExecutionGuid;
  }

  public void setFlowExecutionGuid(String flowExecutionGuid) {
    this.flowExecutionGuid = flowExecutionGuid;
  }

  public Integer getAttempt() {
    return attempt;
  }

  public void setAttempt(Integer attempt) {
    this.attempt = attempt;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getSuspendTime() {
    return suspendTime;
  }

  public void setSuspendTime(Long suspendTime) {
    this.suspendTime = suspendTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public Long getLogTime() {
    return logTime;
  }

  public void setLogTime(Long logTime) {
    this.logTime = logTime;
  }
}
