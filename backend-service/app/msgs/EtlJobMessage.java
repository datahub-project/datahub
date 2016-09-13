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
package msgs;

import com.fasterxml.jackson.databind.JsonNode;
import metadata.etl.models.EtlType;
import metadata.etl.models.EtlJobName;
import metadata.etl.models.RefIdType;


/**
 * Created by zechen on 9/4/15.
 */
public class EtlJobMessage {
  Long whEtlExecId;
  EtlJobName etlJobName;
  EtlType etlType;
  Integer refId;
  RefIdType refIdType;
  JsonNode inputParams;
  Integer whEtlJobId;
  String cmdParam;

  public EtlJobMessage(EtlJobName etlJobName, EtlType etlType, Integer whEtlJobId, Integer refId, RefIdType refIdType, String cmdParam) {
    this.etlJobName = etlJobName;
    this.etlType = etlType;
    this.refId = refId;
    this.refIdType = refIdType;
    this.whEtlJobId = whEtlJobId;
    this.cmdParam = cmdParam;
  }

  public Long getWhEtlExecId() {
    return whEtlExecId;
  }

  public void setWhEtlExecId(Long whEtlExecId) {
    this.whEtlExecId = whEtlExecId;
  }

  public EtlJobName getEtlJobName() {
    return etlJobName;
  }

  public EtlType getEtlType() {
    return etlType;
  }

  public JsonNode getInputParams() {
    return inputParams;
  }

  public void setInputParams(JsonNode inputParams) {
    this.inputParams = inputParams;
  }

  public Integer getWhEtlJobId() {
    return whEtlJobId;
  }

  public Integer getRefId() {
    return refId;
  }

  public void setRefId(Integer refId) {
    this.refId = refId;
  }

  public RefIdType getRefIdType() {
    return refIdType;
  }

  public void setRefIdType(RefIdType refIdType) {
    this.refIdType = refIdType;
  }

  public String getCmdParam() {
    return cmdParam;
  }

  public void setCmdParam(String cmdParam) {
    this.cmdParam = cmdParam;
  }

  /**
   * For debuging
   * @return
   */
  public String toDebugString() {
    return String.format("(jobType:%1s refIdType:%2s refId:%3d whEtlJobId:%4d whEtlExecId:%5d)",
                         this.etlJobName.name(), this.refIdType, this.refId,
                         this.whEtlJobId, this.whEtlExecId);
  }
}
