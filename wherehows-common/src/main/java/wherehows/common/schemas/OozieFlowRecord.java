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
public class OozieFlowRecord extends AbstractRecord {
  Integer appId;
  String flowName;
  String flowPath;
  Integer flowLevel;
  String sourceVersion;
  Long sourceCreatedTime;
  Long sourceModifiedTime;
  Long whExecId;

  public OozieFlowRecord(Integer appId, String flowName, String flowPath, Integer flowLevel, String sourceVersion,
    Long sourceCreatedTime, Long sourceModifiedTime, Long whExecId) {
    this.appId = appId;
    this.flowName = flowName;
    this.flowPath = flowPath;
    this.flowLevel = flowLevel;
    this.sourceVersion = sourceVersion;
    this.sourceCreatedTime = sourceCreatedTime;
    this.sourceModifiedTime = sourceModifiedTime;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowName);
    allFields.add(flowPath);
    allFields.add(flowLevel);
    allFields.add(sourceVersion);
    allFields.add(sourceCreatedTime);
    allFields.add(sourceModifiedTime);
    allFields.add(whExecId);
    return allFields;
  }
}
