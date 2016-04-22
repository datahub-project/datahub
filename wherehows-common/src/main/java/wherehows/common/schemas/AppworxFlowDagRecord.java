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


public class AppworxFlowDagRecord extends AbstractRecord {
  Integer appId;
  Long flowId;
  String flowPath;
  Integer flowVersion;
  String sourceJobPath;
  String targetJobPath;
  Long whExecId;

  public AppworxFlowDagRecord(Integer appId, Long flowId, String flowPath, Integer flowVersion, String sourceJobPath,
    String targetJobPath, Long whExecId) {
    this.appId = appId;
    this.flowId = flowId;
    this.flowPath = flowPath;
    this.flowVersion = flowVersion;
    this.sourceJobPath = sourceJobPath;
    this.targetJobPath = targetJobPath;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowId);
    allFields.add(flowPath);
    allFields.add(flowVersion);
    allFields.add(sourceJobPath);
    allFields.add(targetJobPath);
    allFields.add(whExecId);
    return allFields;
  }
}
