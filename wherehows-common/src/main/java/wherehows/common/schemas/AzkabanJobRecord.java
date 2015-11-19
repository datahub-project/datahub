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
public class AzkabanJobRecord extends AbstractRecord {
  Integer appId;
  String flowPath;
  Integer sourceVersion;
  String jobName;
  String jobPath;
  String jobType;
  Character isCurrent;
  Long whExecId;

  public AzkabanJobRecord(Integer appId, String flowPath, Integer sourceVersion, String jobName, String jobPath,
    String jobType, Character isCurrent, Long whExecId) {
    this.appId = appId;
    this.flowPath = flowPath;
    this.sourceVersion = sourceVersion;
    this.jobName = jobName;
    this.jobPath = jobPath;
    this.jobType = jobType;
    this.isCurrent = isCurrent;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowPath);
    allFields.add(sourceVersion);
    allFields.add(jobName);
    allFields.add(jobPath);
    allFields.add(jobType);
    allFields.add(isCurrent);
    allFields.add(whExecId);
    return allFields;
  }

}
