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
public class OozieFlowOwnerRecord extends AbstractRecord {
  Integer appId;
  String flowPath;
  String ownerId;
  Long whExecId;

  public OozieFlowOwnerRecord(Integer appId, String flowPath, String ownerId, Long whExecId) {
    this.appId = appId;
    this.flowPath = flowPath;
    this.ownerId = ownerId;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(flowPath);
    allFields.add(ownerId);
    allFields.add(whExecId);
    return allFields;
  }
}
