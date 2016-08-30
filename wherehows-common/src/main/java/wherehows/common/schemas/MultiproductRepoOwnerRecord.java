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


public class MultiproductRepoOwnerRecord extends AbstractRecord {

  Integer appId;
  String scmRepoFullname;
  String scmType;
  Integer repoId;
  String ownerType;
  String ownerName;
  Integer sortId;
  String paths;
  Long whExecId;

  public MultiproductRepoOwnerRecord(Integer appId, String scmRepoFullname, String scmType, Integer repoId,
      String ownerType, String ownerName, Integer sortId, String paths, Long whExecId) {
    this.appId = appId;
    this.scmRepoFullname = scmRepoFullname;
    this.scmType = scmType;
    this.repoId = repoId;
    this.ownerType = ownerType;
    this.ownerName = ownerName;
    this.sortId = sortId;
    this.paths = paths;
    this.whExecId = whExecId;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(whExecId);
    allFields.add(scmRepoFullname);
    allFields.add(scmType);
    allFields.add(repoId);
    allFields.add(ownerType);
    allFields.add(ownerName);
    allFields.add(sortId);
    allFields.add(paths);
    return allFields;
  }
}
