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


public class MultiproductProjectRecord extends AbstractRecord {

  Integer appId;
  String projectName;
  String scmType;
  String owneType;
  String ownerName;
  String createTime;
  String license;
  String description;
  Integer numOfRepos;
  String repos;
  Long whExecId;

  public MultiproductProjectRecord(Integer appId, String projectName, String scmType, String owneType, String ownerName,
      String createTime, String license, String description, Long whExecId) {
    this.appId = appId;
    this.projectName = projectName;
    this.scmType = scmType;
    this.owneType = owneType;
    this.ownerName = ownerName;
    this.createTime = createTime;
    this.license = license;
    this.description = description;
    this.whExecId = whExecId;
  }

  public void setRepos(Integer numOfRepos, String repos) {
    this.numOfRepos = numOfRepos;
    this.repos = repos;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(whExecId);
    allFields.add(projectName);
    allFields.add(scmType);
    allFields.add(owneType);
    allFields.add(ownerName);
    allFields.add(createTime);
    allFields.add(numOfRepos);
    allFields.add(repos);
    allFields.add(license);
    allFields.add(description);
    return allFields;
  }
}
