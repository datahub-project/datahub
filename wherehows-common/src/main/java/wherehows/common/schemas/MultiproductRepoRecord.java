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


public class MultiproductRepoRecord extends AbstractRecord {

  Integer appId;
  String scmRepoFullname;
  String scmType;
  Integer repoId;
  String project;
  String ownerType;
  String ownerName;
  String multiproductName;
  String productType;
  String productVersion;
  String namespace;
  Long whExecId;

  public MultiproductRepoRecord(Integer appId, String scmRepoFullname, String scmType, Integer repoId,
      String project, String ownerType, String ownerName, Long whExecId) {
    this.appId = appId;
    this.scmRepoFullname = scmRepoFullname;
    this.scmType = scmType;
    this.repoId = repoId;
    this.project = project;
    this.ownerType = ownerType;
    this.ownerName = ownerName;
    this.whExecId = whExecId;
  }

  public void setMultiproductInfo(String multiproductName, String productType,
      String productVersion, String namespace) {
    this.multiproductName = multiproductName;
    this.productType = productType;
    this.productVersion = productVersion;
    this.namespace = namespace;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(appId);
    allFields.add(whExecId);
    allFields.add(scmRepoFullname);
    allFields.add(scmType);
    allFields.add(repoId);
    allFields.add(project);
    allFields.add(ownerType);
    allFields.add(ownerName);
    allFields.add(multiproductName);
    allFields.add(productType);
    allFields.add(productVersion);
    allFields.add(namespace);
    return allFields;
  }

  public String getScmRepoFullname() {
    return scmRepoFullname;
  }

  public String getScmType() {
    return scmType;
  }

  public Integer getRepoId() {
    return repoId;
  }
}
