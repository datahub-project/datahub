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

public class SCMOwnerRecord extends AbstractRecord {

  String scmUrl;
  String databaseName;
  String databaseType;
  String appName;
  String filePath;
  String committers;
  String scmType;

  public SCMOwnerRecord(String scmUrl, String databaseName, String databaseType, String appName, String filePath,
      String committers, String scmType) {
    this.scmUrl = scmUrl;
    this.databaseName = databaseName;
    this.databaseType = databaseType;
    this.appName = appName;
    this.filePath = filePath;
    this.committers = committers;
    this.scmType = scmType;

  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(scmUrl);
    allFields.add(databaseName);
    allFields.add(databaseType);
    allFields.add(appName);
    allFields.add(filePath);
    allFields.add(committers);
    allFields.add(scmType);
    return allFields;
  }
}
