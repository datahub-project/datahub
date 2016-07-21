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
 * Used to generate one record in the dataset schema data file. Load into staging table.
 * Created by zsun on 8/25/15.
 */
public class HiveDependencyInstanceRecord extends AbstractRecord {
  String objectType;
  String objectSubType;
  String objectName;
  String mapPhrase;
  String isIdentialMap;
  String mappedObjectType;
  String mappedObjectSubType;
  String mappedObjectName;
  String description;
  String objectUrn;
  String mappedObjectUrn;

  public HiveDependencyInstanceRecord(String objectType, String objectSubType, String objectName, String objectUrn,
                               String mapPhrase, String isIdentialMap, String mappedObjectType,
                               String mappedObjectSubType, String mappedObjectName,
                               String mappedObjectUrn, String description) {
    this.objectType = objectType;
    this.objectSubType = objectSubType;
    this.objectName = objectName;
    this.objectUrn = objectUrn;
    this.mapPhrase = mapPhrase;
    this.isIdentialMap = isIdentialMap;
    this.mappedObjectType = mappedObjectType;
    this.mappedObjectSubType = mappedObjectSubType;
    this.mappedObjectName = mappedObjectName;
    this.mappedObjectUrn = mappedObjectUrn;
    this.description = description;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(objectType);
    allFields.add(objectSubType);
    allFields.add(objectName);
    allFields.add(objectUrn);
    allFields.add(mapPhrase);
    allFields.add(isIdentialMap);
    allFields.add(mappedObjectType);
    allFields.add(mappedObjectSubType);
    allFields.add(mappedObjectName);
    allFields.add(mappedObjectUrn);
    allFields.add(description);
    return allFields;
  }


}
