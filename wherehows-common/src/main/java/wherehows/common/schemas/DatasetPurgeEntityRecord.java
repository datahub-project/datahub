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

import java.util.List;


public class DatasetPurgeEntityRecord extends AbstractRecord {

  String identifierType;
  String identifierField;
  String logicalType;
  Boolean isSubject;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetPurgeEntityRecord() {
  }

  public String getIdentifierType() {
    return identifierType;
  }

  public void setIdentifierType(String identifierType) {
    this.identifierType = identifierType;
  }

  public String getIdentifierField() {
    return identifierField;
  }

  public void setIdentifierField(String identifierField) {
    this.identifierField = identifierField;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public void setLogicalType(String logicalType) {
    this.logicalType = logicalType;
  }

  public Boolean getIsSubject() {
    return isSubject;
  }

  public void setIsSubject(Boolean isSubject) {
    this.isSubject = isSubject;
  }
}
