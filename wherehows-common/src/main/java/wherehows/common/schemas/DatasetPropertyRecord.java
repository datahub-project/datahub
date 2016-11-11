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


public class DatasetPropertyRecord extends AbstractRecord {

  ChangeAuditStamp changeAuditStamp;
  String nativeType;
  String uri;
  DatasetCaseSensitiveRecord caseSensitivity;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetPropertyRecord() {
  }

  public ChangeAuditStamp getChangeAuditStamp() {
    return changeAuditStamp;
  }

  public void setChangeAuditStamp(ChangeAuditStamp changeAuditStamp) {
    this.changeAuditStamp = changeAuditStamp;
  }

  public String getNativeType() {
    return nativeType;
  }

  public void setNativeType(String nativeType) {
    this.nativeType = nativeType;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    this.uri = uri;
  }

  public DatasetCaseSensitiveRecord getCaseSensitivity() {
    return caseSensitivity;
  }

  public void setCaseSensitivity(DatasetCaseSensitiveRecord caseSensitivity) {
    this.caseSensitivity = caseSensitivity;
  }
}
