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


public class DatasetIdentifier extends AbstractRecord {

  String dataPlatformUrn;
  String nativeName;
  String dataOrigin;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetIdentifier() {
  }

  public String getDataPlatformUrn() {
    return dataPlatformUrn;
  }

  public void setDataPlatformUrn(String dataPlatformUrn) {
    this.dataPlatformUrn = dataPlatformUrn;
  }

  public String getNativeName() {
    return nativeName;
  }

  public void setNativeName(String nativeName) {
    this.nativeName = nativeName;
  }

  public String getDataOrigin() {
    return dataOrigin;
  }

  public void setDataOrigin(String dataOrigin) {
    this.dataOrigin = dataOrigin;
  }
}
