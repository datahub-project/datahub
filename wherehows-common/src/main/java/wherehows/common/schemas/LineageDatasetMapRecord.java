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


public class LineageDatasetMapRecord extends AbstractRecord {

  String mapDirectionType;
  List<LineageFieldRecord> fieldLineage;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public LineageDatasetMapRecord() {
  }

  public String getMapDirectionType() {
    return mapDirectionType;
  }

  public void setMapDirectionType(String mapDirectionType) {
    this.mapDirectionType = mapDirectionType;
  }

  public List<LineageFieldRecord> getFieldLineage() {
    return fieldLineage;
  }

  public void setFieldLineage(List<LineageFieldRecord> fieldLineage) {
    this.fieldLineage = fieldLineage;
  }
}
