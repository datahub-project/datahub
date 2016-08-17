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


public class DatasetFieldIndexRecord extends AbstractRecord {

  Integer position;
  String fieldPath;
  String descend;
  Integer prefixLength;
  String filter;

  @Override
  public String[] getDbColumnNames() {
    return new String[]{"position", "field_path", "descend", "prefix_length", "filter"};
  }

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetFieldIndexRecord() {
  }

  @Override
  public String toString() {
    try {
      return this.getFieldValueMap().toString();
    } catch (IllegalAccessException ex) {
      return null;
    }
  }

  public Integer getPosition() {
    return position;
  }

  public void setPosition(Integer position) {
    this.position = position;
  }

  public String getFieldPath() {
    return fieldPath;
  }

  public void setFieldPath(String fieldPath) {
    this.fieldPath = fieldPath;
  }

  public String getDescend() {
    return descend;
  }

  public void setDescend(String descend) {
    this.descend = descend;
  }

  public Integer getPrefixLength() {
    return prefixLength;
  }

  public void setPrefixLength(Integer prefixLength) {
    this.prefixLength = prefixLength;
  }

  public String getFilter() {
    return filter;
  }

  public void setFilter(String filter) {
    this.filter = filter;
  }
}
