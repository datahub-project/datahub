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
import java.util.Arrays;
import java.util.List;


/**
 * Used to generate one record in the dataset field data file
 * Created by zsun on 8/25/15.
 */
public class DatasetFieldRecord implements Record {

  String urn;
  Integer sortId;
  Integer parentSortId;
  String parentPath;
  String fieldName;
  String fieldLabel;
  String dataType;
  String isNullable;
  String isIndexed;
  String isPartitioned;
  String defaultValue;
  Integer dataSize;
  String namespace;
  String description;

  List<Object> allFields;
  char SEPR = 0x001A;

  public DatasetFieldRecord(String urn, Integer sortId, Integer parentSortId, String parentPath, String fieldName, String fieldLabel,
    String dataType, String isNullable, String isIndexed, String isPartitioned, String defaultValue, Integer dataSize, String namespace, String description) {

    this.urn = urn;
    this.sortId = sortId;
    this.parentSortId = parentSortId;
    this.parentPath = parentPath;
    this.fieldName = fieldName;
    this.fieldLabel = fieldLabel;
    this.dataType = dataType;
    this.dataSize = dataSize;
    this.isNullable = isNullable;
    this.isIndexed = isIndexed;
    this.isPartitioned = isPartitioned;
    this.defaultValue = defaultValue;
    this.namespace = namespace;
    this.description = description;

    this.allFields = new ArrayList<Object>();
    this.allFields.add(urn);
    this.allFields.add(sortId);
    this.allFields.add(parentSortId);
    this.allFields.add(parentPath);
    this.allFields.add(fieldName);
    this.allFields.add(dataType);
    this.allFields.add(dataSize);
    this.allFields.add(isNullable);
    this.allFields.add(isIndexed);
    this.allFields.add(isPartitioned);
    this.allFields.add(defaultValue);
    this.allFields.add(namespace);
    this.allFields.add(description);
  }

  public DatasetFieldRecord(Object[] allFields) {
    // TODO need to make a check
    // sequence should be : uri, sort_id, parent_sort_id, prefix, column_name, data_type, is_nullable, default_value, data_size, namespace, description
    this.allFields = new ArrayList<Object>(Arrays.asList(allFields));
  }

  @Override
  public String toCsvString() {
    StringBuilder sb = new StringBuilder();
    for (Object o : allFields) {
      // comment have new line
      if (o != null && o.toString().contains("\n")) {
        o = o.toString().replace("\n", "\\n");
      }
      sb.append(o);
      sb.append(SEPR);
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  @Override
  public String toDatabaseValue() {
    return null;
  }
}
