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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import wherehows.common.utils.StringUtil;


/**
 * Created by zechen on 9/16/15.
 */
public abstract class AbstractRecord implements Record {
  @JsonIgnore
  char SEPR = 0x001A;

  @Override
  public String toCsvString() {
    List<Object> allFields = fillAllFields();
    StringBuilder sb = new StringBuilder();
    for (Object o : allFields) {
      sb.append(o);
      sb.append(SEPR);
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  @Override
  public String toDatabaseValue() {
    List<Object> allFields = fillAllFields();
    StringBuilder sb = new StringBuilder();
    for (Object o : allFields) {
      sb.append(StringUtil.toDbString(o));
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  public abstract List<Object> fillAllFields();

  /**
   * return all declared fields of the class, exclude inherited fields
   * @return Field[]
   */
  @JsonIgnore
  public Field[] getAllFields() {
    return this.getClass().getDeclaredFields();
  }

  /**
   * return values of all declared fields as Object[]
   * @return Object[]
   * @throws IllegalAccessException
   */
  @JsonIgnore
  public Object[] getAllValues()
      throws IllegalAccessException {
    final Field[] fields = this.getAllFields();
    final Object[] values = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) {
      fields[i].setAccessible(true);
      values[i] = fields[i].get(this);
    }
    return values;
  }

  /**
   * return the corresponding database column names to the class fields
   * @return
   */
  @JsonIgnore
  public String[] getDbColumnNames() {
    return null;
  }

  /**
   * return values of declared fields, and transform record, collection, map and array into string
   * Primitive data types and other structures are unchanged.
   * @return Object[]
   * @throws IllegalAccessException
   */
  @JsonIgnore
  public Object[] getAllValuesToString()
      throws IllegalAccessException {
    final Object[] values = getAllValues();
    for (int i = 0; i < values.length; i++) {
      values[i] = StringUtil.objectToJsonString(values[i]);
    }
    return values;
  }

  /**
   * get the field-value map of the record
   * use db column name as field name, if not available, use class field names
   * @return Map: String-Object
   * @throws IllegalAccessException
   */
  @JsonIgnore
  public Map<String, Object> getFieldValueMap()
      throws IllegalAccessException {
    String[] columns = this.getDbColumnNames();
    Object[] values = this.getAllValues();

    Map<String, Object> map = new HashMap<>();
    if (columns != null && columns.length == values.length) {
      for (int i = 0; i < columns.length; i++) {
        map.put(columns[i], values[i]);
      }
    } else {
      Field[] fields = this.getAllFields();
      for (int i = 0; i < fields.length; i++) {
        map.put(fields[i].getName(), values[i]);
      }
    }
    return map;
  }

  /**
   * Convert database query result into AbstractRecord
   * use column name to field mapping to assign each field
   * @param map <String, Object>
   */
  @JsonIgnore
  public void convertToRecord(Map<String, Object> map) {
    final Field[] fields = getAllFields();
    final String[] columns = getDbColumnNames();
    if (fields.length != columns.length) {
      return;
    }

    final ObjectMapper om = new ObjectMapper();

    for (int i = 0; i < columns.length; i++) {
      final Class<?> type = fields[i].getType();
      final Object value = map.get(columns[i]);
      try {
        if (value == null) {
        } else if (Collection.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type)
            || Object[].class.isAssignableFrom(type) || Record.class.isAssignableFrom(type)) {
          fields[i].set(this, om.readValue((String) value, om.constructType(type)));
        } else if (Integer.class.isAssignableFrom(type)) {
          // may need to convert from Long (database unsigned int) to Integer
          fields[i].set(this, StringUtil.toInt(value));
        } else if (value instanceof Date) {
          fields[i].set(this, value.toString());
        } else {
          fields[i].set(this, value);
        }
      } catch (IllegalAccessException | IOException ex) {
      }
    }
  }
}
