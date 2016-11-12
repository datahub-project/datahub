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
import java.util.Map;


public class DatasetOriginalSchemaRecord extends AbstractRecord {

  String format;
  String text;
  Map<String, String> checksum;

  @Override
  public List<Object> fillAllFields() {
    return null;
  }

  public DatasetOriginalSchemaRecord() {
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public Map<String, String> getChecksum() {
    return checksum;
  }

  public void setChecksum(Map<String, String> checksum) {
    this.checksum = checksum;
  }
}
