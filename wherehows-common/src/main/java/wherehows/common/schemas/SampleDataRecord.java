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


/**
 * Created by zsun on 8/19/15.
 */
public class SampleDataRecord implements Record {

  String source;
  String path;
  String refUrn;
  String sampleData;

  /**
   * Used by hdfs
   * @param path
   * @param sampleData
   */
  public SampleDataRecord(String path, List<Object> sampleData) {
    this.source = "hdfs"; // default hdfs
    this.path = path;
    this.sampleData = "{\"sample\": " + sampleData.toString() + " }";
  }

  /**
   * Used by teradata
   * @param source teradata or hdfs
   * @param path
   * @param refUrn
   * @param sampleData
   */
  public SampleDataRecord(String source, String path, String refUrn, String sampleData) {
    this.source = source;
    this.path = path;
    this.refUrn = refUrn;
    this.sampleData = sampleData;
  }

  @Override
  public String toCsvString() {
    String result = source + "://" + path + "\u001a" + refUrn + "\u001a" + sampleData;
    return result;
  }

  @Override
  public String toDatabaseValue() {
    return null;
  }

  public void setAbstractPath(String abstractPath) {
    this.path = abstractPath;
  }
}
