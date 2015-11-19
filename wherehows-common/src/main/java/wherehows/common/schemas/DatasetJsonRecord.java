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

/**
 * This record is used in hadoop job running on gateway.
 * It will generate the json file
 * Created by zsun on 7/7/15.
 */
public class DatasetJsonRecord implements Record {

  String schemaString;
  String abstractPath;
  Long modificationTime;
  String owner;
  String group;
  String permission;
  String codec;
  String storage;
  String data_source;

  public DatasetJsonRecord(String schema, String abstractPath, Long modificationTime, String owner, String group,
    String permission, String codec, String storage, String data_source) {
    this.schemaString = schema;
    this.abstractPath = abstractPath;
    this.modificationTime = modificationTime;
    this.owner = owner;
    this.group = group;
    this.permission = permission;
    this.codec = codec;
    this.storage = storage;
    this.data_source = data_source;
  }

  @Override
  public String toCsvString() {
    // TODO avro and orc are different
    String result = (schemaString.substring(0, schemaString.length() - 1) + ", " +
      "\"uri\":\"hdfs://" + abstractPath + "\",\"attributes\": {" +
      "\"modification_time\":" + modificationTime + "," +
      "\"owner\":\"" + owner + "\"," +
      "\"group\":\"" + group + "\"," +
      "\"file_permission\":\"" + permission + "\"," +
      // "\"size\":" + data_size + "," +
      "\"codec\": \"" + codec + "\"," +
      "\"storage_type\": \"" + storage + "\"," +
      "\"source\":\"" + data_source + "\"}" +
      "}");
    return result;
  }

  @Override
  public String toDatabaseValue() {
    return null;
  }

  public void setAbstractPath(String abstractPath) {
    this.abstractPath = abstractPath;
  }
}
