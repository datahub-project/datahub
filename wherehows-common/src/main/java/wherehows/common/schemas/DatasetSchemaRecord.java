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
import java.util.List;


/**
 * Used to generate one record in the dataset schema data file. Load into staging table.
 * Created by zsun on 8/25/15.
 */
public class DatasetSchemaRecord extends AbstractRecord {
  String name;
  String schema;
  String properties;
  String fields;
  String urn;
  String source;
  String datasetType;
  String storageType;
  String samplePartitionFullPath;
  Integer sourceCreated;
  Integer sourceModified;

  public DatasetSchemaRecord(String name, String schema, String properties, String fields, String urn, String source,
                             String samplePartitionFullPath, Integer sourceCreated, Integer sourceModified) {
    this.name = name;
    this.schema = schema;
    this.properties = properties;
    this.fields = fields;
    this.urn = urn;
    this.source = source;
    this.datasetType = null;
    this.storageType = null;
    this.samplePartitionFullPath = samplePartitionFullPath;
    this.sourceCreated = sourceCreated;
    this.sourceModified = sourceModified;

  }

  public DatasetSchemaRecord(String name, String schema, String properties, String fields, String urn, String source,
                             String datasetType, String storageType, String samplePartitionFullPath,
    Integer sourceCreated, Integer sourceModified) {
    this.name = name;
    this.schema = schema;
    this.properties = properties;
    this.fields = fields;
    this.urn = urn;
    this.source = source;
    this.datasetType = datasetType;
    this.storageType = storageType;
    this.samplePartitionFullPath = samplePartitionFullPath;
    this.sourceCreated = sourceCreated;
    this.sourceModified = sourceModified;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(name);
    allFields.add(schema);
    allFields.add(properties);
    allFields.add(fields);
    allFields.add(urn);
    allFields.add(source);
    allFields.add(datasetType);
    allFields.add(storageType);
    allFields.add(samplePartitionFullPath);
    allFields.add(sourceCreated);
    allFields.add(sourceModified);
    return allFields;
  }


}
