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
public class DatasetInstanceRecord extends AbstractRecord {
  String datasetUrn;
  String deploymentTier;
  String dataCenter;
  String serverCluster;
  String slice;
  Integer statusId;
  String nativeName;
  String logicalName;
  String version;
  Long instanceCreatedUnixtime;
  String schema;
  String viewExpandedText;
  String abstractedDatasetUrn;

  public DatasetInstanceRecord(String datasetUrn, String deploymentTier, String dataCenter,
                               String serverCluster, String slice, Integer statusId, String nativeName,
                               String logicalName, String version, Long instanceCreatedUnixtime,
                               String schema, String viewExpandedText, String abstractedDatasetUrn) {
    this.datasetUrn = datasetUrn;
    this.deploymentTier = deploymentTier;
    this.dataCenter = dataCenter;
    this.serverCluster = serverCluster;
    this.slice = slice;
    this.statusId = statusId;
    this.nativeName = nativeName;
    this.logicalName = logicalName;
    this.version = version;
    this.instanceCreatedUnixtime = instanceCreatedUnixtime;
    this.schema = schema;
    this.viewExpandedText = viewExpandedText;
    this.abstractedDatasetUrn = abstractedDatasetUrn;
  }

  @Override
  public List<Object> fillAllFields() {
    List<Object> allFields = new ArrayList<>();
    allFields.add(datasetUrn);
    allFields.add(deploymentTier);
    allFields.add(dataCenter);
    allFields.add(serverCluster);
    allFields.add(slice);
    allFields.add(statusId);
    allFields.add(nativeName);
    allFields.add(logicalName);
    allFields.add(version);
    allFields.add(instanceCreatedUnixtime);
    allFields.add(schema);
    allFields.add(viewExpandedText);
    allFields.add(abstractedDatasetUrn);
    return allFields;
  }


}
