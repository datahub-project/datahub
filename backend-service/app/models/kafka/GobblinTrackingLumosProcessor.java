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
package models.kafka;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericData;
import wherehows.common.schemas.GobblinTrackingLumosRecord;
import wherehows.common.schemas.Record;
import wherehows.common.utils.ClusterUtil;
import wherehows.common.utils.StringUtil;


public class GobblinTrackingLumosProcessor extends KafkaConsumerProcessor {

  // dissect datasourceColo 'prod-lva1' into two parts: developmentEnv and datacenter
  private final String DatasourceColoRegex = "(\\w+)-(\\w+)";
  private final Pattern DatasourceColoPattern = Pattern.compile(DatasourceColoRegex);

  // get partition from directory
  private final String DirectoryPartitionRegex = "^.*\\/(\\d+-\\w+-\\d+)\\/.*$";
  private final Pattern DirectoryPartitionPattern = Pattern.compile(DirectoryPartitionRegex);

  // regular partition pattern, 146xxxx-ww-dddd
  final String RegularPartitionRegex = "146\\d{7,10}-\\w+-\\d+";
  final Pattern RegularPartitionPattern = Pattern.compile(RegularPartitionRegex);

  // get Epoch time from Partition, 146xxxxxxxxxx-ww-dddd
  private final String PartitionEpochRegex = "(\\d+)-\\w+-\\d+";
  private final Pattern PartitionEpochPattern = Pattern.compile(PartitionEpochRegex);

  /**
   * Process a Gobblin tracking event lumos record
   * @param record
   * @param topic
   * @return Record
   * @throws Exception
   */
  @Override
  public Record process(GenericData.Record record, String topic)
      throws Exception {
    GobblinTrackingLumosRecord eventRecord = null;

    if (record != null && record.get("namespace") != null && record.get("name") != null) {
      final String name = record.get("name").toString();

      // only handle "DeltaPublished" and "SnapshotPublished"
      if (name.equals("DeltaPublished") || name.equals("SnapshotPublished")) {
        final long timestamp = (long) record.get("timestamp");
        final Map<String, String> metadata = StringUtil.convertObjectMapToStringMap(record.get("metadata"));
        // logger.info("Processing Gobblin tracking event record: " + name + ", timestamp: " + timestamp);

        final String jobContext = "Lumos:" + name;
        final String cluster = ClusterUtil.matchClusterCode(metadata.get("clusterIdentifier"));
        final String projectName = metadata.get("azkabanProjectName");
        final String flowId = metadata.get("azkabanFlowId");
        final String jobId = metadata.get("azkabanJobId");
        final int execId = Integer.parseInt(metadata.get("azkabanExecId"));
        // final String metricContextId = metadata.get("metricContextID");
        // final String metricContextName = metadata.get("metricContextName");

        final String dataset = metadata.get("datasetUrn");
        final String targetDirectory = metadata.get("TargetDirectory");

        final String datasourceColo = metadata.get("DatasourceColo");
        final String sourceDatabase = metadata.get("Database");
        final String sourceTable = metadata.get("Table");
        String datacenter = null;
        String devEnv = null;
        final Matcher sourceColoMatcher = DatasourceColoPattern.matcher(datasourceColo);
        if (sourceColoMatcher.find()) {
          datacenter = sourceColoMatcher.group(2);
          devEnv = sourceColoMatcher.group(1);
        } else {
          datacenter = datasourceColo;
        }

        final long recordCount = StringUtil.parseLong(metadata.get("recordCount"));

        final String partitionType = "snapshot";
        final String partition = metadata.get("partition");
        String partitionName = null;
        String subpartitionType = null;
        String subpartitionName = null;

        final long dropdate = StringUtil.parseLong(metadata.get("Dropdate"));
        long maxDataDateEpoch3 = dropdate;
        long maxDataKey = 0; // if field is null, default value 0
        if (!isPartitionRegular(partition)) {
          maxDataKey = StringUtil.parseLong(getPartitionEpoch(partition));
        }

        // handle name 'SnapshotPublished'
        if (name.equals("SnapshotPublished")) {
          partitionName = partition;
          if (dropdate < 1460000000000L) {
            maxDataDateEpoch3 = StringUtil.parseLong(getPartitionEpoch(targetDirectory));
          }
        }
        // handle name 'DeltaPublished'
        else {
          partitionName = partitionFromTargetDirectory(targetDirectory);
          subpartitionType = "_delta";
          subpartitionName = partition;
          if (dropdate < 1460000000000L) {
            maxDataDateEpoch3 = StringUtil.parseLong(getPartitionEpoch(subpartitionName));
          }
        }

        eventRecord =
            new GobblinTrackingLumosRecord(timestamp, cluster, jobContext, projectName, flowId, jobId, execId);
        eventRecord.setDatasetUrn(dataset, targetDirectory, partitionType, partitionName, subpartitionType,
            subpartitionName);
        eventRecord.setMaxDataDate(maxDataDateEpoch3, maxDataKey);
        eventRecord.setSource(datacenter, devEnv, sourceDatabase, sourceTable);
        eventRecord.setRecordCount(recordCount);
      }
    }
    return eventRecord;
  }

  /**
   * get partition name from targetDirectory for delta published
   * @param targetDirectory String
   * @return String partitionName or null
   */
  private String partitionFromTargetDirectory(String targetDirectory) {
    final Matcher m = DirectoryPartitionPattern.matcher(targetDirectory);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  /**
   * get epoch time from partition first part
   * @param partition String
   * @return String
   */
  private String getPartitionEpoch(String partition) {
    final Matcher m = PartitionEpochPattern.matcher(partition);
    if (m.find()) {
      return m.group(1);
    }
    return null;
  }

  /**
   * check if partition is in the form of 146xxxxxxxxxx-ww-dddd
   * @param partition
   * @return boolean
   */
  private boolean isPartitionRegular(String partition) {
    return RegularPartitionPattern.matcher(partition).find();
  }
}
