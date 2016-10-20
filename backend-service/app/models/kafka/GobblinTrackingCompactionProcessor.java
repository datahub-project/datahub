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
import wherehows.common.schemas.GobblinTrackingCompactionRecord;
import wherehows.common.schemas.Record;
import wherehows.common.utils.ClusterUtil;
import wherehows.common.utils.StringUtil;


/**
 * Kafka message processor for Gobblin tracking event compaction topic
 */
public class GobblinTrackingCompactionProcessor extends KafkaConsumerProcessor {

  // for data tracking urn, such as '/data/tracking/ab-c/daily-dedup/2016/...'
  // cut into 3 pieces: dataset + partitionType + partition
  private final String UrnRegex = "^(\\/\\w+\\/\\w+\\/[\\w-]+)\\/([\\w-]+)\\/(\\d+.*)$";
  private final Pattern UrnPattern = Pattern.compile(UrnRegex);

  /**
   * Process a Gobblin tracking event compaction record
   * @param record
   * @param topic
   * @return Record
   * @throws Exception
   */
  @Override
  public Record process(GenericData.Record record, String topic)
      throws Exception {
    GobblinTrackingCompactionRecord eventRecord = null;

    // only handle namespace "compaction.tracking.events"
    if (record != null && record.get("namespace") != null && record.get("name") != null
        && "compaction.tracking.events".equals(record.get("namespace").toString())) {
      final String name = record.get("name").toString();

      // for event name "CompactionCompleted" or "CompactionRecordCounts"
      if (name.equals("CompactionCompleted") || name.equals("CompactionRecordCounts")) {
        // logger.info("Processing Gobblin tracking event record: " + name);
        final long timestamp = (long) record.get("timestamp");
        final Map<String, String> metadata = StringUtil.convertObjectMapToStringMap(record.get("metadata"));

        final String jobContext = "Gobblin:" + name;
        final String cluster = ClusterUtil.matchClusterCode(metadata.get("clusterIdentifier"));
        // final String cluster = parseClusterIdentifier(metadata.get("clusterIdentifier")).get("cluster");
        final String projectName = metadata.get("azkabanProjectName");
        final String flowId = metadata.get("azkabanFlowId");
        final String jobId = metadata.get("azkabanJobId");
        final int execId = Integer.parseInt(metadata.get("azkabanExecId"));

        // final String metricContextId = metadata.get("metricContextID");
        // final String metricContextName = metadata.get("metricContextName");
        final String dedupeStatus = metadata.get("dedupeStatus");

        String dataset = null;
        String partitionType = null;
        String partitionName = null;
        long recordCount = 0;
        long lateRecordCount = 0;

        if (name.equals("CompactionCompleted")) {
          dataset = metadata.get("datasetUrn");
          partitionName = metadata.get("partition");
          recordCount = StringUtil.parseLong(metadata.get("recordCount"));
        }
        // name = "CompactionRecordCounts"
        else {
          final Matcher m = UrnPattern.matcher(metadata.get("DatasetOutputPath"));
          if (m.find()) {
            dataset = m.group(1);
            partitionType = m.group(2);
            partitionName = m.group(3);
          }

          recordCount = StringUtil.parseLong(metadata.get("RegularRecordCount"));
          lateRecordCount = StringUtil.parseLong(metadata.get("LateRecordCount"));
        }

        eventRecord =
            new GobblinTrackingCompactionRecord(timestamp, jobContext, cluster, projectName, flowId, jobId, execId);
        eventRecord.setDatasetUrn(dataset, partitionType, partitionName);
        eventRecord.setRecordCount(recordCount);
        eventRecord.setLateRecordCount(lateRecordCount);
        eventRecord.setDedupeStatus(dedupeStatus);
      }
    }
    return eventRecord;
  }
}
