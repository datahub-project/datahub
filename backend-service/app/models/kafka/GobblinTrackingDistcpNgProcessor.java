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
import wherehows.common.schemas.GobblinTrackingDistcpNgRecord;
import wherehows.common.schemas.Record;
import wherehows.common.utils.ClusterUtil;
import wherehows.common.utils.StringUtil;


/**
 * Kafka message processor for Gobblin tracking event distcp_ng topic
 */
public class GobblinTrackingDistcpNgProcessor extends KafkaConsumerProcessor {

  // for data path hdfs://ltx1-holdemnn01.grid.linkedin.com:9000/data/tracking/abc/hourly/2016/07/10/00/part-2560590.avro
  // cut into 4 pieces: cluster + dataset + partitionType + partition
  private final String PathRegex = "^\\w+:\\/\\/(\\w+-\\w+\\.grid.*:\\d+)\\/(.*)\\/([\\w-]+)\\/(\\d+.*\\/\\d+)\\/\\w.*$";
  private final Pattern PathPattern = Pattern.compile(PathRegex);

  /**
   * Process a Gobblin tracking event distcp_ng event record
   * @param record GenericData.Record
   * @param topic
   * @return Record
   * @throws Exception
   */
  @Override
  public Record process(GenericData.Record record, String topic)
      throws Exception {
    GobblinTrackingDistcpNgRecord eventRecord = null;

    // handle namespace "gobblin.copy.CopyDataPublisher"
    if (record != null && record.get("namespace") != null && record.get("name") != null
        && "gobblin.copy.CopyDataPublisher".equals(record.get("namespace").toString())) {
      final String name = record.get("name").toString();

      if (name.equals("DatasetPublished")) { // || name.equals("FilePublished")) {
        // logger.info("Processing Gobblin tracking event record: " + name);
        final long timestamp = (long) record.get("timestamp");
        final Map<String, String> metadata = StringUtil.convertObjectMapToStringMap(record.get("metadata"));

        final String jobContext = "DistcpNG:" + name;
        final String cluster = ClusterUtil.matchClusterCode(metadata.get("clusterIdentifier"));
        final String projectName = metadata.get("azkabanProjectName");
        final String flowId = metadata.get("azkabanFlowId");
        final String jobId = metadata.get("azkabanJobId");
        final int execId = StringUtil.parseInteger(metadata.get("azkabanExecId"));
        // final String metricContextId = metadata.get("metricContextID");
        // final String metricContextName = metadata.get("metricContextName");

        final long upstreamTimestamp = StringUtil.parseLong(metadata.get("upstreamTimestamp"));
        final long originTimestamp = StringUtil.parseLong(metadata.get("originTimestamp"));
        final String sourcePath = metadata.get("SourcePath");
        final String targetPath = metadata.get("TargetPath");

        final String dataset = metadata.get("datasetUrn");
        String partitionType = null;
        String partitionName = null;

        if (name.equals("DatasetPublished")) {
          partitionName = metadata.get("partition");
        }
        // name "FilePublished"
        else {
          final Matcher m = PathPattern.matcher(targetPath);
          if (m.find()) {
            partitionType = m.group(3);
            partitionName = m.group(4);
          }
        }

        eventRecord =
            new GobblinTrackingDistcpNgRecord(timestamp, jobContext, cluster, projectName, flowId, jobId, execId);
        eventRecord.setDatasetUrn(dataset, partitionType, partitionName);
        eventRecord.setEventInfo(upstreamTimestamp, originTimestamp, sourcePath, targetPath);
      }
    }
    return eventRecord;
  }
}
