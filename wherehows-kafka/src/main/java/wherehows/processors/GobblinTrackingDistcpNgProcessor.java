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
package wherehows.processors;

import java.util.regex.Pattern;
import org.apache.avro.generic.GenericData;


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
  public void process(GenericData.Record record, String topic)
      throws Exception {
  }
}
