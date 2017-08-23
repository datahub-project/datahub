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
  public void process(GenericData.Record record, String topic)
      throws Exception {
  }
}
