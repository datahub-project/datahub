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
package metadata.etl.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wherehows.common.schemas.Record;


/**
 * Abstract class for Kafka consumer message processor.
 *
 */
public abstract class KafkaConsumerProcessor {

  protected static final Logger logger = LoggerFactory.getLogger(KafkaConsumerProcessor.class);

  // for 'ltx1-holdemrm01.grid.linkedin.com:8032', extract 'ltx1', 'holdem'
  private final String ClusterIdentifierRegex = "^(\\w{4})-(\\w+)\\w{2}\\d{2}\\.grid.*$";
  private final Pattern ClusterIdentifierPattern = Pattern.compile(ClusterIdentifierRegex);


  /**
   * Abstract method 'process' to be implemented by specific processor
   * input Kafka record, process information and write to DB.
   * @param record
   * @param topic
   * @throws Exception
   */
  public abstract Record process(GenericData.Record record, String topic) throws Exception;


  /**
   * Parse Integer value from a String, if null or exception, return 0
   * @param text String
   * @return int
   */
  protected long parseLong(String text) {
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  /**
   * Parse cluster identifier to get datacenter and cluster
   * if length > 10 and in the form of 'ltx1-holdemrm01.grid.linkedin.com:8032', extract 'ltx1', 'holdem'
   * otherwise, put the original String as cluster
   * @param text String
   * @return Map<String, String>
   */
  protected Map<String, String> parseClusterIdentifier(String text) {
    final Map<String, String> map = new HashMap<>(4);
    if (text.length() > 10) {
      final Matcher m = ClusterIdentifierPattern.matcher(text);
      if (m.find()) {
        map.put("datacenter", m.group(1));
        map.put("cluster", m.group(2));
      }
    } else {
      map.put("cluster", text);
    }
    return map;
  }
}
