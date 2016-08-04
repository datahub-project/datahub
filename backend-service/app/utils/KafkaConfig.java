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
package utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import metadata.etl.models.EtlJobName;
import models.daos.EtlJobPropertyDao;
import play.Logger;


/**
 * Utilities for Kafka configurations and topics
 */
public class KafkaConfig {

  private static Properties _props = new Properties();
  // Map of <topic_name, topic_content>
  private static Map<String, Topic> _topics = new HashMap<>();

  public static final EtlJobName KAFKA_JOBNAME = EtlJobName.KAFKA_CONSUMER_ETL;

  /**
   * Class for storing Kafka Topic info
   */
  public static class Topic {
    public String topic;
    public int numOfWorkers; // number of kafka consumer workers
    public String processor; // processor class to invoke
    public String dbTable; // Database table to write to

    Topic(String topic, int numOfWorkers, String processor, String dbTable) {
      this.topic = topic;
      this.numOfWorkers = numOfWorkers;
      this.processor = processor;
      this.dbTable = dbTable;
    }
  }

  /**
   * Update Kafka properties and topics from etl_job_properies table
   * @throws Exception
   */
  public static void updateKafkaProperties(int kafkaJobRefId) throws Exception {
    Properties props = EtlJobPropertyDao.getJobProperties(KAFKA_JOBNAME, kafkaJobRefId);
    if (props == null || props.size() < 5) {
      Logger.error("Fail to update Kafka job properties for " + KAFKA_JOBNAME.name()
          + ", job ref id: " + kafkaJobRefId);
      return;
    } else {
      Logger.info("Get Kafka job properties for " + KAFKA_JOBNAME.name() + ", job ref id: " + kafkaJobRefId);
    }

    String[] topics = ((String) props.remove("kafka.topics")).split("\\s*,\\s*");
    String[] processors = ((String) props.remove("kafka.processors")).split("\\s*,\\s*");
    String[] dbTables = ((String) props.remove("kafka.db.tables")).split("\\s*,\\s*");

    _props.clear();
    _props.putAll(props);

    _topics.clear();
    for (int i = 0; i < topics.length; i++) {
      // use 1 Kafka worker to handle each topic
      _topics.put(topics[i], new Topic(topics[i], 1, processors[i], dbTables[i]));
    }
  }

  /**
   * get Kafka configuration
   * @return
   */
  public static Properties getProperties() {
    return _props;
  }

  /**
   * get Kafka topics
   * @return
   */
  public static Map<String, Topic> getTopics() {
    return _topics;
  }

}
