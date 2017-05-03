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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.javaapi.consumer.ConsumerConnector;
import metadata.etl.models.EtlJobName;
import models.daos.EtlJobPropertyDao;
import org.apache.avro.generic.GenericData;
import play.Logger;
import utils.JdbcUtil;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.writers.DatabaseWriter;


/**
 * Utilities for Kafka configurations and topics
 */
public class KafkaConfig {

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

  private final Properties _props = new Properties();
  // Map of <topic_name, topic_content>
  private final Map<String, Topic> _topics = new HashMap<>();
  private final Map<String, Object> _topicProcessorClass = new HashMap<>();
  private final Map<String, Method> _topicProcessorMethod = new HashMap<>();
  private final Map<String, DatabaseWriter> _topicDbWriter = new HashMap<>();

  private ConsumerConnector _consumer;
  private SchemaRegistryClient _schemaRegistryClient;

  /**
   * Update Kafka properties and topics from wh_etl_job_properties table
   * @throws Exception
   */
  public void updateKafkaProperties(EtlJobName jobName, int jobRefId)
      throws Exception {
    Properties props;
    try {
      props = EtlJobPropertyDao.getJobProperties(jobName, jobRefId);
    } catch (Exception e) {
      Logger.error("Fail to update Kafka job properties for " + jobName.name() + ", ref id: " + jobRefId);
      return;
    }
    Logger.info("Get Kafka job properties for " + jobName.name() + ", job ref id: " + jobRefId);

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
   * update processor class, method and db writer for each topic
   */
  public void updateTopicProcessor() {
    for (String topic : _topics.keySet()) {
      try {
        // get the processor class and method
        final Class processorClass = Class.forName(_topics.get(topic).processor);
        _topicProcessorClass.put(topic, processorClass.newInstance());

        final Method method = processorClass.getDeclaredMethod("process", GenericData.Record.class, String.class);
        _topicProcessorMethod.put(topic, method);

        // get the database writer
        final DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, _topics.get(topic).dbTable);
        _topicDbWriter.put(topic, dw);
      } catch (Exception e) {
        Logger.error("Fail to create Processor for topic: " + topic, e);
        _topicProcessorClass.remove(topic);
        _topicProcessorMethod.remove(topic);
        _topicDbWriter.remove(topic);
      }
    }
  }

  public Object getProcessorClass(String topic) {
    return _topicProcessorClass.get(topic);
  }

  public Method getProcessorMethod(String topic) {
    return _topicProcessorMethod.get(topic);
  }

  public DatabaseWriter getDbWriter(String topic) {
    return _topicDbWriter.get(topic);
  }

  public Map<String, DatabaseWriter> getTopicDbWriters() {
    return _topicDbWriter;
  }

  /**
   * close the Config
   */
  public void close() {
    if (_consumer != null) {
      _consumer.shutdown();
    }
  }

  /**
   * get Kafka configuration
   * @return
   */
  public Properties getProperties() {
    return _props;
  }

  /**
   * get Kafka topics
   * @return
   */
  public Map<String, Topic> getTopics() {
    return _topics;
  }

  public ConsumerConnector getConsumer() {
    return _consumer;
  }

  public void setConsumer(ConsumerConnector consumer) {
    _consumer = consumer;
  }

  public SchemaRegistryClient getSchemaRegistryClient() {
    return _schemaRegistryClient;
  }

  public void setSchemaRegistryClient(SchemaRegistryClient schemaRegistryClient) {
    _schemaRegistryClient = schemaRegistryClient;
  }
}
