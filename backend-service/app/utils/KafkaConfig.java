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
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Utilities for Kafka configurations and topics
 */
public class KafkaConfig {

  private static Properties _props = new Properties();
  // Map of <topic_name, topic_content>
  private static Map<String, Topic> _topics = new HashMap<>();

  public static final String GET_KAFKA_PROPERTIES = "SELECT * FROM wh_kafka_property ORDER BY `wh_kafka_property_id`";
  public static final String GET_KAFKA_TOPICS = "SELECT * FROM wh_kafka_topic";

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
   * get Kafka properties from database, also update _props
   * @return Properties
   * @throws Exception
   */
  public static Properties getKafkaProperties() throws Exception {
    final List<Map<String, Object>> rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_KAFKA_PROPERTIES);

    _props.clear();
    for (Map<String, Object> row : rows) {
      _props.put("schemaRegistryUrl", row.get("schema_registry_url"));
      _props.put("zookeeper.connect", row.get("zookeeper_connect"));
      _props.put("group.id", row.get("group_id"));
      // Integer object needs toString() to convert
      _props.put("zookeeper.session.timeout.ms", row.get("zookeeper_session_timeout_ms").toString());
      _props.put("auto.commit.interval.ms", row.get("auto_commit_interval_ms").toString());
      _props.put("zookeeper.sync.time.ms", row.get("zookeeper_sync_time_ms").toString());
    }
    return _props;
  }

  /**
   * default values of kafka properties, for testing purpose
   * @return Properties
   */
  public static Properties getKafkaPropertiesDefault() {
    _props.clear();
    _props.put("schemaRegistryUrl", "http://lva1-schema-registry-vip-2.corp.linkedin.com:12250/schemaRegistry"); // + "/schemas"
    _props.put("zookeeper.connect", "zk-ltx1-kafka.corp.linkedin.com:12913/kafka-aggregate-metrics");
    _props.put("group.id", "wherehows_dev");
    _props.put("zookeeper.session.timeout.ms", "8000");
    _props.put("auto.commit.interval.ms", "1000");
    _props.put("zookeeper.sync.time.ms", "200");
    return _props;
  }

  /**
   * get Kafka topics from database, also update _topics list
   * @return Map<String topic, Topic>
   */
  public static Map<String, Topic> getKafkaTopics() throws Exception {
    final List<Map<String, Object>> rows = JdbcUtil.wherehowsJdbcTemplate.queryForList(GET_KAFKA_TOPICS);

    _topics.clear();
    for (Map<String, Object> row : rows) {
      final String topic = (String) row.get("topic_name");
      //final int kafkaPropertyId = (Integer) row.get("wh_kafka_property_id");
      final int workers = (Integer) row.get("num_of_workers");
      final String processor = (String) row.get("processor");
      final String table = (String) row.get("db_table");
      _topics.put(topic, new Topic(topic, workers, processor, table));
    }
    return _topics;
  }

  /**
   * default values of kafka topics, for testing purpose
   * @return Map<String topic, Topic>
   */
  public static Map<String, Topic> getKafkaTopicsDefault() {
    _topics.clear();

    String topic = "GobblinTrackingEvent";
    _topics.put(topic, new Topic(topic, 1, "metadata.etl.kafka.GobblinTrackingCompactionProcessor",
        "stg_gobblin_tracking_compaction"));

    topic = "GobblinTrackingEvent_lumos";
    _topics.put(topic, new Topic(topic, 1, "metadata.etl.kafka.GobblinTrackingLumosProcessor",
        "stg_gobblin_tracking_lumos"));

    topic = "GobblinTrackingEvent_distcp_ng";
    _topics.put(topic, new Topic(topic, 1, "metadata.etl.kafka.GobblinTrackingDistcpNgProcessor",
        "stg_gobblin_tracking_distcp_ng"));

    topic = "MetastoreTableAuditEvent";
    _topics.put(topic, new Topic(topic, 1, "metadata.etl.kafka.MetastoreAuditProcessor",
        "stg_metastore_audit"));

    topic = "MetastorePartitionAuditEvent";
    _topics.put(topic, new Topic(topic, 1, "metadata.etl.kafka.MetastoreAuditProcessor",
        "stg_metastore_audit"));

    return _topics;
  }

}
