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
package actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import metadata.etl.models.EtlJobName;
import models.daos.ClusterDao;
import models.daos.EtlJobDao;
import org.apache.avro.generic.GenericData;

import msgs.KafkaResponseMsg;
import play.Logger;
import play.Play;
import utils.JdbcUtil;
import utils.KafkaConfig;
import utils.KafkaConfig.Topic;
import wherehows.common.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.schemas.AbstractRecord;
import wherehows.common.utils.ClusterUtil;
import wherehows.common.writers.DatabaseWriter;


/**
 * Akka actor responsible to manage Kafka workers and use kafka topic processor
 * to deal with response messages
 *
 */
public class KafkaConsumerMaster extends UntypedActor {

  private static List<Integer> _kafkaJobList;
  private static ConsumerConnector _consumer;
  private static Properties _kafkaConfig;
  private static Map<String, Topic> _kafkaTopics;

  private final Map<String, Object> _topicProcessorClass;
  private final Map<String, Method> _topicProcessorMethod;
  private final Map<String, DatabaseWriter> _topicDbWriter;

  public KafkaConsumerMaster() {
    this._topicProcessorClass = new HashMap<>();
    this._topicProcessorMethod = new HashMap<>();
    this._topicDbWriter = new HashMap<>();
  }

  @Override
  public void preStart() throws Exception {
    _kafkaJobList = Play.application().configuration().getIntList("kafka.consumer.etl.jobid", null);;
    if (_kafkaJobList == null || _kafkaJobList.size() == 0) {
      Logger.error("Kafka job id error, kafkaJobList: " + _kafkaJobList);
      getContext().stop(getSelf());
    }
    Logger.info("Start the KafkaConsumerMaster... Kafka etl job id list: " + _kafkaJobList);

    // handle 1 kafka connection
    Map<String, Object> kafkaEtlJob = EtlJobDao.getEtlJobById(_kafkaJobList.get(0));
    final int kafkaJobRefId = Integer.parseInt(kafkaEtlJob.get("ref_id").toString());
    final String kafkaJobName = kafkaEtlJob.get("wh_etl_job_name").toString();

    if (!kafkaJobName.equals(EtlJobName.KAFKA_CONSUMER_ETL.name())) {
      Logger.error("Kafka job info error: job name '" + kafkaJobName + "' not equal "
          + EtlJobName.KAFKA_CONSUMER_ETL.name());
      getContext().stop(getSelf());
    }

    // get Kafka configurations from database
    KafkaConfig.updateKafkaProperties(kafkaJobRefId);
    _kafkaConfig = KafkaConfig.getProperties();
    _kafkaTopics = KafkaConfig.getTopics();

    // get list of cluster information from database and update ClusterUtil
    ClusterUtil.updateClusterInfo(ClusterDao.getClusterInfo());

    for (String topic : _kafkaTopics.keySet()) {
      // get the processor class and method
      final Class processorClass = Class.forName(_kafkaTopics.get(topic).processor);
      _topicProcessorClass.put(topic, processorClass.newInstance());

      final Method method = processorClass.getDeclaredMethod("process", GenericData.Record.class, String.class);
      _topicProcessorMethod.put(topic, method);

      // get the database writer
      final DatabaseWriter dw = new DatabaseWriter(JdbcUtil.wherehowsJdbcTemplate, _kafkaTopics.get(topic).dbTable);
      _topicDbWriter.put(topic, dw);
    }

    // create Kafka consumer connector
    final SchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient((String) _kafkaConfig.get("schemaRegistryUrl"));
    Logger.info("Create Kafka Consumer Config: " + _kafkaConfig.toString());
    final ConsumerConfig cfg = new ConsumerConfig(_kafkaConfig);
    _consumer = Consumer.createJavaConsumerConnector(cfg);

    // create Kafka message streams
    final Map<String, Integer> topicCountMap = new HashMap<>();
    for (Topic topic : _kafkaTopics.values()) {
      topicCountMap.put(topic.topic, topic.numOfWorkers);
    }

    final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
        _consumer.createMessageStreams(topicCountMap);

    for (String topic : _kafkaTopics.keySet()) {
      final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

      int threadNumber = 0;
      for (final KafkaStream<byte[], byte[]> stream : streams) {
        ActorRef childActor =
            getContext().actorOf(
                Props.create(KafkaConsumerWorker.class,
                    topic, threadNumber, stream, schemaRegistryClient,
                    _topicProcessorClass.get(topic), _topicProcessorMethod.get(topic)));

        childActor.tell("Start", getSelf());
        threadNumber++;
      }
    }
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof KafkaResponseMsg) {
      final KafkaResponseMsg kafkaMsg = (KafkaResponseMsg) message;
      final String topic = kafkaMsg.getTopic();
      final AbstractRecord record = kafkaMsg.getRecord();

      if (record != null && _kafkaTopics.containsKey(topic)) {
        Logger.debug("Writing to DB kafka event record: " + topic);
        final DatabaseWriter dbWriter = _topicDbWriter.get(topic);

        try {
          dbWriter.append(record);
          // dbWriter.close();
          dbWriter.insert();
        } catch (SQLException | IOException e) {
          Logger.error("Error while inserting event record: " + record, e);
        }
      } else {
        Logger.error("Unknown kafka response " + kafkaMsg);
      }
    } else {
      unhandled(message);
    }
  }

  @Override
  public void postStop() {
    Logger.info("Terminating KafkaConsumerMaster...");
    if (_consumer != null) {
      _consumer.shutdown();
      _kafkaConfig.clear();
      _kafkaTopics.clear();
    }
  }
}
