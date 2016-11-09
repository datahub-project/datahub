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

import msgs.KafkaCommMsg;
import play.Logger;
import play.Play;
import models.kafka.KafkaConfig;
import models.kafka.KafkaConfig.Topic;
import play.db.DB;
import wherehows.common.PathAnalyzer;
import wherehows.common.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.utils.ClusterUtil;


/**
 * Akka actor responsible for managing Kafka workers
 */
public class KafkaConsumerMaster extends UntypedActor {

  // List of kafka job IDs
  private static List<Integer> _kafkaJobList;
  // map of kafka job id to configs
  private static Map<Integer, KafkaConfig> _kafkaConfigs = new HashMap<>();

  @Override
  public void preStart()
      throws Exception {
    _kafkaJobList = Play.application().configuration().getIntList("kafka.consumer.etl.jobid", null);
    if (_kafkaJobList == null || _kafkaJobList.size() == 0) {
      context().stop(getSelf());
      Logger.error("Kafka job id error, kafkaJobList: " + _kafkaJobList);
      return;
    }
    Logger.info("Start KafkaConsumerMaster... Kafka job id list: " + _kafkaJobList);

    for (final int kafkaJobId : _kafkaJobList) {
      try {
        // handle 1 kafka connection
        Map<String, Object> kafkaEtlJob = EtlJobDao.getEtlJobById(kafkaJobId);
        final int kafkaJobRefId = Integer.parseInt(kafkaEtlJob.get("ref_id").toString());
        final String kafkaJobName = kafkaEtlJob.get("wh_etl_job_name").toString();

        // get Kafka configurations from database
        final KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.updateKafkaProperties(EtlJobName.valueOf(kafkaJobName), kafkaJobRefId);
        final Properties kafkaProps = kafkaConfig.getProperties();
        final Map<String, Topic> kafkaTopics = kafkaConfig.getTopics();

        kafkaConfig.updateTopicProcessor();

        // create Kafka consumer connector
        Logger.info("Create Kafka Consumer with config: " + kafkaProps.toString());
        final SchemaRegistryClient schemaRegistryClient =
            new CachedSchemaRegistryClient((String) kafkaProps.get("schemaRegistryUrl"));
        final ConsumerConfig cfg = new ConsumerConfig(kafkaProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(cfg);

        // create Kafka message streams
        final Map<String, Integer> topicCountMap = new HashMap<>();
        for (Topic topic : kafkaTopics.values()) {
          topicCountMap.put(topic.topic, topic.numOfWorkers);
        }

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
            consumerConnector.createMessageStreams(topicCountMap);

        // add config to kafka config map
        kafkaConfig.setSchemaRegistryClient(schemaRegistryClient);
        kafkaConfig.setConsumer(consumerConnector);
        _kafkaConfigs.put(kafkaJobId, kafkaConfig);

        // create workers to handle each message stream
        for (String topic : kafkaTopics.keySet()) {
          final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

          int threadNumber = 0;
          for (final KafkaStream<byte[], byte[]> stream : streams) {
            ActorRef childActor = getContext().actorOf(
                Props.create(KafkaConsumerWorker.class, topic, threadNumber, stream, schemaRegistryClient,
                    kafkaConfig.getProcessorClass(topic), kafkaConfig.getProcessorMethod(topic),
                    kafkaConfig.getDbWriter(topic)));

            childActor.tell("Start", getSelf());
            threadNumber++;
          }
        }
        Logger.info("Initiate Kafka consumer job " + kafkaJobId + " with topics " + kafkaTopics.keySet());
      } catch (Exception e) {
        Logger.error("Initiating Kafka properties on startup fail, job id: " + kafkaJobId, e);
      }
    }

    try {
      // get list of cluster information from database and update ClusterUtil
      ClusterUtil.updateClusterInfo(ClusterDao.getClusterInfo());
    } catch (Exception e) {
      Logger.error("Fail to fetch cluster info from DB ", e);
    }

    try {
      // initialize PathAnalyzer
      PathAnalyzer.initialize(DB.getConnection("wherehows"));
    } catch (Exception e) {
      Logger.error("Fail to initialize PathAnalyzer from DB wherehows.", e);
    }
  }

  @Override
  public void onReceive(Object message)
      throws Exception {
    if (message instanceof KafkaCommMsg) {
      final KafkaCommMsg kafkaCommMsg = (KafkaCommMsg) message;
      Logger.debug(kafkaCommMsg.toString());
    } else {
      unhandled(message);
    }
  }

  @Override
  public void postStop() {
    Logger.info("Terminating KafkaConsumerMaster...");
    for (KafkaConfig config : _kafkaConfigs.values()) {
      config.close();
    }
  }
}
