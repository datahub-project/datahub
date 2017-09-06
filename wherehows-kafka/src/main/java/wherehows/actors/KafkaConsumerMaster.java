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
package wherehows.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.extern.slf4j.Slf4j;
import wherehows.common.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.utils.JobsUtil;
import wherehows.msgs.KafkaCommMsg;
import wherehows.processors.KafkaConfig;
import wherehows.processors.KafkaConfig.Topic;

import static wherehows.main.ApplicationStart.*;


/**
 * Akka actor responsible for managing Kafka workers
 */
@Slf4j
public class KafkaConsumerMaster extends UntypedActor {

  public static final Config config = ConfigFactory.load();

  public static final String KAFKA_CONSUMER_JOB = config.getString("kafka.consumer.dir");

  private static final String KAFKA_JOB_TYPE = "kafka";

  // Map of kafka job name to properties
  private static Map<String, Properties> _kafkaJobList;
  // Map of kafka job name to configs
  private static Map<String, KafkaConfig> _kafkaConfigs = new HashMap<>();

  @Override
  public void preStart() throws Exception {

    _kafkaJobList = JobsUtil.getEnabledJobsByType(KAFKA_CONSUMER_JOB, KAFKA_JOB_TYPE);
    log.info("Kafka jobs: {}", _kafkaJobList.keySet());

    if (_kafkaJobList.size() == 0) {
      context().stop(getSelf());
      return;
    }
    log.info("ApplicationStart KafkaConsumerMaster...");

    for (Map.Entry<String, Properties> entry : _kafkaJobList.entrySet()) {

      final String kafkaJobName = entry.getKey();
      try {
        // handle 1 kafka connection
        final KafkaConfig kafkaConfig = new KafkaConfig();
        kafkaConfig.updateKafkaProperties(kafkaJobName, entry.getValue());
        final Properties kafkaProps = kafkaConfig.getProperties();
        final Map<String, Topic> kafkaTopics = kafkaConfig.getTopics();

        kafkaConfig.updateTopicProcessor(DAO_FACTORY);

        // create Kafka consumer connector
        log.info("Create Kafka Consumer with config: " + kafkaProps.toString());
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
        _kafkaConfigs.put(kafkaJobName, kafkaConfig);

        // create workers to handle each message stream
        for (String topic : kafkaTopics.keySet()) {
          final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

          int threadNumber = 0;
          for (final KafkaStream<byte[], byte[]> stream : streams) {
            ActorRef childActor = getContext().actorOf(
                Props.create(KafkaConsumerWorker.class, topic, threadNumber, stream, schemaRegistryClient,
                    kafkaConfig.getProcessorClass(topic), kafkaConfig.getProcessorMethod(topic),
                    kafkaConfig.getDbWriter(topic)));

            childActor.tell("ApplicationStart", getSelf());
            threadNumber++;
          }
        }
        log.info("Initiate Kafka consumer job " + kafkaJobName + " with topics " + kafkaTopics.keySet());
      } catch (Exception e) {
        log.error("Initiating Kafka properties on startup fail, job name: " + kafkaJobName, e);
      }
    }

  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message instanceof KafkaCommMsg) {
      final KafkaCommMsg kafkaCommMsg = (KafkaCommMsg) message;
      log.debug(kafkaCommMsg.toString());
    } else {
      unhandled(message);
    }
  }

  @Override
  public void postStop() {
    log.info("Terminating KafkaConsumerMaster...");
    for (KafkaConfig config : _kafkaConfigs.values()) {
      config.close();
    }
  }
}
