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
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.common.Constant;
import wherehows.common.utils.JobsUtil;
import wherehows.dao.DaoFactory;
import wherehows.msgs.KafkaCommMsg;
import wherehows.processors.KafkaMessageProcessor;

import static wherehows.main.ApplicationStart.*;
import static wherehows.utils.KafkaClientUtil.*;


@Slf4j
public class KafkaClientMaster extends UntypedActor {

  private final String KAFKA_JOB_DIR;

  // Map of kafka job name to properties
  private static Map<String, Properties> _kafkaJobList;

  // List of kafka workers
  private static List<ActorRef> _kafkaWorkers = new ArrayList<>();

  private static final Config CONFIG = ConfigFactory.load();

  public KafkaClientMaster(String kafkaJobDir) {
    this.KAFKA_JOB_DIR = kafkaJobDir;
  }

  @Override
  public void preStart() throws Exception {

    _kafkaJobList = JobsUtil.getEnabledJobs(KAFKA_JOB_DIR);
    log.info("Kafka jobs: {}", _kafkaJobList.keySet());

    if (_kafkaJobList.size() == 0) {
      context().stop(getSelf());
      return;
    }
    log.info("Starting KafkaClientMaster...");

    for (Map.Entry<String, Properties> entry : _kafkaJobList.entrySet()) {
      // handle one kafka topic
      final String kafkaJobName = entry.getKey();
      final Properties props = entry.getValue();

      final int numberOfWorkers = Integer.parseInt(props.getProperty(Constant.KAFKA_WORKER_COUNT, "1"));

      log.info("Create Kafka client with config: " + props);
      try {
        // create worker
        for (int i = 0; i < numberOfWorkers; i++) {
          ActorRef worker = makeKafkaWorker(kafkaJobName, props);
          _kafkaWorkers.add(worker);
          worker.tell(KafkaWorker.WORKER_START, getSelf());
          log.info("Started Kafka worker #{} for job {}", i, kafkaJobName);
        }
      } catch (Exception e) {
        log.error("Error starting Kafka job: " + kafkaJobName, e);
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
  public void postStop() throws Exception {
    log.info("Terminating KafkaClientMaster...");
    KafkaWorker.RUNNING = false;
    for (ActorRef worker : _kafkaWorkers) {
      getContext().stop(worker);
    }
  }

  private ActorRef makeKafkaWorker(@Nonnull String kafkaJobName, @Nonnull Properties props) throws Exception {
    final String consumerTopic = props.getProperty(Constant.KAFKA_CONSUMER_TOPIC_KEY, null);
    final String processor = props.getProperty(Constant.KAFKA_PROCESSOR_KEY, null);
    final String producerTopic = props.getProperty(Constant.KAFKA_PRODUCER_TOPIC_KEY, null);

    if (consumerTopic == null || processor == null) {
      throw new Exception("Missing required configs for: " + kafkaJobName);
    }

    // create consumer
    Properties consumerProp = getPropertyTrimPrefix(props, "consumer");

    KafkaConsumer<String, IndexedRecord> consumer = getConsumer(consumerProp);
    consumer.subscribe(Collections.singletonList(consumerTopic));

    // create producer if configured
    KafkaProducer<String, IndexedRecord> producer = null;
    if (producerTopic != null) {
      Properties producerProp = getPropertyTrimPrefix(props, "producer");
      producer = getProducer(producerProp);
    }

    // get processor instance
    Class processorClass = Class.forName(processor);
    Constructor<?> ctor = processorClass.getConstructor(Config.class, DaoFactory.class, String.class, KafkaProducer.class);
    KafkaMessageProcessor processorInstance =
        (KafkaMessageProcessor) ctor.newInstance(CONFIG, DAO_FACTORY, producerTopic, producer);

    // create worker
    return getContext().actorOf(Props.create(KafkaWorker.class, consumerTopic, consumer, processorInstance));
  }
}
