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
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.common.Constant;
import wherehows.common.utils.JobsUtil;
import wherehows.dao.DaoFactory;
import wherehows.msgs.KafkaCommMsg;
import wherehows.processors.KafkaMessageProcessor;

import static wherehows.main.ApplicationStart.DAO_FACTORY;
import static wherehows.utils.KafkaClientUtil.*;


@Slf4j
public class KafkaClientMaster extends UntypedActor {

  private final String KAFKA_JOB_DIR;

  // Map of kafka job name to properties
  private static Map<String, Properties> _kafkaJobList;

  // Map of kafka job name to worker actor
  private static Map<String, ActorRef> _kafkaWorkers = new HashMap<>();

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
      log.info("Create Kafka client with config: " + props);
      try {
        final String consumerTopic = props.getProperty(Constant.KAFKA_CONSUMER_TOPIC_KEY, null);
        final String processor = props.getProperty(Constant.KAFKA_PROCESSOR_KEY, null);
        final String producerTopic = props.getProperty(Constant.KAFKA_PRODUCER_TOPIC_KEY, null);

        if (consumerTopic == null || processor == null) {
          log.error("Missing required configs for: " + kafkaJobName);
          continue;
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
        Constructor<?> ctor = processorClass.getConstructor(DaoFactory.class, KafkaProducer.class);
        KafkaMessageProcessor processorInstance = (KafkaMessageProcessor) ctor.newInstance(DAO_FACTORY, producer);

        // create worker
        ActorRef worker =
            getContext().actorOf(Props.create(KafkaWorker.class, consumerTopic, consumer, processorInstance));

        _kafkaWorkers.put(consumerTopic, worker);
        worker.tell("ApplicationStart", getSelf());

        log.info("Started Kafka job: " + kafkaJobName + " listening to topic: " + consumerTopic);
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
    for (ActorRef worker : _kafkaWorkers.values()) {
      getContext().stop(worker);
    }
  }
}
