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

import akka.actor.UntypedActor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import wherehows.processors.KafkaMessageProcessor;


/**
 * Akka actor for listening to a Kafka topic and waiting for messages.
 */
@Slf4j
public class KafkaWorker extends UntypedActor {

  public static boolean RUNNING = true;

  private final String _topic;

  private final KafkaConsumer<String, IndexedRecord> _consumer;

  private final KafkaMessageProcessor _processor;

  private final int _consumer_poll_interval = 1000;

  private int _receivedRecordCount;

  public KafkaWorker(String topic, KafkaConsumer<String, IndexedRecord> consumer, KafkaMessageProcessor processor) {
    this._topic = topic;
    this._consumer = consumer;
    this._processor = processor;
    this._receivedRecordCount = 0;  // number of received kafka messages
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message.equals("ApplicationStart")) {
      try {
        while (RUNNING) {
          ConsumerRecords<String, IndexedRecord> records = _consumer.poll(_consumer_poll_interval);
          for (ConsumerRecord<String, IndexedRecord> record : records) {
            _receivedRecordCount++;

            try {
              _processor.process(record.value());
            } catch (Exception e) {
              log.error("Processor Error: ", e);
            }

            if (_receivedRecordCount % 1000 == 0) {
              log.debug(_topic + " received " + _receivedRecordCount);
            }
          }

          _consumer.commitSync();
        }
      } catch (Exception e) {
        log.error("Consumer Error ", e);
      } finally {
        log.info("Shutting down consumer and worker for topic: " + _topic);
        try {
          _consumer.close();
        } catch (Exception e) {
          log.error("Error closing consumer for topic " + _topic + " : " + e.toString());
        }
      }
    } else {
      unhandled(message);
    }
  }

  @Override
  public void postStop() throws Exception {
    log.info("Stop worker for topic: " + _topic);
    try {
      _consumer.close();
    } catch (Exception e) {
      log.error("Error closing consumer for topic " + _topic + " : " + e.toString());
    }
  }
}
