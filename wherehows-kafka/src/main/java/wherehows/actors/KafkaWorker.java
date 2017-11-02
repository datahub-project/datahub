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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import wherehows.processors.KafkaMessageProcessor;


/**
 * Akka actor for listening to a Kafka topic and waiting for messages.
 */
@Slf4j
public class KafkaWorker extends UntypedActor {

  public static boolean RUNNING = true;

  public static final String WORKER_START = "WORKER_START";

  private final int _consumer_poll_interval = 1000;

  private final String _topic;

  private TopicPartition _partition;  // assuming there is only one partition assigned to each worker

  private final KafkaConsumer<String, IndexedRecord> _consumer;

  private final KafkaMessageProcessor _processor;

  private long _receivedRecordCount;  // number of received kafka messages

  public KafkaWorker(String topic, KafkaConsumer<String, IndexedRecord> consumer, KafkaMessageProcessor processor) {
    this._topic = topic;
    this._consumer = consumer;
    this._processor = processor;
    this._receivedRecordCount = 0;
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (message.equals(WORKER_START)) {
      while (RUNNING) {
        try {
          ConsumerRecords<String, IndexedRecord> records = _consumer.poll(_consumer_poll_interval);
          for (ConsumerRecord<String, IndexedRecord> record : records) {
            _receivedRecordCount++;

            try {
              _processor.process(record.value());
            } catch (Exception e) {
              log.error("Processor Error: ", e);
              log.error("Message content: " + record.toString());
            }

            if (_receivedRecordCount % 1000 == 0) {
              log.info(_topic + " received " + _receivedRecordCount);
            }
          }

          _consumer.commitSync();
        } catch (SerializationException e) {
          log.error("Serialization Error: ", e);
          moveOffset(1);
        } catch (Exception e) {
          log.error("Consumer Error ", e);
          moveOffset(1);
        }
      }
    } else {
      unhandled(message);
    }
  }

  private void moveOffset(int amount) {
    // Assuming only one partition! Can't get partition info from ConsumerRecords if poll exception.
    List<PartitionInfo> partitions = _consumer.partitionsFor(_topic);
    if (partitions.size() != 1) {
      throw new RuntimeException("Kafka worker partition error: topic " + _topic + ", partitions " + partitions);
    }

    _partition = new TopicPartition(_topic, partitions.get(0).partition());
    // add amount to current offset
    long offset = _consumer.position(_partition) + amount;
    // seek to new offset
    _consumer.seek(_partition, offset);
    log.info("Set topic {} partition {} offset to {}", _topic, _partition, offset);
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
