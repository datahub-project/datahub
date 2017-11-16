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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

  private static final int POLL_TIMEOUT_MS = 1000;

  private static final Pattern DESERIALIZATION_ERROR_PARTITION_PATTERN =
      Pattern.compile("Error deserializing key/value for partition .+-(\\d+) at offset \\d+");

  private final String _topic;

  private final KafkaConsumer<String, IndexedRecord> _consumer;

  private final KafkaMessageProcessor _processor;

  private long _receivedRecordCount;  // number of received kafka messages

  public KafkaWorker(@Nonnull String topic, @Nonnull KafkaConsumer<String, IndexedRecord> consumer,
      @Nonnull KafkaMessageProcessor processor) {
    this._topic = topic;
    this._consumer = consumer;
    this._processor = processor;
    this._receivedRecordCount = 0;
  }

  @Override
  public void onReceive(@Nonnull Object message) throws Exception {
    if (!message.equals(WORKER_START)) {
      log.warn("Must send WORKER_START message first!");
      unhandled(message);
    }

    while (RUNNING) {
      ConsumerRecords<String, IndexedRecord> records;
      try {
        records = _consumer.poll(POLL_TIMEOUT_MS);
      } catch (SerializationException e) {
        log.error("Serialization Error: ", e);
        moveOffset(extractPartition(e), 1);
        continue;
      }

      try {
        process(records);
      } catch (Exception e) {
        log.error("Unhandled processing exception: ", e);
        break;
      }
    }

    getContext().stop(getSelf());
  }

  private void process(@Nonnull ConsumerRecords<String, IndexedRecord> records) {
    for (ConsumerRecord<String, IndexedRecord> record : records) {
      _receivedRecordCount++;
      _processor.process(record.value());
      if (_receivedRecordCount % 1000 == 0) {
        log.info("{}: received {} messages", _topic, _receivedRecordCount);
      }
    }
    _consumer.commitAsync();
  }

  @Nonnull
  private TopicPartition extractPartition(@Nonnull SerializationException exception) {
    // Manually extract the partition from exception until https://issues.apache.org/jira/browse/KAFKA-4740 is fixed.
    Matcher matcher = DESERIALIZATION_ERROR_PARTITION_PATTERN.matcher(exception.getMessage());
    if (!matcher.find()) {
      throw new RuntimeException("Unable to parse deserialization error message");
    }
    int partition = Integer.parseInt(matcher.group(1));
    return new TopicPartition(_topic, partition);
  }

  private void moveOffset(@Nonnull TopicPartition partition, int amount) {
    long offset = _consumer.position(partition) + amount;
    // seek to new offset
    _consumer.seek(partition, offset);
    log.info("Set topic {} partition {} offset to {}", _topic, partition, offset);
    _consumer.commitAsync();
  }

  @Override
  public void postStop() throws Exception {
    log.info("Stop worker for topic: {}", _topic);
    try {
      _consumer.close();
    } catch (Exception e) {
      log.error("Error closing consumer for topic {}: ", _topic, e);
    }
  }
}
