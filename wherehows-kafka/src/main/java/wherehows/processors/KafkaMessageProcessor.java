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
package wherehows.processors;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import wherehows.dao.DaoFactory;


/**
 * Abstract class for Kafka message processor.
 */
public abstract class KafkaMessageProcessor {

  private final String _producerTopic;

  private final KafkaProducer<String, IndexedRecord> _procuder;

  public KafkaMessageProcessor(String producerTopic,
      KafkaProducer<String, IndexedRecord> producer) {
    this._producerTopic = producerTopic;
    this._procuder = producer;
  }

  /**
   * Abstract method 'process' to be implemented by specific processor
   * @param indexedRecord IndexedRecord
   */
  public abstract void process(IndexedRecord indexedRecord);

  protected Future<RecordMetadata> sendMessage(IndexedRecord message) {
    return this._procuder.send(new ProducerRecord(_producerTopic, message));
  }

}
