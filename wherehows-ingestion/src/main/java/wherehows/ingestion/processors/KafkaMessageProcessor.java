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
package wherehows.ingestion.processors;

import java.util.Properties;
import javax.annotation.Nonnull;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import wherehows.dao.DaoFactory;


/**
 * Abstract class for Kafka message processor.
 */
public abstract class KafkaMessageProcessor {

  final Properties _config;

  final DaoFactory _daoFactory;

  private final String _producerTopic;

  private final KafkaProducer<String, IndexedRecord> _producer;

  KafkaMessageProcessor(@Nonnull Properties config, @Nonnull DaoFactory daoFactory, @Nonnull String producerTopic,
      @Nonnull KafkaProducer<String, IndexedRecord> producer) {
    this._config = config;
    this._daoFactory = daoFactory;
    this._producerTopic = producerTopic;
    this._producer = producer;
  }

  /**
   * Abstract method 'process' to be implemented by specific processor
   * @param indexedRecord IndexedRecord
   */
  public abstract void process(IndexedRecord indexedRecord);

  void sendMessage(IndexedRecord message) {
    this._producer.send(new ProducerRecord(_producerTopic, message));
  }
}
