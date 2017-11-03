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

import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.dao.DaoFactory;


/**
 * Abstract class for Kafka message processor.
 */
public abstract class KafkaMessageProcessor {

  protected final DaoFactory DAO_FACTORY;

  protected final String _producerTopic;

  protected final KafkaProducer<String, IndexedRecord> PRODUCER;

  public KafkaMessageProcessor(DaoFactory daoFactory, String producerTopic, KafkaProducer<String, IndexedRecord> producer) {
    this.DAO_FACTORY = daoFactory;
    this._producerTopic = producerTopic;
    this.PRODUCER = producer;
  }

  /**
   * Abstract method 'process' to be implemented by specific processor
   * @param indexedRecord IndexedRecord
   */
  public abstract void process(IndexedRecord indexedRecord);

}
