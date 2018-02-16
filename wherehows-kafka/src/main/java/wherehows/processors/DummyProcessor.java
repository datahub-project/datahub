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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import wherehows.dao.DaoFactory;


/**
 * Simple Kafka message processor that prints the record to log.
 */
@Slf4j
public class DummyProcessor extends KafkaMessageProcessor {

  public DummyProcessor(DaoFactory daoFactory, String producerTopic, KafkaProducer<String, IndexedRecord> producer) {
    super(producerTopic, producer);
  }

  /**
   * Simply print the message content
   * @param indexedRecord IndexedRecord
   */
  public void process(IndexedRecord indexedRecord) {
    log.info(indexedRecord.toString());
    //System.out.println(record.toString());
  }
}
