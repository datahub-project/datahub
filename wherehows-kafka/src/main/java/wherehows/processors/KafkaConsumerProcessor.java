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
import org.apache.avro.generic.GenericData;



/**
 * Abstract class for Kafka consumer message processor.
 *
 */
@Slf4j
public abstract class KafkaConsumerProcessor {

  /**
   * Abstract method 'process' to be implemented by specific processor
   * input Kafka record, process information and write to DB.
   * @param record GenericData.Record
   * @param topic
   * @return wherehows.common.schemas.Record
   * @throws Exception
   */
  public abstract void process(GenericData.Record record, String topic) throws Exception;

}
