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
package wherehows.ingestion.converters;

import org.apache.avro.generic.IndexedRecord;


/**
 * Interface for Kafka message converter.
 */
public interface KafkaMessageConverter<T extends IndexedRecord> {

  /**
   * Method 'convert' to be implemented by specific converter.
   * Input one kafka message, output one converted kafka message.
   *
   * @param indexedRecord IndexedRecord
   * @return IndexedRecord
   */
  T convert(T indexedRecord);
}
