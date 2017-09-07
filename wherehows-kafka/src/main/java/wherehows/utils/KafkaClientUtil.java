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
package wherehows.utils;

import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;


public class KafkaClientUtil {

  /**
   * Get Kafka Consumer given full set of Properties
   * Required configs: bootstrap.servers , group.id , client.id , key.deserializer , value.deserializer
   * Optional configs if using avro deserializer: schema.registry.url, consumer.specific.avro.reader=true
   * @param props Properties
   * @return KafkaConsumer
   */
  public static KafkaConsumer<String, IndexedRecord> getConsumer(Properties props) {
    return new KafkaConsumer<>(props);
  }

  /**
   * Get Kafka producer given full set of Properties
   * Required configs: bootstrap.servers , client.id , key.serializer , value.serializer
   * Optional configs include: schema.registry.url
   * @param props Properties
   * @return KafkaProducer
   */
  public static KafkaProducer<String, IndexedRecord> getProducer(Properties props) {
    return new KafkaProducer<>(props);
  }

  /**
   * Get configs with certain key prefix, and trim that key prefix
   * @param props Properties
   * @return properties, keys have prefix trimmed
   */
  public static Properties getPropertyTrimPrefix(Properties props, String prefix) {
    Properties newProps = new Properties();

    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String key = (String) entry.getKey();
      if (key.startsWith(prefix)) {
        newProps.setProperty(key.substring(prefix.length() + 1), (String) entry.getValue());
      }
    }
    return newProps;
  }
}
