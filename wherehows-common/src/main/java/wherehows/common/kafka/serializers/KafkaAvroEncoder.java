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

package wherehows.common.kafka.serializers;

import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import wherehows.common.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * At present, this only works for value not key and it only supports IndexedRecord. To register for
 * a topic, user will need to provide a list of record_name:topic_name map as a CSV format. By
 * default, the encoder will use record name as topic.
 */
public class KafkaAvroEncoder extends AbstractKafkaAvroSerializer implements Encoder<Object> {

  public KafkaAvroEncoder(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  /**
   * Constructor used by Kafka producer.
   */
  public KafkaAvroEncoder(VerifiableProperties props) {
    configure(serializerConfig(props));
  }

  @Override
  public byte[] toBytes(Object object) {
    return serializeImpl(getOldSubjectName(object), object);
  }
}
