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

package wherehows.common.kafka.formatter;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import wherehows.common.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import wherehows.common.kafka.serializers.AbstractKafkaAvroSerializer;
import kafka.common.KafkaException;
import kafka.producer.KeyedMessage;
import kafka.common.MessageReader;

/**
 * Example
 * To use AvroMessageReader, first make sure that Zookeeper, Kafka and schema registry server are
 * all started. Second, make sure the jar for AvroMessageReader and its dependencies are included
 * in the classpath of kafka-console-producer.sh. Then run the following
 * command.
 *
 * 1. Send Avro string as value. (make sure there is no space in the schema string)
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic t1 \
 *   --line-reader io.confluent.kafka.formatter.AvroMessageReader \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property value.schema='{"type":"string"}'
 *
 * In the shell, type in the following.
 * "a"
 * "b"
 *
 * 2. Send Avro record as value.
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic t1 \
 *   --line-reader io.confluent.kafka.formatter.AvroMessageReader \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
 *
 * In the shell, type in the following.
 * {"f1": "value1"}
 *
 * 3. Send Avro string as key and Avro record as value.
 * bin/kafka-console-producer.sh --broker-list localhost:9092 --topic t1 \
 *   --line-reader io.confluent.kafka.formatter.AvroMessageReader \
 *   --property schema.registry.url=http://localhost:8081 \
 *   --property parse.key=true \
 *   --property key.schema='{"type":"string"}' \
 *   --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
 *
 * In the shell, type in the following.
 * "key1" \t {"f1": "value1"}
 *
 */
public class AvroMessageReader extends AbstractKafkaAvroSerializer implements MessageReader {

  private String topic = null;
  private BufferedReader reader = null;
  private Boolean parseKey = false;
  private String keySeparator = "\t";
  private boolean ignoreError = false;
  private final DecoderFactory decoderFactory = DecoderFactory.get();
  private Schema keySchema = null;
  private Schema valueSchema = null;
  private String keySubject = null;
  private String valueSubject = null;

  /**
   * Constructor needed by kafka console producer.
   */
  public AvroMessageReader() {
  }

  /**
   * For testing only.
   */
  AvroMessageReader(SchemaRegistryClient schemaRegistryClient, Schema keySchema, Schema valueSchema,
                    String topic, boolean parseKey, BufferedReader reader) {
    this.schemaRegistry = schemaRegistryClient;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    this.topic = topic;
    this.keySubject = topic + "-key";
    this.valueSubject = topic + "-value";
    this.parseKey = parseKey;
    this.reader = reader;
  }

  @Override
  public void init(java.io.InputStream inputStream, java.util.Properties props) {
    topic = props.getProperty("topic");
    if (props.containsKey("parse.key")) {
      parseKey = props.getProperty("parse.key").trim().toLowerCase().equals("true");
    }
    if (props.containsKey("key.separator")) {
      keySeparator = props.getProperty("key.separator");
    }
    if (props.containsKey("ignore.error")) {
      ignoreError = props.getProperty("ignore.error").trim().toLowerCase().equals("true");
    }
    reader = new BufferedReader(new InputStreamReader(inputStream));
    String url = props.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    if (url == null) {
      throw new ConfigException("Missing schema registry url!");
    }
    schemaRegistry = new CachedSchemaRegistryClient(
        url, AbstractKafkaAvroSerDeConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT);
    if (!props.containsKey("value.schema")) {
      throw new ConfigException("Must provide the Avro schema string in value.schema");
    }
    String valueSchemaString = props.getProperty("value.schema");
    Schema.Parser parser = new Schema.Parser();
    valueSchema = parser.parse(valueSchemaString);

    if (parseKey) {
      if (!props.containsKey("key.schema")) {
        throw new ConfigException("Must provide the Avro schema string in key.schema");
      }
      String keySchemaString = props.getProperty("key.schema");
      keySchema = parser.parse(keySchemaString);
    }
    keySubject = topic + "-key";
    valueSubject = topic + "-value";
  }

  @Override
  public ProducerRecord<byte[], byte[]> readMessage() {
    try {
      String line = reader.readLine();
      if (line == null) {
        return null;
      }
      if (!parseKey) {
        Object value = jsonToAvro(line, valueSchema);
        byte[] serializedValue = serializeImpl(valueSubject, value);
        return new ProducerRecord<>(topic, serializedValue);
      } else {
        int keyIndex = line.indexOf(keySeparator);
        if (keyIndex < 0) {
          if (ignoreError) {
            Object value = jsonToAvro(line, valueSchema);
            byte[] serializedValue = serializeImpl(valueSubject, value);
            return new ProducerRecord<>(topic, serializedValue);
          } else {
            throw new KafkaException("No key found in line " + line);
          }
        } else {
          String keyString = line.substring(0, keyIndex);
          String valueString = (keyIndex + keySeparator.length() > line.length()) ?
                               "" : line.substring(keyIndex + keySeparator.length());
          Object key = jsonToAvro(keyString, keySchema);
          byte[] serializedKey = serializeImpl(keySubject, key);
          Object value = jsonToAvro(valueString, valueSchema);
          byte[] serializedValue = serializeImpl(valueSubject, value);
          return new ProducerRecord<>(topic, serializedKey, serializedValue);
        }
      }
    } catch (IOException e) {
      throw new KafkaException("Error reading from input", e);
    }
  }

  private Object jsonToAvro(String jsonString, Schema schema) {
    try {
      DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
      Object object = reader.read(null, decoderFactory.jsonDecoder(schema, jsonString));

      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;
    } catch (IOException e) {
      throw new SerializationException(
          String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
    } catch (AvroRuntimeException e) {
      throw new SerializationException(
          String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
    }
  }

  @Override
  public void close() {
    // nothing to do
  }
}
