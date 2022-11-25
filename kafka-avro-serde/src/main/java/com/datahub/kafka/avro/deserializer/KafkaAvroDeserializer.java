package com.datahub.kafka.avro.deserializer;

import com.datahub.kafka.InMemorySchemaRegistry;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;


public class KafkaAvroDeserializer implements Deserializer<Object> {

  private static final InMemorySchemaRegistry REGISTRY = InMemorySchemaRegistry.getInstance();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    final Schema schema = REGISTRY.getSchema(topic);

    if (schema == null) {
      // TODO tell how to configure it
      throw new RuntimeException("Cannot find schema for topic " + topic + " please configure it");
    }

    try (ByteArrayInputStream os = new ByteArrayInputStream(data)) {
      DatumReader<Object> datumReader = new GenericDatumReader<>(schema);
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(os, null);
      return datumReader.read(data, decoder);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object deserialize(String topic, Headers headers, byte[] data) {
    return Deserializer.super.deserialize(topic, headers, data);
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}
