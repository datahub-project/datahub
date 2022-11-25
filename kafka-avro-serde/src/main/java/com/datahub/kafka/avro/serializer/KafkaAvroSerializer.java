package com.datahub.kafka.avro.serializer;

import com.datahub.kafka.InMemorySchemaRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaAvroSerializer implements Serializer<Object> {

  private static final InMemorySchemaRegistry REGISTRY = InMemorySchemaRegistry.getInstance();

  @Override
  public byte[] serialize(String topic, Object data) {
    final Schema schema = REGISTRY.getSchema(topic);

    if (schema == null) {
      // TODO tell how to configure it
      throw new RuntimeException("Cannot find schema for topic " + topic + " please configure it");
    }

    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
      datumWriter.write(data, encoder);
      encoder.flush();
      return os.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, Object data) {
    return Serializer.super.serialize(topic, headers, data);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}