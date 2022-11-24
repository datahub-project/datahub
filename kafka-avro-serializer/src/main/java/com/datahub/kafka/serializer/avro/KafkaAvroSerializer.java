package com.datahub.kafka.serializer.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KafkaAvroSerializer implements Serializer<Object> {

  protected static final byte MAGIC_BYTE = 0x0;
  protected static final int idSize = 4;

  private class SchemaAndId {
    private final Schema schema;
    private final long id;

    public SchemaAndId(Schema schema, long id) {
      this.schema = schema;
      this.id = id;
    }

    public Schema getSchema() {
      return schema;
    }

    public long getId() {
      return id;
    }
  }
  private final Map<String, SchemaAndId> schemas = new HashMap<>();

  @Override
  public byte[] serialize(String topic, Object data) {
    SchemaAndId schemaAndId = schemas.get(topic);
    if (schemaAndId == null) {
      // TODO tell how to configure it
      throw new RuntimeException("Cannot find schema for topic " + topic + " please configure it");
    }
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()){

      DatumWriter<Object> datumWriter = getWriter(data, schemaAndId);
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(os, null);
      datumWriter.write(data,encoder);
      os.write(MAGIC_BYTE);
      os.write(ByteBuffer.allocate(idSize).putInt(schemaAndId.getId()).array());
      encoder.flush();
      return os.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  private DatumWriter<Object> getWriter(Object data, SchemaAndId schemaAndId) {
    if (data instanceof SpecificRecord) {
      return new SpecificDatumWriter<>(schemaAndId.getSchema());
    } else if (data instanceof GenericRecord) {
      return new GenericDatumWriter<>(schemaAndId.getSchema());
    } else {
      throw new RuntimeException("Cannot serialize instance of " + data.getClass());
    }
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    //        "datahub..name"
    for (Map.Entry<String, ?> entry: configs.entrySet()) {
      entry.getKey().startsWith("datahub");
    }
  }

  @Override
  public byte[] serialize(String topic, Headers headers, Object data) {
    return serialize(topic, data);
  }

  @Override
  public void close() { }
}