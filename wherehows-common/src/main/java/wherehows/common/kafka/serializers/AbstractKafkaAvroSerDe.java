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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import wherehows.common.kafka.SchemaId;
import wherehows.common.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.SchemaRegistryClient;
import wherehows.common.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * Common fields and helper methods for both the serializer and the deserializer.
 */
public abstract class AbstractKafkaAvroSerDe {
  protected static final byte MAGIC_BYTE = 0x0;
  //protected static final int idSize = 4;
  protected static final SchemaId.Type ID_TYPE = SchemaId.Type.UUID;

  private static final Map<String, Schema> primitiveSchemas;
  protected SchemaRegistryClient schemaRegistry;

  static {
    Schema.Parser parser = new Schema.Parser();
    primitiveSchemas = new HashMap<>();
    primitiveSchemas.put("Null", createPrimitiveSchema(parser, "null"));
    primitiveSchemas.put("Boolean", createPrimitiveSchema(parser, "boolean"));
    primitiveSchemas.put("Integer", createPrimitiveSchema(parser, "int"));
    primitiveSchemas.put("Long", createPrimitiveSchema(parser, "long"));
    primitiveSchemas.put("Float", createPrimitiveSchema(parser, "float"));
    primitiveSchemas.put("Double", createPrimitiveSchema(parser, "double"));
    primitiveSchemas.put("String", createPrimitiveSchema(parser, "string"));
    primitiveSchemas.put("Bytes", createPrimitiveSchema(parser, "bytes"));
  }

  private static Schema createPrimitiveSchema(Schema.Parser parser, String type) {
    String schemaString = String.format("{\"type\" : \"%s\"}", type);
    return parser.parse(schemaString);
  }

  protected void configureClientProperties(AbstractKafkaAvroSerDeConfig config) {
    try {
      List<String> urls = config.getSchemaRegistryUrls();
      int  maxSchemaObject = config.getMaxSchemasPerSubject();

      if (schemaRegistry == null) {
        schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject);
      }
    } catch (io.confluent.common.config.ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  /**
   * Get the subject name for the given topic and value type.
   */
  protected static String getSubjectName(String topic, boolean isKey) {
    if (isKey) {
      return topic + "-key";
    } else {
      return topic + "-value";
    }
  }

  /**
   * Get the subject name used by the old Encoder interface, which relies only on the value type rather than the topic.
   */
  protected static String getOldSubjectName(Object value) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema().getName() + "-value";
    } else {
      throw new SerializationException("Primitive types are not supported yet");
    }
  }

  protected Schema getSchema(Object object) {
    if (object == null) {
      return primitiveSchemas.get("Null");
    } else if (object instanceof Boolean) {
      return primitiveSchemas.get("Boolean");
    } else if (object instanceof Integer) {
      return primitiveSchemas.get("Integer");
    } else if (object instanceof Long) {
      return primitiveSchemas.get("Long");
    } else if (object instanceof Float) {
      return primitiveSchemas.get("Float");
    } else if (object instanceof Double) {
      return primitiveSchemas.get("Double");
    } else if (object instanceof CharSequence) {
      return primitiveSchemas.get("String");
    } else if (object instanceof byte[]) {
      return primitiveSchemas.get("Bytes");
    } else if (object instanceof GenericContainer) {
      return ((GenericContainer) object).getSchema();
    } else {
      throw new IllegalArgumentException(
          "Unsupported Avro type. Supported types are null, Boolean, Integer, Long, "
              + "Float, Double, String, byte[] and IndexedRecord");
    }
  }

  public String register(String subject, Schema schema) throws IOException, RestClientException {
    return schemaRegistry.register(subject, schema);
  }

  public Schema getByID(String id) throws IOException, RestClientException {
    return schemaRegistry.getByID(id);
  }

  public Schema getBySubjectAndID(String subject, String id) throws IOException, RestClientException {
    return schemaRegistry.getBySubjectAndID(subject, id);
  }
}
