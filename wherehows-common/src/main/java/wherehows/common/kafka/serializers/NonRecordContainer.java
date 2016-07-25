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
import org.apache.kafka.common.errors.SerializationException;

import java.util.Objects;

/**
 * Wrapper for all non-record types that includes the schema for the data.
 */
public class NonRecordContainer implements GenericContainer {
  private final Schema schema;
  private final Object value;

  public NonRecordContainer(Schema schema, Object value) {
    if (schema == null)
      throw new SerializationException("Schema may not be null.");
    this.schema = schema;
    this.value = value;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NonRecordContainer that = (NonRecordContainer) o;
    return Objects.equals(schema, that.schema) &&
            Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, value);
  }
}
