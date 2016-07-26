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

package wherehows.common.kafka.schemaregistry.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.ArrayList;
import java.util.List;

public class AvroCompatibilityChecker {

  // Check if the new schema can be used to read data produced by the latest schema
  private static SchemaValidator BACKWARD_VALIDATOR =
      new SchemaValidatorBuilder().canReadStrategy().validateLatest();
  public static AvroCompatibilityChecker BACKWARD_CHECKER = new AvroCompatibilityChecker(
      BACKWARD_VALIDATOR);
  // Check if data produced by the new schema can be read by the latest schema
  private static SchemaValidator FORWARD_VALIDATOR =
      new SchemaValidatorBuilder().canBeReadStrategy().validateLatest();
  public static AvroCompatibilityChecker FORWARD_CHECKER = new AvroCompatibilityChecker(
      FORWARD_VALIDATOR);
  // Check if the new schema is both forward and backward compatible with the latest schema
  private static SchemaValidator FULL_VALIDATOR =
      new SchemaValidatorBuilder().mutualReadStrategy().validateLatest();
  public static AvroCompatibilityChecker FULL_CHECKER = new AvroCompatibilityChecker(
      FULL_VALIDATOR);
  private static SchemaValidator NO_OP_VALIDATOR = new SchemaValidator() {
    @Override
    public void validate(Schema schema, Iterable<Schema> schemas) throws SchemaValidationException {
      // do nothing
    }
  };
  public static AvroCompatibilityChecker NO_OP_CHECKER = new AvroCompatibilityChecker(
      NO_OP_VALIDATOR);
  private final SchemaValidator validator;

  private AvroCompatibilityChecker(SchemaValidator validator) {
    this.validator = validator;
  }

  /**
   * Check the compatibility between the new schema and the latest schema
   */
  public boolean isCompatible(Schema newSchema, Schema latestSchema) {
    List<Schema> schemas = new ArrayList<Schema>();
    schemas.add(latestSchema);

    try {
      validator.validate(newSchema, schemas);
    } catch (SchemaValidationException e) {
      return false;
    }

    return true;
  }
}
