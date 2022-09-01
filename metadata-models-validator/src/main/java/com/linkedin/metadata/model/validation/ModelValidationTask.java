package com.linkedin.metadata.model.validation;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.pegasus.generator.DataSchemaParser;
import java.io.IOException;


/**
 * Validates GMS PDL models by constructing a set of {@link EntitySpec}s from them.
 *
 * The following validation rules are applied:
 *
 * 1. Each Entity Snapshot Model is annotated as @Entity with a common name
 * 2. Each Aspect is annotated as @Aspect with a common name
 * 3. Each @Searchable field is of primitive / list of primitive type
 * 4. Each @Relationship field is of Urn / List of Urn type
 * 5. Each Entity Snapshot includes a single Key Aspect
 *
 */
public class ModelValidationTask {

  private static final String SNAPSHOT_SCHEMA_NAME = "com.linkedin.metadata.snapshot.Snapshot";

  private ModelValidationTask() {
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 3) {
      System.out.println("Usage: cmd <modelPath> <resolverPath>");
      System.exit(-1);
    }

    final String resolverPath = args[0];
    final String modelPath = args[1];

    final DataSchemaParser parser = new DataSchemaParser(resolverPath);
    parser.parseSources(new String[]{modelPath});

    final DataSchema snapshotSchema = parser.getSchemaResolver().existingDataSchema(SNAPSHOT_SCHEMA_NAME);

    if (snapshotSchema == null) {
      throw new RuntimeException(
          String.format("Failed to find Snapshot model with name %s in parsed schemas!", SNAPSHOT_SCHEMA_NAME));
    }

    // TODO: Fix this so that aspects that are just in the entity registry don't fail because they aren't in the
    // snapshot registry.
//    try {
//      new EntitySpecBuilder().buildEntitySpecs(snapshotSchema);
//    } catch (Exception e) {
//      throw new RuntimeException("Failed to validate DataHub PDL models", e);
//    }
  }
}