package com.linkedin.mxe;

import com.linkedin.avro.legacy.LegacyAvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityChecker;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.avro.*;
import org.testng.Reporter;
import org.testng.annotations.Test;

import java.util.Random;

import static com.linkedin.mxe.RegisterSchemas.DEFAULT_SCHEMA_REGISTRY_URL;
import static org.testng.Assert.*;


public class SchemaCompatibilityTests {

  private static final String FORWARD_INCOMPATIBILITY_WARNING = "" // format
      + "************************************************************************\n" //format
      + " New schema is forward incompatible with the current schema.\n" // format
      + " Consider bumping up the major version.\n" // format
      + "************************************************************************\n";

  private static final AvroCompatibilityChecker AVRO_BACKWARD_COMPATIBILITY_CHECKER =
          AvroCompatibilityChecker.BACKWARD_CHECKER;

  private static final AvroCompatibilityChecker AVRO_FORWARD_COMPATIBILITY_CHECKER =
          AvroCompatibilityChecker.FORWARD_CHECKER;

  @Test
  public void testBackwardCompatibility() {
    final CachedSchemaRegistryClient client = RegisterSchemas.createClient(DEFAULT_SCHEMA_REGISTRY_URL);

    Configs.TOPIC_SCHEMA_MAP.forEach((topic, schema) -> {
      Schema olderSchema = findLastRegisteredSchemaMetadata(topic, client);
      if (olderSchema == null) {
        Reporter.log("Unable to find registered schema for " + topic + true);
        return;
      }

      // Check backward compatibility, i.e. can new schema fits old data
      assertTrue(AVRO_BACKWARD_COMPATIBILITY_CHECKER.isCompatible(schema, olderSchema),
              "New schema is backward incompatible with the current schema \n\n");

      // Check forward compatibility, i.e. can new data fits old schema
      assertTrue(AVRO_FORWARD_COMPATIBILITY_CHECKER.isCompatible(schema, olderSchema),
              FORWARD_INCOMPATIBILITY_WARNING + "\n\n");
    });
  }

  private Schema findLastRegisteredSchemaMetadata(String topic, CachedSchemaRegistryClient client) {
    try {
      SchemaMetadata metadata = client.getLatestSchemaMetadata(topic);
      return client.getBySubjectAndID(topic, metadata.getId());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private SchemaMetadata makeSchemaMetadata(String topic, Schema schema) {
    LegacyAvroSchema legacyAvroSchema = new LegacyAvroSchema(topic, schema.toString(false));
    Random rand = new Random();
    return new SchemaMetadata(rand.nextInt(), rand.nextInt(), legacyAvroSchema.toString());
  }
}
