/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.models.util;

import static org.junit.Assert.*;

import java.util.Map;
import org.junit.Test;

public class SchemaKeyTest {
  @Test
  public void testSchemaUrnGeneration() {
    // Test data
    SchemaKey schemaKey =
        SchemaKey.builder()
            .platform("test-platform")
            .instance("DEV")
            .database("test-database")
            .schema("test-schema")
            .build();

    System.out.println(schemaKey.guidDict());

    // Generate URN
    String urn = schemaKey.asUrnString();
    // With instance
    String expectedUrn =
        TestHelper.generateContainerKeyGuid(
            "schema",
            Map.of(
                "platform", "test-platform",
                "instance", "DEV",
                "database", "test-database",
                "schema", "test-schema"));

    // Assert
    assertEquals(expectedUrn, urn);
  }

  @Test
  public void testSchemaUrnGenerationNoInstance() {
    // Test data
    ContainerKey containerKey =
        SchemaKey.builder()
            .platform("test-platform")
            .database("test-database")
            .schema("test-schema")
            .build();

    // Generate URN
    String urn = containerKey.asUrnString();
    // Without instance
    String expectedUrn =
        TestHelper.generateContainerKeyGuid(
            "schema",
            Map.of(
                "platform", "test-platform",
                "database", "test-database",
                "schema", "test-schema"));

    // Assert
    assertEquals(expectedUrn, urn);
  }
}
