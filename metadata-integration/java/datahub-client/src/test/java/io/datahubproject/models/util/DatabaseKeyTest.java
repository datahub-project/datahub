package io.datahubproject.models.util;

import static org.junit.Assert.*;

import java.util.Map;
import org.junit.Test;

public class DatabaseKeyTest {
  @Test
  public void testDatabaseUrnGeneration() {
    // Test data
    DatabaseKey databaseKey =
        DatabaseKey.builder()
            .platform("test-platform")
            .instance("DEV")
            .database("test-database")
            .build();

    System.out.println(databaseKey.guidDict());

    // Generate URN
    String urn = databaseKey.asUrnString();
    // With instance
    //    "urn:li:container:e40f103ea7c6def4f4b24cd858d5e412";
    String expectedUrn =
        TestHelper.generateContainerKeyGuid(
            "database",
            Map.of("platform", "test-platform", "instance", "DEV", "database", "test-database"));

    // Assert
    assertEquals(expectedUrn, urn);
  }

  @Test
  public void testDatabaseUrnGenerationNoInstance() {
    // Test data
    ContainerKey containerKey =
        DatabaseKey.builder().platform("test-platform").database("test-database").build();

    // Generate URN
    String urn = containerKey.asUrnString();
    // Without instance
    // "urn:li:container:1929d86c0a92e2d3bb9ba193c8c2b66f";
    String expectedUrn =
        TestHelper.generateContainerKeyGuid(
            "database", Map.of("platform", "test-platform", "database", "test-database"));

    // Assert
    assertEquals(expectedUrn, urn);
  }
}
