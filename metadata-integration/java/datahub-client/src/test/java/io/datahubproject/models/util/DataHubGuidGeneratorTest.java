package io.datahubproject.models.util;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class DataHubGuidGeneratorTest {

  @Test
  public void testGuidGeneration() throws NoSuchAlgorithmException, JsonProcessingException {
    // Test data
    Map<String, String> obj = new HashMap<>();
    obj.put("container", "test-container");

    // Generate GUID
    String guid = DataHubGuidGenerator.dataHubGuid(obj);

    // Assert
    assertEquals("4d90f727b9d10ba7cea297dc8b427985", guid);
  }

  @Test
  public void testContainerUrnGeneration() {
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
    String expectedUrn = "urn:li:container:e40f103ea7c6def4f4b24cd858d5e412";

    // Assert
    assertEquals(expectedUrn, urn);
  }

  @Test
  public void testContainerUrnGenerationNoInstance() {
    // Test data
    ContainerKey containerKey =
        DatabaseKey.builder().platform("test-platform").database("test-database").build();

    // Generate URN
    String urn = containerKey.asUrnString();
    // Without instance
    String expectedUrn = "urn:li:container:1929d86c0a92e2d3bb9ba193c8c2b66f";

    // Assert
    assertEquals(expectedUrn, urn);
  }
}
