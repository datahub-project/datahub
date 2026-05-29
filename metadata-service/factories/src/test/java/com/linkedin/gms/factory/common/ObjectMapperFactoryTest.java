package com.linkedin.gms.factory.common;

import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class ObjectMapperFactoryTest {

  @Test
  public void testPrimaryMapperParsesValidJson() throws Exception {
    ObjectMapper mapper = new ObjectMapperFactory().objectMapper();
    String validJson = "{\"query\": \"{ me { corpUser { urn } } }\"}";
    var node = mapper.readTree(validJson);
    assertNotNull(node);
    assertEquals(node.get("query").asText(), "{ me { corpUser { urn } } }");
  }

  @Test
  public void testApiSanitizingMapperParsesValidJson() throws Exception {
    String validJson = "{\"query\": \"{ me { corpUser { urn } } }\"}";
    var node = ObjectMapperFactory.API_SANITIZING_MAPPER.readTree(validJson);
    assertNotNull(node);
    assertEquals(node.get("query").asText(), "{ me { corpUser { urn } } }");
  }

  @Test
  public void testApiSanitizingMapperRedactsSourceInParseErrors() {
    String malformedJson = "{\"key\": ]bad}";
    try {
      ObjectMapperFactory.API_SANITIZING_MAPPER.readTree(malformedJson);
      fail("Expected JsonParseException for malformed JSON");
    } catch (Exception e) {
      assertTrue(e instanceof JsonParseException);
      String message = e.getMessage();
      assertFalse(
          message.contains(malformedJson),
          "Parse error must not contain the raw input: " + message);
      assertTrue(
          message.contains("REDACTED"),
          "Source should be REDACTED when INCLUDE_SOURCE_IN_LOCATION is disabled: " + message);
    }
  }

  @Test
  public void testPrimaryMapperIsNotAffected() {
    ObjectMapper primary = new ObjectMapperFactory().objectMapper();
    assertNotSame(
        primary.getFactory(),
        ObjectMapperFactory.API_SANITIZING_MAPPER.getFactory(),
        "Mappers must use separate JsonFactory instances");
  }
}
