package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StructuredPropertiesPatchBuilderTest {

  private TestableStructuredPropertiesPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_PROPERTY_URN =
      "urn:li:structuredProperty:io.acryl.classification";

  // Test helper class to expose protected method
  private static class TestableStructuredPropertiesPatchBuilder
      extends StructuredPropertiesPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableStructuredPropertiesPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testSetStringProperty() throws URISyntaxException {
    Urn propertyUrn = Urn.createFromString(TEST_PROPERTY_URN);
    builder.setStringProperty(propertyUrn, "confidential");
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/properties/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("propertyUrn").asText(), propertyUrn.toString());

    JsonNode values = operation.getRight().get("values");
    assertNotNull(values);
    assertTrue(values.isArray());
    assertEquals(values.get(0).get("string").asText(), "confidential");
  }

  @Test
  public void testSetNumberProperty() throws URISyntaxException {
    Urn propertyUrn = Urn.createFromString(TEST_PROPERTY_URN);
    builder.setNumberProperty(propertyUrn, 42);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/properties/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("propertyUrn").asText(), propertyUrn.toString());

    JsonNode values = operation.getRight().get("values");
    assertNotNull(values);
    assertTrue(values.isArray());
    assertEquals(values.get(0).get("double").asInt(), 42);
  }

  @Test
  public void testRemoveProperty() throws URISyntaxException {
    Urn propertyUrn = Urn.createFromString(TEST_PROPERTY_URN);
    builder.removeProperty(propertyUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/properties/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testBuildPlainAdd() throws Exception {
    Urn propertyUrn = Urn.createFromString(TEST_PROPERTY_URN);
    MetadataChangeProposal mcp = builder.setStringProperty(propertyUrn, "value").build();

    assertEquals(mcp.getAspect().getContentType(), "application/json-patch+json");

    byte[] bytes = mcp.getAspect().getValue().copyBytes();
    JsonNode payload = new ObjectMapper().readTree(bytes);

    assertTrue(payload.isObject());
    JsonNode patches = payload.get("patch");
    assertTrue(patches.isArray());
    assertEquals(patches.size(), 1);
    JsonNode op = patches.get(0);
    assertEquals(op.get("op").asText(), "add");
    assertTrue(op.get("path").asText().startsWith("/properties/"));
  }
}
