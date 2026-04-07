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

public class DocumentationPatchBuilderTest {

  private TestableDocumentationPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_SOURCE_URN = "urn:li:corpuser:docBot";

  // Test helper class to expose protected method
  private static class TestableDocumentationPatchBuilder extends DocumentationPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableDocumentationPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testAddDocumentationUnattributed() throws URISyntaxException {
    builder.addDocumentation("Some documentation text");
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    // Unattributed: BASE_PATH + "" = "/documentations/" (trailing slash only)
    assertEquals(operation.getMiddle(), "/documentations/");
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("documentation").asText(), "Some documentation text");
    // No attribution node for unattributed docs
    assertNull(operation.getRight().get("attribution"));
  }

  @Test
  public void testRemoveDocumentationSpecific() throws URISyntaxException {
    Urn sourceUrn = Urn.createFromString(TEST_SOURCE_URN);
    builder.removeDocumentation(sourceUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/documentations/"));
    // Path should contain the encoded source (not wildcard)
    assertTrue(!operation.getMiddle().endsWith("/*"));
    assertTrue(operation.getMiddle().length() > "/documentations/".length());
    assertNull(operation.getRight());
  }

  @Test
  public void testRemoveDocumentationAll() throws URISyntaxException {
    builder.removeDocumentation();
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    // Remove all: plain path at field level (no wildcard)
    assertEquals(operation.getMiddle(), "/documentations");
    assertNull(operation.getRight());
  }

  @Test
  public void testBuildPlainAdd() throws Exception {
    MetadataChangeProposal mcp = builder.addDocumentation("text").build();

    assertEquals(mcp.getAspect().getContentType(), "application/json-patch+json");

    byte[] bytes = mcp.getAspect().getValue().copyBytes();
    JsonNode payload = new ObjectMapper().readTree(bytes);

    // Plain patch: a JSON array, no arrayPrimaryKeys envelope
    assertTrue(payload.isArray(), "add must produce a plain JSON array (no envelope)");
    assertEquals(payload.size(), 1);
    JsonNode op = payload.get(0);
    assertEquals(op.get("op").asText(), "add");
    assertEquals(op.get("path").asText(), "/documentations/");
  }
}
