package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
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
    // Trailing slash indicates unattributed documentation
    assertEquals(operation.getMiddle(), "/documentations/");
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("documentation").asText(), "Some documentation text");
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
    // Remove all: no trailing slash
    assertEquals(operation.getMiddle(), "/documentations");
    assertNull(operation.getRight());
  }
}
