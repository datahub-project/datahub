package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlobalTagsPatchBuilderTest {

  private TestableGlobalTagsPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_TAG_URN = "urn:li:tag:Test/Tag";

  // Test helper class to expose protected method
  private static class TestableGlobalTagsPatchBuilder extends GlobalTagsPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableGlobalTagsPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);

    builder.addTag(tagUrn, "Test context");

    // First call build()
    builder.build();

    // Then verify we can still access pathValues and they're correct
    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    // Verify we can call build() again without issues
    builder.build();

    // And verify pathValues are still accessible and correct
    pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);
  }

  @Test
  public void testAddTagWithContext() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    String context = "Test context";
    builder.addTag(tagUrn, context);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/tags/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("urn").asText(), tagUrn.toString());
    assertEquals(operation.getRight().get("context").asText(), context);
  }

  @Test
  public void testAddTagWithoutContext() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    builder.addTag(tagUrn, null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/tags/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("urn").asText(), tagUrn.toString());
    assertNull(operation.getRight().get("context"));
  }

  @Test
  public void testRemoveTag() throws URISyntaxException {
    TagUrn tagUrn = TagUrn.createFromString(TEST_TAG_URN);
    builder.removeTag(tagUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/tags/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testMultipleOperations() throws URISyntaxException {
    TagUrn tagUrn1 = TagUrn.createFromString(TEST_TAG_URN);
    TagUrn tagUrn2 = TagUrn.createFromString("urn:li:tag:AnotherTag");

    builder.addTag(tagUrn1, "Context 1").addTag(tagUrn2, null).removeTag(tagUrn1);

    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 3);
  }

  @Test
  public void testGetEntityTypeWithoutUrnThrowsException() {
    TestableGlobalTagsPatchBuilder builderWithoutUrn = new TestableGlobalTagsPatchBuilder();
    TagUrn tagUrn;
    try {
      tagUrn = TagUrn.createFromString(TEST_TAG_URN);
      builderWithoutUrn.addTag(tagUrn, null);

      assertThrows(IllegalStateException.class, builderWithoutUrn::build);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
