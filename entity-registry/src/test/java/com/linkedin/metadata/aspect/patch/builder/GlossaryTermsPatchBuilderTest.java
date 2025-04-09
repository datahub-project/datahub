package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GlossaryTermsPatchBuilderTest {

  private TestableGlossaryTermsPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_GLOSSARY_TERM_URN =
      "urn:li:glossaryTerm:TestTerm";

  // Test helper class to expose protected method
  private static class TestableGlossaryTermsPatchBuilder
      extends GlossaryTermsPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableGlossaryTermsPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    GlossaryTermUrn termUrn = GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN);

    builder.addTerm(termUrn, "Test context");

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
  public void testAddTermWithContext() throws URISyntaxException {
    GlossaryTermUrn termUrn = GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN);
    String context = "Test context";
    builder.addTerm(termUrn, context);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/glossaryTerms/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("urn").asText(), termUrn.toString());
    assertEquals(operation.getRight().get("context").asText(), context);
  }

  @Test
  public void testAddTermWithoutContext() throws URISyntaxException {
    GlossaryTermUrn termUrn = GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN);
    builder.addTerm(termUrn, null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/glossaryTerms/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("urn").asText(), termUrn.toString());
    assertNull(operation.getRight().get("context"));
  }

  @Test
  public void testRemoveTerm() throws URISyntaxException {
    GlossaryTermUrn termUrn = GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN);
    builder.removeTerm(termUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/glossaryTerms/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testMultipleOperations() throws URISyntaxException {
    GlossaryTermUrn termUrn1 = GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN);
    GlossaryTermUrn termUrn2 = GlossaryTermUrn.createFromString("urn:li:glossaryTerm:AnotherTerm");

    builder.addTerm(termUrn1, "Context 1")
        .addTerm(termUrn2, null)
        .removeTerm(termUrn1);

    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 3);
  }

  @Test
  public void testGetEntityTypeWithoutUrnThrowsException() {
    TestableGlossaryTermsPatchBuilder builderWithoutUrn = new TestableGlossaryTermsPatchBuilder();
    GlossaryTermUrn termUrn;
    try {
      termUrn = GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_URN);
      builderWithoutUrn.addTerm(termUrn, null);

      assertThrows(IllegalStateException.class, builderWithoutUrn::build);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}