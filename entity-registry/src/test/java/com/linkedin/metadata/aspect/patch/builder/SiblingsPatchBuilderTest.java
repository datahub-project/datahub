package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SiblingsPatchBuilderTest {

  private TestableSiblingsPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_SIBLING_URN =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,test.model,PROD)";

  // Test helper class to expose protected method
  private static class TestableSiblingsPatchBuilder extends SiblingsPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableSiblingsPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    Urn siblingUrn = Urn.createFromString(TEST_SIBLING_URN);
    builder.addSibling(siblingUrn, true);

    List<ImmutableTriple<String, String, JsonNode>> pathValuesBefore = builder.getTestPathValues();
    builder.build();
    List<ImmutableTriple<String, String, JsonNode>> pathValuesAfter = builder.getTestPathValues();

    assertEquals(pathValuesBefore, pathValuesAfter);
  }

  @Test
  public void testAddSiblingWithPrimary() throws URISyntaxException {
    Urn siblingUrn = Urn.createFromString(TEST_SIBLING_URN);
    builder.addSibling(siblingUrn, true);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2); // One for sibling, one for primary

    // Check sibling addition
    ImmutableTriple<String, String, JsonNode> siblingOperation = pathValues.get(0);
    assertEquals(siblingOperation.getLeft(), "add");
    assertTrue(siblingOperation.getMiddle().startsWith("/siblings/"));
    assertEquals(siblingOperation.getRight().asText(), siblingUrn.toString());

    // Check primary flag
    ImmutableTriple<String, String, JsonNode> primaryOperation = pathValues.get(1);
    assertEquals(primaryOperation.getLeft(), "add");
    assertEquals(primaryOperation.getMiddle(), "/primary");
    assertTrue(primaryOperation.getRight().asBoolean());
  }

  @Test
  public void testAddSiblingWithoutPrimary() throws URISyntaxException {
    Urn siblingUrn = Urn.createFromString(TEST_SIBLING_URN);
    builder.addSibling(siblingUrn, false);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1); // Only sibling, no primary flag

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/siblings/"));
    assertEquals(operation.getRight().asText(), siblingUrn.toString());
  }

  @Test
  public void testRemoveSibling() throws URISyntaxException {
    Urn siblingUrn = Urn.createFromString(TEST_SIBLING_URN);
    builder.removeSibling(siblingUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/siblings/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testMultipleOperations() throws URISyntaxException {
    Urn siblingUrn1 = Urn.createFromString(TEST_SIBLING_URN);
    Urn siblingUrn2 =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:dbt,another.model,PROD)");

    builder.addSibling(siblingUrn1, true).addSibling(siblingUrn2, false).removeSibling(siblingUrn1);

    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 4); // add sibling1 + primary, add sibling2, remove sibling1
  }

  @Test
  public void testGetEntityTypeWithoutUrnThrowsException() {
    TestableSiblingsPatchBuilder builderWithoutUrn = new TestableSiblingsPatchBuilder();
    assertThrows(IllegalStateException.class, builderWithoutUrn::build);
  }
}
