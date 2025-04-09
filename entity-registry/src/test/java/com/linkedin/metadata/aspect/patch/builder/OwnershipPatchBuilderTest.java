package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class OwnershipPatchBuilderTest {

  private TestableOwnershipPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_OWNER_URN =
      "urn:li:corpuser:test/User";

  // Test helper class to expose protected method
  private static class TestableOwnershipPatchBuilder
      extends OwnershipPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableOwnershipPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    Urn ownerUrn = CorpuserUrn.createFromString(TEST_OWNER_URN);

    builder.addOwner(ownerUrn, OwnershipType.TECHNICAL_OWNER);

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
  public void testAddOwner() throws URISyntaxException {
    Urn ownerUrn = CorpuserUrn.createFromString(TEST_OWNER_URN);
    OwnershipType ownershipType = OwnershipType.TECHNICAL_OWNER;

    builder.addOwner(ownerUrn, ownershipType);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/owners/"));
    assertTrue(operation.getMiddle().contains("/" + ownershipType.toString()));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("owner").asText(), ownerUrn.toString());
    assertEquals(operation.getRight().get("type").asText(), ownershipType.toString());
  }

  @Test
  public void testRemoveOwner() throws URISyntaxException {
    Urn ownerUrn = CorpuserUrn.createFromString(TEST_OWNER_URN);

    builder.removeOwner(ownerUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/owners/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testRemoveOwnershipType() throws URISyntaxException {
    Urn ownerUrn = CorpuserUrn.createFromString(TEST_OWNER_URN);
    OwnershipType ownershipType = OwnershipType.TECHNICAL_OWNER;

    builder.removeOwnershipType(ownerUrn, ownershipType);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/owners/"));
    assertTrue(operation.getMiddle().contains("/" + ownershipType.toString()));
    assertNull(operation.getRight());
  }

  @Test
  public void testMultipleOperations() throws URISyntaxException {
    Urn ownerUrn1 = CorpuserUrn.createFromString(TEST_OWNER_URN);
    Urn ownerUrn2 = CorpuserUrn.createFromString("urn:li:corpuser:anotherUser");

    builder.addOwner(ownerUrn1, OwnershipType.TECHNICAL_OWNER)
        .addOwner(ownerUrn1, OwnershipType.BUSINESS_OWNER)
        .addOwner(ownerUrn2, OwnershipType.DATA_STEWARD)
        .removeOwnershipType(ownerUrn1, OwnershipType.TECHNICAL_OWNER)
        .removeOwner(ownerUrn2);

    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 5);
  }

  @Test
  public void testGetEntityTypeWithoutUrnThrowsException() {
    TestableOwnershipPatchBuilder builderWithoutUrn = new TestableOwnershipPatchBuilder();
    Urn ownerUrn;
    try {
      ownerUrn = CorpuserUrn.createFromString(TEST_OWNER_URN);
      builderWithoutUrn.addOwner(ownerUrn, OwnershipType.TECHNICAL_OWNER);

      assertThrows(IllegalStateException.class, builderWithoutUrn::build);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testDifferentOwnershipTypes() throws URISyntaxException {
    Urn ownerUrn = CorpuserUrn.createFromString(TEST_OWNER_URN);

    // Test different ownership types
    builder.addOwner(ownerUrn, OwnershipType.TECHNICAL_OWNER)
        .addOwner(ownerUrn, OwnershipType.BUSINESS_OWNER)
        .addOwner(ownerUrn, OwnershipType.DATA_STEWARD);

    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 4);

    // Verify each ownership type has the correct path and value
    for (int i = 0; i < pathValues.size(); i++) {
      ImmutableTriple<String, String, JsonNode> operation = pathValues.get(i);
      assertEquals(operation.getLeft(), "add");
      assertTrue(operation.getMiddle().startsWith("/owners/"));
      assertTrue(operation.getRight().isObject());
      assertEquals(operation.getRight().get("owner").asText(), ownerUrn.toString());
    }
  }
}