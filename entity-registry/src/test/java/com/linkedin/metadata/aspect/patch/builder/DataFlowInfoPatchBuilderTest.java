package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.TimeStamp;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataFlowInfoPatchBuilderTest {

  private TestableDataFlowInfoPatchBuilder builder;
  private static final String TEST_URN = "urn:li:dataFlow:(test,flow1,PROD)";

  // Test helper class to expose protected method
  private static class TestableDataFlowInfoPatchBuilder extends DataFlowInfoPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableDataFlowInfoPatchBuilder();
    builder.urn(Urn.createFromString(TEST_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    String testName = "testFlow";
    String testDescription = "Test description";

    builder.setName(testName).setDescription(testDescription).addCustomProperty("key1", "value1");

    // First call build()
    builder.build();

    // Then verify we can still access pathValues and they're correct
    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 3);

    // Verify the operations are still intact
    assertEquals(pathValues.get(0).getLeft(), "add");
    assertEquals(pathValues.get(0).getMiddle(), "/name");
    assertEquals(pathValues.get(0).getRight().asText(), testName);

    assertEquals(pathValues.get(1).getLeft(), "add");
    assertEquals(pathValues.get(1).getMiddle(), "/description");
    assertEquals(pathValues.get(1).getRight().asText(), testDescription);

    assertEquals(pathValues.get(2).getLeft(), "add");
    assertTrue(pathValues.get(2).getMiddle().startsWith("/customProperties/"));
    assertEquals(pathValues.get(2).getRight().asText(), "value1");

    // Verify we can call build() again without issues
    builder.build();

    // And verify pathValues are still accessible and correct
    pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 3);
  }

  @Test
  public void testSetName() {
    String testName = "testFlow";
    builder.setName(testName);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/name");
    assertEquals(operation.getRight().asText(), testName);
  }

  @Test
  public void testSetDescription() {
    String testDescription = "Test description";
    builder.setDescription(testDescription);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/description");
    assertEquals(operation.getRight().asText(), testDescription);
  }

  @Test
  public void testSetDescriptionNull() {
    builder.setDescription(null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertEquals(operation.getMiddle(), "/description");
    assertNull(operation.getRight());
  }

  @Test
  public void testSetProject() {
    String testProject = "testProject";
    builder.setProject(testProject);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/project");
    assertEquals(operation.getRight().asText(), testProject);
  }

  @Test
  public void testSetProjectNull() {
    builder.setProject(null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertEquals(operation.getMiddle(), "/project");
    assertNull(operation.getRight());
  }

  @Test
  public void testSetCreated() throws URISyntaxException {
    long time = System.currentTimeMillis();
    String actor = "urn:li:corpuser:testUser";
    TimeStamp created = new TimeStamp();
    created.setTime(time);
    created.setActor(Urn.createFromString(actor));

    builder.setCreated(created);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/created");
    JsonNode createdNode = operation.getRight();
    assertTrue(createdNode.isObject());
    assertEquals(createdNode.get("time").asLong(), time);
    assertEquals(createdNode.get("actor").asText(), actor);
  }

  @Test
  public void testSetCreatedNull() {
    builder.setCreated(null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertEquals(operation.getMiddle(), "/created");
    assertNull(operation.getRight());
  }

  @Test
  public void testSetLastModified() throws URISyntaxException {
    long time = System.currentTimeMillis();
    String actor = "urn:li:corpuser:testUser";
    TimeStamp lastModified = new TimeStamp();
    lastModified.setTime(time);
    lastModified.setActor(Urn.createFromString(actor));

    builder.setLastModified(lastModified);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/lastModified");
    JsonNode lastModifiedNode = operation.getRight();
    assertTrue(lastModifiedNode.isObject());
    assertEquals(lastModifiedNode.get("time").asLong(), time);
    assertEquals(lastModifiedNode.get("actor").asText(), actor);
  }

  @Test
  public void testSetLastModifiedNull() {
    builder.setLastModified(null);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertEquals(operation.getMiddle(), "/lastModified");
    assertNull(operation.getRight());
  }

  @Test
  public void testAddCustomProperties() {
    builder.addCustomProperty("key1", "value1").addCustomProperty("key2", "value2");
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2);

    pathValues.forEach(
        operation -> {
          assertEquals(operation.getLeft(), "add");
          assertTrue(operation.getMiddle().startsWith("/customProperties/"));
          assertTrue(operation.getRight().isTextual());
        });
  }

  @Test
  public void testRemoveCustomProperty() {
    builder.removeCustomProperty("key1");
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertEquals(operation.getMiddle(), "/customProperties/key1");
    assertNull(operation.getRight());
  }

  @Test
  public void testSetCustomProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    properties.put("key2", "value2");

    builder.setCustomProperties(properties);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/customProperties");
    assertTrue(operation.getRight().isObject());
  }
}
