// ABOUTME: Unit tests for DataProcessInstanceInputPatchBuilder.
// ABOUTME: Verifies add/remove operations for inputs and inputEdges.
package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataProcessInstanceInputPatchBuilderTest {

  private TestableDataProcessInstanceInputPatchBuilder builder;
  private static final String TEST_DPI_URN = "urn:li:dataProcessInstance:test-instance-123";
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";

  private static class TestableDataProcessInstanceInputPatchBuilder
      extends DataProcessInstanceInputPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableDataProcessInstanceInputPatchBuilder();
    builder.urn(Urn.createFromString(TEST_DPI_URN));
  }

  @Test
  public void testAddInput() throws URISyntaxException {
    Urn inputUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.addInput(inputUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputs/"));
    assertTrue(operation.getRight().isTextual());
    assertEquals(operation.getRight().asText(), inputUrn.toString());
  }

  @Test
  public void testRemoveInput() throws URISyntaxException {
    Urn inputUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.removeInput(inputUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/inputs/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testAddInputEdgeWithUrn() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.addInputEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testAddInputEdgeWithEdge() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    Edge edge = new Edge();
    edge.setDestinationUrn(datasetUrn);

    builder.addInputEdge(edge);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testRemoveInputEdge() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.removeInputEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/inputEdges/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testMultipleOperations() throws URISyntaxException {
    Urn inputUrn1 = Urn.createFromString(TEST_DATASET_URN);
    Urn inputUrn2 =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,OtherTable,PROD)");

    builder.addInput(inputUrn1).addInputEdge(inputUrn2);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2);
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    Urn inputUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.addInput(inputUrn);

    builder.build();
    List<ImmutableTriple<String, String, JsonNode>> pathValues1 = builder.getTestPathValues();
    assertEquals(pathValues1.size(), 1);

    builder.build();
    List<ImmutableTriple<String, String, JsonNode>> pathValues2 = builder.getTestPathValues();
    assertEquals(pathValues2.size(), 1);
  }
}
