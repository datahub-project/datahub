// ABOUTME: Unit tests for DataProcessInstanceOutputPatchBuilder.
// ABOUTME: Verifies add/remove operations for outputs and outputEdges.
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

public class DataProcessInstanceOutputPatchBuilderTest {

  private TestableDataProcessInstanceOutputPatchBuilder builder;
  private static final String TEST_DPI_URN = "urn:li:dataProcessInstance:test-instance-123";
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";

  private static class TestableDataProcessInstanceOutputPatchBuilder
      extends DataProcessInstanceOutputPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableDataProcessInstanceOutputPatchBuilder();
    builder.urn(Urn.createFromString(TEST_DPI_URN));
  }

  @Test
  public void testAddOutput() throws URISyntaxException {
    Urn outputUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.addOutput(outputUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/outputs/"));
    assertTrue(operation.getRight().isTextual());
    assertEquals(operation.getRight().asText(), outputUrn.toString());
  }

  @Test
  public void testRemoveOutput() throws URISyntaxException {
    Urn outputUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.removeOutput(outputUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/outputs/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testAddOutputEdgeWithUrn() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.addOutputEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/outputEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testAddOutputEdgeWithEdge() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    Edge edge = new Edge();
    edge.setDestinationUrn(datasetUrn);

    builder.addOutputEdge(edge);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/outputEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testRemoveOutputEdge() throws URISyntaxException {
    Urn datasetUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.removeOutputEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/outputEdges/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testMultipleOperations() throws URISyntaxException {
    Urn outputUrn1 = Urn.createFromString(TEST_DATASET_URN);
    Urn outputUrn2 =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:mysql,OtherTable,PROD)");

    builder.addOutput(outputUrn1).addOutputEdge(outputUrn2);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2);
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    Urn outputUrn = Urn.createFromString(TEST_DATASET_URN);
    builder.addOutput(outputUrn);

    builder.build();
    List<ImmutableTriple<String, String, JsonNode>> pathValues1 = builder.getTestPathValues();
    assertEquals(pathValues1.size(), 1);

    builder.build();
    List<ImmutableTriple<String, String, JsonNode>> pathValues2 = builder.getTestPathValues();
    assertEquals(pathValues2.size(), 1);
  }
}
