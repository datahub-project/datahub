package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageDirection;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DataJobInputOutputPatchBuilderTest {

  private TestableDataJobInputOutputPatchBuilder builder;
  private static final String TEST_DATAJOB_URN =
      "urn:li:dataJob:(urn:li:dataFlow:(test,flow1,PROD),job1)";
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_DATASET_FIELD_URN =
      "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD),id)";

  // Test helper class to expose protected method
  private static class TestableDataJobInputOutputPatchBuilder
      extends DataJobInputOutputPatchBuilder {
    public List<ImmutableTriple<String, String, JsonNode>> getTestPathValues() {
      return getPathValues();
    }
  }

  @BeforeMethod
  public void setup() throws URISyntaxException {
    builder = new TestableDataJobInputOutputPatchBuilder();
    builder.urn(Urn.createFromString(TEST_DATAJOB_URN));
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws URISyntaxException {
    DataJobUrn dataJobUrn = DataJobUrn.createFromString(TEST_DATAJOB_URN);
    DatasetUrn datasetUrn = DatasetUrn.createFromString(TEST_DATASET_URN);

    builder
        .addInputDatajobEdge(dataJobUrn)
        .addInputDatasetEdge(datasetUrn)
        .addOutputDatasetEdge(datasetUrn);

    // First call build()
    builder.build();

    // Then verify we can still access pathValues and they're correct
    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 3);

    // Verify we can call build() again without issues
    builder.build();

    // And verify pathValues are still accessible and correct
    pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 3);
  }

  @Test
  public void testAddInputDatajobEdge() throws URISyntaxException {
    DataJobUrn dataJobUrn = DataJobUrn.createFromString(TEST_DATAJOB_URN);
    builder.addInputDatajobEdge(dataJobUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputDatajobEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), dataJobUrn.toString());
  }

  @Test
  public void testRemoveInputDatajobEdge() throws URISyntaxException {
    DataJobUrn dataJobUrn = DataJobUrn.createFromString(TEST_DATAJOB_URN);
    builder.removeInputDatajobEdge(dataJobUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/inputDatajobEdges/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testAddInputDatasetEdge() throws URISyntaxException {
    DatasetUrn datasetUrn = DatasetUrn.createFromString(TEST_DATASET_URN);
    builder.addInputDatasetEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputDatasetEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testRemoveInputDatasetEdge() throws URISyntaxException {
    DatasetUrn datasetUrn = DatasetUrn.createFromString(TEST_DATASET_URN);
    builder.removeInputDatasetEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/inputDatasetEdges/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testAddOutputDatasetEdge() throws URISyntaxException {
    DatasetUrn datasetUrn = DatasetUrn.createFromString(TEST_DATASET_URN);
    builder.addOutputDatasetEdge(datasetUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/outputDatasetEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testAddInputDatasetField() throws URISyntaxException {
    Urn fieldUrn = Urn.createFromString(TEST_DATASET_FIELD_URN);
    builder.addInputDatasetField(fieldUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputDatasetFields/"));
    assertTrue(operation.getRight().isTextual());
    assertEquals(operation.getRight().asText(), fieldUrn.toString());
  }

  @Test
  public void testRemoveInputDatasetField() throws URISyntaxException {
    Urn fieldUrn = Urn.createFromString(TEST_DATASET_FIELD_URN);
    builder.removeInputDatasetField(fieldUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertTrue(operation.getMiddle().startsWith("/inputDatasetFields/"));
    assertNull(operation.getRight());
  }

  @Test
  public void testAddOutputDatasetField() throws URISyntaxException {
    Urn fieldUrn = Urn.createFromString(TEST_DATASET_FIELD_URN);
    builder.addOutputDatasetField(fieldUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/outputDatasetFields/"));
    assertTrue(operation.getRight().isTextual());
    assertEquals(operation.getRight().asText(), fieldUrn.toString());
  }

  @Test
  public void testAddEdgeWithDirection() throws URISyntaxException {
    DatasetUrn datasetUrn = DatasetUrn.createFromString(TEST_DATASET_URN);
    Edge edge = new Edge();
    edge.setDestinationUrn(datasetUrn);

    builder.addEdge(edge, LineageDirection.UPSTREAM);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getTestPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertTrue(operation.getMiddle().startsWith("/inputDatasetEdges/"));
    assertTrue(operation.getRight().isObject());
    assertEquals(operation.getRight().get("destinationUrn").asText(), datasetUrn.toString());
  }

  @Test
  public void testInvalidEntityTypeThrowsException() throws URISyntaxException {
    Urn invalidUrn = Urn.createFromString("urn:li:glossaryTerm:invalid");
    Edge edge = new Edge();
    edge.setDestinationUrn(invalidUrn);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          builder.addEdge(edge, LineageDirection.UPSTREAM);
        });
  }
}
