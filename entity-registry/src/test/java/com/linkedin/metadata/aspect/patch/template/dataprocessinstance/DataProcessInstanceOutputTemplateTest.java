// ABOUTME: Unit tests for DataProcessInstanceOutputTemplate PATCH operations.
// ABOUTME: Verifies outputs (URN array) and outputEdges (Edge array) transformations.
package com.linkedin.metadata.aspect.patch.template.dataprocessinstance;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataprocess.DataProcessInstanceOutput;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataProcessInstanceOutputTemplateTest {

  @Test
  public void testGetDefault() {
    DataProcessInstanceOutputTemplate template = new DataProcessInstanceOutputTemplate();
    DataProcessInstanceOutput defaultOutput = template.getDefault();

    Assert.assertNotNull(defaultOutput);
    Assert.assertNotNull(defaultOutput.getOutputs());
    Assert.assertTrue(defaultOutput.getOutputs().isEmpty());
    Assert.assertNotNull(defaultOutput.getOutputEdges());
    Assert.assertTrue(defaultOutput.getOutputEdges().isEmpty());
  }

  @Test
  public void testAddOutputEdge() throws Exception {
    DataProcessInstanceOutputTemplate template = new DataProcessInstanceOutputTemplate();
    DataProcessInstanceOutput output = template.getDefault();
    JsonPatchBuilder patchOperations = Json.createPatchBuilder();

    JsonObjectBuilder edgeNode = Json.createObjectBuilder();
    edgeNode.add("destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)");

    patchOperations.add(
        "/outputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)",
        edgeNode.build());
    DataProcessInstanceOutput result = template.applyPatch(output, patchOperations.build());

    Assert.assertEquals(result.getOutputEdges().size(), 1);
    Assert.assertEquals(
        result.getOutputEdges().get(0).getDestinationUrn(),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)"));
  }

  @Test
  public void testAddOutput() throws Exception {
    DataProcessInstanceOutputTemplate template = new DataProcessInstanceOutputTemplate();
    DataProcessInstanceOutput output = template.getDefault();
    JsonPatchBuilder patchOperations = Json.createPatchBuilder();

    patchOperations.add(
        "/outputs/urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)");
    DataProcessInstanceOutput result = template.applyPatch(output, patchOperations.build());

    Assert.assertEquals(result.getOutputs().size(), 1);
    Assert.assertEquals(
        result.getOutputs().get(0),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)"));
  }

  @Test
  public void testRemoveOutputEdge() throws Exception {
    DataProcessInstanceOutputTemplate template = new DataProcessInstanceOutputTemplate();
    DataProcessInstanceOutput output = template.getDefault();

    JsonPatchBuilder addOperations = Json.createPatchBuilder();
    JsonObjectBuilder edgeNode = Json.createObjectBuilder();
    edgeNode.add("destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)");
    addOperations.add(
        "/outputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)",
        edgeNode.build());
    DataProcessInstanceOutput withEdge = template.applyPatch(output, addOperations.build());
    Assert.assertEquals(withEdge.getOutputEdges().size(), 1);

    JsonPatchBuilder removeOperations = Json.createPatchBuilder();
    removeOperations.remove(
        "/outputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,OutputDataset,PROD)");
    DataProcessInstanceOutput result = template.applyPatch(withEdge, removeOperations.build());

    Assert.assertEquals(result.getOutputEdges().size(), 0);
  }
}
