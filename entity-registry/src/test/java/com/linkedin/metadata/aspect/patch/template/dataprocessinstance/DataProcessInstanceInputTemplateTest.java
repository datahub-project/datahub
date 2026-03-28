// ABOUTME: Unit tests for DataProcessInstanceInputTemplate PATCH operations.
// ABOUTME: Verifies inputs (URN array) and inputEdges (Edge array) transformations.
package com.linkedin.metadata.aspect.patch.template.dataprocessinstance;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataprocess.DataProcessInstanceInput;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataProcessInstanceInputTemplateTest {

  @Test
  public void testGetDefault() {
    DataProcessInstanceInputTemplate template = new DataProcessInstanceInputTemplate();
    DataProcessInstanceInput defaultInput = template.getDefault();

    Assert.assertNotNull(defaultInput);
    Assert.assertNotNull(defaultInput.getInputs());
    Assert.assertTrue(defaultInput.getInputs().isEmpty());
    Assert.assertNotNull(defaultInput.getInputEdges());
    Assert.assertTrue(defaultInput.getInputEdges().isEmpty());
  }

  @Test
  public void testAddInputEdge() throws Exception {
    DataProcessInstanceInputTemplate template = new DataProcessInstanceInputTemplate();
    DataProcessInstanceInput input = template.getDefault();
    JsonPatchBuilder patchOperations = Json.createPatchBuilder();

    JsonObjectBuilder edgeNode = Json.createObjectBuilder();
    edgeNode.add("destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");

    patchOperations.add(
        "/inputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)",
        edgeNode.build());
    DataProcessInstanceInput result = template.applyPatch(input, patchOperations.build());

    Assert.assertEquals(result.getInputEdges().size(), 1);
    Assert.assertEquals(
        result.getInputEdges().get(0).getDestinationUrn(),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)"));
  }

  @Test
  public void testAddInput() throws Exception {
    DataProcessInstanceInputTemplate template = new DataProcessInstanceInputTemplate();
    DataProcessInstanceInput input = template.getDefault();
    JsonPatchBuilder patchOperations = Json.createPatchBuilder();

    patchOperations.add(
        "/inputs/urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    DataProcessInstanceInput result = template.applyPatch(input, patchOperations.build());

    Assert.assertEquals(result.getInputs().size(), 1);
    Assert.assertEquals(
        result.getInputs().get(0),
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)"));
  }

  @Test
  public void testRemoveInputEdge() throws Exception {
    DataProcessInstanceInputTemplate template = new DataProcessInstanceInputTemplate();
    DataProcessInstanceInput input = template.getDefault();

    JsonPatchBuilder addOperations = Json.createPatchBuilder();
    JsonObjectBuilder edgeNode = Json.createObjectBuilder();
    edgeNode.add("destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    addOperations.add(
        "/inputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)",
        edgeNode.build());
    DataProcessInstanceInput withEdge = template.applyPatch(input, addOperations.build());
    Assert.assertEquals(withEdge.getInputEdges().size(), 1);

    JsonPatchBuilder removeOperations = Json.createPatchBuilder();
    removeOperations.remove(
        "/inputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleDataset,PROD)");
    DataProcessInstanceInput result = template.applyPatch(withEdge, removeOperations.build());

    Assert.assertEquals(result.getInputEdges().size(), 0);
  }
}
