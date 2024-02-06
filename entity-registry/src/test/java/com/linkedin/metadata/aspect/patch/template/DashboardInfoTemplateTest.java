package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.jsonpointer.JsonPointer;
import com.github.fge.jsonpatch.AddOperation;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchOperation;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.metadata.aspect.patch.template.dashboard.DashboardInfoTemplate;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardInfoTemplateTest {

  @Test
  public void testDashboardInfoTemplate() throws Exception {
    DashboardInfoTemplate dashboardInfoTemplate = new DashboardInfoTemplate();
    DashboardInfo dashboardInfo = dashboardInfoTemplate.getDefault();
    List<JsonPatchOperation> patchOperations = new ArrayList<>();
    ObjectNode edgeNode = instance.objectNode();
    edgeNode.put(
        "destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
    JsonPatchOperation operation =
        new AddOperation(
            new JsonPointer(
                "/datasetEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
            edgeNode);
    patchOperations.add(operation);
    JsonPatch patch = new JsonPatch(patchOperations);
    DashboardInfo result = dashboardInfoTemplate.applyPatch(dashboardInfo, patch);

    Assert.assertEquals(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        result.getDatasetEdges().get(0).getDestinationUrn());
  }
}
