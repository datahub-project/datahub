package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.jsonpointer.JsonPointer;
import com.github.fge.jsonpatch.AddOperation;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchOperation;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.chart.ChartInfoTemplate;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ChartInfoTemplateTest {

  @Test
  public void testChartInfoTemplate() throws Exception {
    ChartInfoTemplate chartInfoTemplate = new ChartInfoTemplate();
    ChartInfo dashboardInfo = chartInfoTemplate.getDefault();
    List<JsonPatchOperation> patchOperations = new ArrayList<>();
    ObjectNode edgeNode = instance.objectNode();
    edgeNode.put(
        "destinationUrn", "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)");
    JsonPatchOperation operation =
        new AddOperation(
            new JsonPointer(
                "/inputEdges/urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
            edgeNode);
    patchOperations.add(operation);
    JsonPatch patch = new JsonPatch(patchOperations);
    ChartInfo result = chartInfoTemplate.applyPatch(dashboardInfo, patch);

    Assert.assertEquals(
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
        result.getInputEdges().get(0).getDestinationUrn());
  }
}
