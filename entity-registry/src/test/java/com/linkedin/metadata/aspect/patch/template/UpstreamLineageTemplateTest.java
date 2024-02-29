package com.linkedin.metadata.aspect.patch.template;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;

import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jackson.jsonpointer.JsonPointer;
import com.github.fge.jsonpatch.AddOperation;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchOperation;
import com.github.fge.jsonpatch.RemoveOperation;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.patch.template.dataset.UpstreamLineageTemplate;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UpstreamLineageTemplateTest {
  @Test
  public void testPatchUpstream() throws Exception {
    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    List<JsonPatchOperation> patchOperations = new ArrayList<>();
    ObjectNode fineGrainedLineageNode = instance.objectNode();
    NumericNode upstreamConfidenceScore = instance.numberNode(1.0f);
    fineGrainedLineageNode.set("confidenceScore", upstreamConfidenceScore);
    JsonPatchOperation operation =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)//urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)"),
            fineGrainedLineageNode);
    patchOperations.add(operation);
    JsonPatch jsonPatch = new JsonPatch(patchOperations);

    // Initial population test
    UpstreamLineage result = upstreamLineageTemplate.applyPatch(upstreamLineage, jsonPatch);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap = new DataMap();
    dataMap.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage = new FineGrainedLineage(dataMap);
    UrnArray urns = new UrnArray();
    Urn urn1 =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)");
    urns.add(urn1);
    UrnArray upstreams = new UrnArray();
    Urn upstreamUrn =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)");
    upstreams.add(upstreamUrn);
    fineGrainedLineage.setDownstreams(urns);
    fineGrainedLineage.setUpstreams(upstreams);
    fineGrainedLineage.setTransformOperation("CREATE");
    fineGrainedLineage.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    Assert.assertEquals(result.getFineGrainedLineages().get(0), fineGrainedLineage);

    // Test non-overwrite upstreams and correct confidence score and types w/ overwrite
    ObjectNode finegrainedLineageNode2 = instance.objectNode();
    finegrainedLineageNode2.set(
        "upstreamType", instance.textNode(FineGrainedLineageUpstreamType.FIELD_SET.name()));
    finegrainedLineageNode2.set("confidenceScore", upstreamConfidenceScore);
    finegrainedLineageNode2.set(
        "downstreamType", instance.textNode(FineGrainedLineageDownstreamType.FIELD.name()));
    JsonPatchOperation operation2 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:someQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            finegrainedLineageNode2);
    NumericNode upstreamConfidenceScore2 = instance.numberNode(0.1f);
    ObjectNode finegrainedLineageNode3 = instance.objectNode();
    finegrainedLineageNode3.set(
        "upstreamType", instance.textNode(FineGrainedLineageUpstreamType.DATASET.name()));
    finegrainedLineageNode3.set("confidenceScore", upstreamConfidenceScore2);
    finegrainedLineageNode3.set(
        "downstreamType", instance.textNode(FineGrainedLineageDownstreamType.FIELD_SET.name()));
    JsonPatchOperation operation3 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:someQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            finegrainedLineageNode3);
    List<JsonPatchOperation> patchOperations2 = new ArrayList<>();
    patchOperations2.add(operation2);
    patchOperations2.add(operation3);
    JsonPatch jsonPatch2 = new JsonPatch(patchOperations2);
    UpstreamLineage result2 = upstreamLineageTemplate.applyPatch(result, jsonPatch2);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap2 = new DataMap();
    dataMap2.put("confidenceScore", 0.1);
    FineGrainedLineage fineGrainedLineage2 = new FineGrainedLineage(dataMap2);
    UrnArray urns2 = new UrnArray();
    Urn urn2 =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)");
    urns2.add(urn2);
    Urn downstreamUrn2 =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)");
    UrnArray downstreams2 = new UrnArray();
    downstreams2.add(downstreamUrn2);
    fineGrainedLineage2.setUpstreams(urns2);
    fineGrainedLineage2.setDownstreams(downstreams2);
    fineGrainedLineage2.setTransformOperation("CREATE");
    fineGrainedLineage2.setUpstreamType(FineGrainedLineageUpstreamType.DATASET);
    fineGrainedLineage2.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    fineGrainedLineage2.setQuery(UrnUtils.getUrn("urn:li:query:someQuery"));
    Assert.assertEquals(result2.getFineGrainedLineages().get(1), fineGrainedLineage2);

    // Check different queries
    ObjectNode finegrainedLineageNode4 = instance.objectNode();
    finegrainedLineageNode4.set(
        "upstreamType", instance.textNode(FineGrainedLineageUpstreamType.FIELD_SET.name()));
    finegrainedLineageNode4.set("confidenceScore", upstreamConfidenceScore);
    finegrainedLineageNode4.set(
        "downstreamType", instance.textNode(FineGrainedLineageDownstreamType.FIELD.name()));
    JsonPatchOperation operation4 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            finegrainedLineageNode4);
    List<JsonPatchOperation> patchOperations3 = new ArrayList<>();
    patchOperations3.add(operation4);
    JsonPatch jsonPatch3 = new JsonPatch(patchOperations3);
    UpstreamLineage result3 = upstreamLineageTemplate.applyPatch(result2, jsonPatch3);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap3 = new DataMap();
    dataMap3.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage3 = new FineGrainedLineage(dataMap3);
    UrnArray urns3 = new UrnArray();
    Urn urn3 =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)");
    urns3.add(urn3);

    Urn upstreamUrn3 =
        UrnUtils.getUrn(
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)");
    UrnArray upstreamUrns3 = new UrnArray();
    upstreamUrns3.add(upstreamUrn3);
    fineGrainedLineage3.setDownstreams(urns3);
    fineGrainedLineage3.setUpstreams(upstreamUrns3);
    fineGrainedLineage3.setTransformOperation("CREATE");
    fineGrainedLineage3.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage3.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    fineGrainedLineage3.setQuery(UrnUtils.getUrn("urn:li:query:anotherQuery"));
    // Splits into two for different types
    Assert.assertEquals(result3.getFineGrainedLineages().get(2), fineGrainedLineage3);

    // Check different transform types
    ObjectNode finegrainedLineageNode5 = instance.objectNode();
    finegrainedLineageNode5.set(
        "upstreamType", instance.textNode(FineGrainedLineageUpstreamType.FIELD_SET.name()));
    finegrainedLineageNode5.set("confidenceScore", upstreamConfidenceScore);
    finegrainedLineageNode5.set(
        "downstreamType", instance.textNode(FineGrainedLineageDownstreamType.FIELD.name()));
    JsonPatchOperation operation5 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            finegrainedLineageNode5);
    List<JsonPatchOperation> patchOperations4 = new ArrayList<>();
    patchOperations4.add(operation5);
    JsonPatch jsonPatch4 = new JsonPatch(patchOperations4);
    UpstreamLineage result4 = upstreamLineageTemplate.applyPatch(result3, jsonPatch4);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap4 = new DataMap();
    dataMap4.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage4 = new FineGrainedLineage(dataMap4);
    fineGrainedLineage4.setUpstreams(upstreamUrns3);
    fineGrainedLineage4.setDownstreams(urns3);
    fineGrainedLineage4.setTransformOperation("TRANSFORM");
    fineGrainedLineage4.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage4.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    fineGrainedLineage4.setQuery(UrnUtils.getUrn("urn:li:query:anotherQuery"));
    // New entry in array because of new transformation type
    Assert.assertEquals(result4.getFineGrainedLineages().get(3), fineGrainedLineage4);

    // Remove
    JsonPatchOperation removeOperation =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)/NONE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)"));
    JsonPatchOperation removeOperation2 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:someQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"));
    JsonPatchOperation removeOperation3 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"));
    JsonPatchOperation removeOperation4 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"));

    List<JsonPatchOperation> removeOperations = new ArrayList<>();
    removeOperations.add(removeOperation);
    removeOperations.add(removeOperation2);
    removeOperations.add(removeOperation3);
    removeOperations.add(removeOperation4);
    JsonPatch removePatch = new JsonPatch(removeOperations);
    UpstreamLineage finalResult = upstreamLineageTemplate.applyPatch(result4, removePatch);
    Assert.assertEquals(finalResult, upstreamLineageTemplate.getDefault());
  }
}
