package com.linkedin.metadata.models.registry.patch;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.*;

import com.fasterxml.jackson.databind.node.NumericNode;
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
import com.linkedin.metadata.models.registry.template.dataset.UpstreamLineageTemplate;
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
    NumericNode upstreamConfidenceScore = instance.numberNode(1.0f);
    JsonPatchOperation operation =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)"),
            upstreamConfidenceScore);
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
    fineGrainedLineage.setUpstreams(urns);
    fineGrainedLineage.setTransformOperation("CREATE");
    fineGrainedLineage.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    Assert.assertEquals(result.getFineGrainedLineages().get(0), fineGrainedLineage);

    // Test non-overwrite upstreams and correct confidence score
    JsonPatchOperation operation2 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            upstreamConfidenceScore);
    NumericNode upstreamConfidenceScore2 = instance.numberNode(0.1f);
    JsonPatchOperation operation3 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            upstreamConfidenceScore2);
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
    urns2.add(urn1);
    urns2.add(urn2);
    fineGrainedLineage2.setUpstreams(urns2);
    fineGrainedLineage2.setTransformOperation("CREATE");
    fineGrainedLineage2.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage2.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    Assert.assertEquals(result2.getFineGrainedLineages().get(0), fineGrainedLineage2);

    // Check different upstream types
    JsonPatchOperation operation4 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/DATASET/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)"),
            upstreamConfidenceScore);
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
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)");
    urns3.add(urn3);
    fineGrainedLineage3.setUpstreams(urns3);
    fineGrainedLineage3.setTransformOperation("CREATE");
    fineGrainedLineage3.setUpstreamType(FineGrainedLineageUpstreamType.DATASET);
    fineGrainedLineage3.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    // Splits into two for different types
    Assert.assertEquals(result3.getFineGrainedLineages().get(1), fineGrainedLineage3);

    // Check different transform types
    JsonPatchOperation operation5 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/TRANSFORM/upstreamType/DATASET/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)"),
            upstreamConfidenceScore);
    List<JsonPatchOperation> patchOperations4 = new ArrayList<>();
    patchOperations4.add(operation5);
    JsonPatch jsonPatch4 = new JsonPatch(patchOperations4);
    UpstreamLineage result4 = upstreamLineageTemplate.applyPatch(result3, jsonPatch4);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap4 = new DataMap();
    dataMap4.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage4 = new FineGrainedLineage(dataMap4);
    UrnArray urns4 = new UrnArray();
    Urn urn4 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)");
    urns4.add(urn4);
    fineGrainedLineage4.setUpstreams(urns4);
    fineGrainedLineage4.setTransformOperation("TRANSFORM");
    fineGrainedLineage4.setUpstreamType(FineGrainedLineageUpstreamType.DATASET);
    fineGrainedLineage4.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    // New entry in array because of new transformation type
    Assert.assertEquals(result4.getFineGrainedLineages().get(2), fineGrainedLineage4);

    // Remove
    JsonPatchOperation removeOperation =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)"));
    JsonPatchOperation removeOperation2 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"));
    JsonPatchOperation removeOperation3 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/DATASET/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)"));
    JsonPatchOperation removeOperation4 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/TRANSFORM/upstreamType/DATASET/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)"));

    List<JsonPatchOperation> removeOperations = new ArrayList<>();
    removeOperations.add(removeOperation);
    removeOperations.add(removeOperation2);
    removeOperations.add(removeOperation3);
    removeOperations.add(removeOperation4);
    JsonPatch removePatch = new JsonPatch(removeOperations);
    UpstreamLineage finalResult = upstreamLineageTemplate.applyPatch(result4, removePatch);
    Assert.assertEquals(upstreamLineageTemplate.getDefault(), finalResult);
  }

  @Test
  public void testPatchDownstream() throws Exception {
    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    List<JsonPatchOperation> patchOperations = new ArrayList<>();
    NumericNode downstreamConfidenceScore = instance.numberNode(1.0f);
    JsonPatchOperation operation =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)"),
            downstreamConfidenceScore);
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
    fineGrainedLineage.setDownstreams(urns);
    fineGrainedLineage.setTransformOperation("CREATE");
    fineGrainedLineage.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    fineGrainedLineage.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    Assert.assertEquals(result.getFineGrainedLineages().get(0), fineGrainedLineage);

    // Test non-overwrite downstreams and correct confidence score
    JsonPatchOperation operation2 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            downstreamConfidenceScore);
    NumericNode downstreamConfidenceScore2 = instance.numberNode(0.1f);
    JsonPatchOperation operation3 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"),
            downstreamConfidenceScore2);
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
    urns2.add(urn1);
    urns2.add(urn2);
    fineGrainedLineage2.setDownstreams(urns2);
    fineGrainedLineage2.setTransformOperation("CREATE");
    fineGrainedLineage2.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    fineGrainedLineage2.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    Assert.assertEquals(result2.getFineGrainedLineages().get(0), fineGrainedLineage2);

    // Check different downstream types
    JsonPatchOperation operation4 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)"),
            downstreamConfidenceScore);
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
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)");
    urns3.add(urn3);
    fineGrainedLineage3.setDownstreams(urns3);
    fineGrainedLineage3.setTransformOperation("CREATE");
    fineGrainedLineage3.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    fineGrainedLineage3.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    // Splits into two for different types
    Assert.assertEquals(result3.getFineGrainedLineages().get(1), fineGrainedLineage3);

    // Check different transform types
    JsonPatchOperation operation5 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/TRANSFORM/downstreamType/FIELD/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)"),
            downstreamConfidenceScore);
    List<JsonPatchOperation> patchOperations4 = new ArrayList<>();
    patchOperations4.add(operation5);
    JsonPatch jsonPatch4 = new JsonPatch(patchOperations4);
    UpstreamLineage result4 = upstreamLineageTemplate.applyPatch(result3, jsonPatch4);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap4 = new DataMap();
    dataMap4.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage4 = new FineGrainedLineage(dataMap4);
    UrnArray urns4 = new UrnArray();
    Urn urn4 =
        UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)");
    urns4.add(urn4);
    fineGrainedLineage4.setDownstreams(urns4);
    fineGrainedLineage4.setTransformOperation("TRANSFORM");
    fineGrainedLineage4.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    fineGrainedLineage4.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    // New entry in array because of new transformation type
    Assert.assertEquals(result4.getFineGrainedLineages().get(2), fineGrainedLineage4);

    // Remove
    JsonPatchOperation removeOperation =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)"));
    JsonPatchOperation removeOperation2 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)"));
    JsonPatchOperation removeOperation3 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD)"));
    JsonPatchOperation removeOperation4 =
        new RemoveOperation(
            new JsonPointer(
                "/fineGrainedLineages/TRANSFORM/downstreamType/FIELD/urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD)"));

    List<JsonPatchOperation> removeOperations = new ArrayList<>();
    removeOperations.add(removeOperation);
    removeOperations.add(removeOperation2);
    removeOperations.add(removeOperation3);
    removeOperations.add(removeOperation4);
    JsonPatch removePatch = new JsonPatch(removeOperations);
    UpstreamLineage finalResult = upstreamLineageTemplate.applyPatch(result4, removePatch);
    Assert.assertEquals(upstreamLineageTemplate.getDefault(), finalResult);
  }

  @Test
  public void testUpAndDown() throws Exception {
    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    List<JsonPatchOperation> patchOperations = new ArrayList<>();
    NumericNode downstreamConfidenceScore = instance.numberNode(1.0f);
    JsonPatchOperation operation =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/downstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)"),
            downstreamConfidenceScore);
    patchOperations.add(operation);
    NumericNode upstreamConfidenceScore = instance.numberNode(1.0f);
    JsonPatchOperation operation2 =
        new AddOperation(
            new JsonPointer(
                "/fineGrainedLineages/CREATE/upstreamType/FIELD_SET/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)"),
            upstreamConfidenceScore);
    patchOperations.add(operation2);
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
    fineGrainedLineage.setTransformOperation("CREATE");
    fineGrainedLineage.setUpstreams(urns);
    fineGrainedLineage.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    fineGrainedLineage.setDownstreams(urns);

    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap2 = new DataMap();
    dataMap2.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage2 = new FineGrainedLineage(dataMap2);
    fineGrainedLineage2.setTransformOperation("CREATE");
    fineGrainedLineage2.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage2.setDownstreamType(FineGrainedLineageDownstreamType.FIELD_SET);
    fineGrainedLineage2.setDownstreams(urns);

    Assert.assertEquals(result.getFineGrainedLineages().get(1), fineGrainedLineage2);
  }
}
