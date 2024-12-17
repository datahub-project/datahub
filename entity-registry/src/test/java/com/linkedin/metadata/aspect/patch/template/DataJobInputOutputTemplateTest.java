package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.utils.GenericRecordUtils.*;
import static org.testng.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.metadata.aspect.patch.template.datajob.DataJobInputOutputTemplate;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import jakarta.json.JsonValue;
import org.testng.annotations.Test;

public class DataJobInputOutputTemplateTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testPatchUpstream() throws Exception {
    DataJobInputOutputTemplate dataJobInputOutputTemplate = new DataJobInputOutputTemplate();
    DataJobInputOutput dataJobInputOutput = dataJobInputOutputTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);
    jsonPatchBuilder.add(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)//urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)",
        fineGrainedLineageNode.build());

    // Initial population test
    DataJobInputOutput result =
        dataJobInputOutputTemplate.applyPatch(dataJobInputOutput, jsonPatchBuilder.build());
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
    assertEquals(result.getFineGrainedLineages().get(0), fineGrainedLineage);

    // Test non-overwrite upstreams and correct confidence score and types w/ overwrite
    JsonObjectBuilder finegrainedLineageNode2 = Json.createObjectBuilder();
    finegrainedLineageNode2.add(
        "upstreamType", Json.createValue(FineGrainedLineageUpstreamType.FIELD_SET.name()));
    finegrainedLineageNode2.add("confidenceScore", upstreamConfidenceScore);
    finegrainedLineageNode2.add(
        "downstreamType", Json.createValue(FineGrainedLineageDownstreamType.FIELD.name()));

    JsonPatchBuilder patchOperations2 = Json.createPatchBuilder();
    patchOperations2.add(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:someQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)",
        finegrainedLineageNode2.build());

    JsonValue upstreamConfidenceScore2 = Json.createValue(0.1f);
    JsonObjectBuilder finegrainedLineageNode3 = Json.createObjectBuilder();
    finegrainedLineageNode3.add(
        "upstreamType", Json.createValue(FineGrainedLineageUpstreamType.DATASET.name()));
    finegrainedLineageNode3.add("confidenceScore", upstreamConfidenceScore2);
    finegrainedLineageNode3.add(
        "downstreamType", Json.createValue(FineGrainedLineageDownstreamType.FIELD_SET.name()));

    patchOperations2.add(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:someQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)",
        finegrainedLineageNode3.build());

    JsonPatch jsonPatch2 = patchOperations2.build();

    DataJobInputOutput result2 = dataJobInputOutputTemplate.applyPatch(result, jsonPatch2);
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
    assertEquals(result2.getFineGrainedLineages().get(1), fineGrainedLineage2);

    // Check different queries
    JsonObjectBuilder finegrainedLineageNode4 = Json.createObjectBuilder();
    finegrainedLineageNode4.add(
        "upstreamType", Json.createValue(FineGrainedLineageUpstreamType.FIELD_SET.name()));
    finegrainedLineageNode4.add("confidenceScore", upstreamConfidenceScore);
    finegrainedLineageNode4.add(
        "downstreamType", Json.createValue(FineGrainedLineageDownstreamType.FIELD.name()));

    JsonPatchBuilder patchOperations3 = Json.createPatchBuilder();
    patchOperations3.add(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)",
        finegrainedLineageNode4.build());

    JsonPatch jsonPatch3 = patchOperations3.build();
    DataJobInputOutput result3 = dataJobInputOutputTemplate.applyPatch(result2, jsonPatch3);
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
    assertEquals(result3.getFineGrainedLineages().get(2), fineGrainedLineage3);

    // Check different transform types
    JsonObjectBuilder finegrainedLineageNode5 = Json.createObjectBuilder();
    finegrainedLineageNode5.add(
        "upstreamType", Json.createValue(FineGrainedLineageUpstreamType.FIELD_SET.name()));
    finegrainedLineageNode5.add("confidenceScore", upstreamConfidenceScore);
    finegrainedLineageNode5.add(
        "downstreamType", Json.createValue(FineGrainedLineageDownstreamType.FIELD.name()));

    JsonPatchBuilder patchOperations4 = Json.createPatchBuilder();
    patchOperations4.add(
        "/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)",
        finegrainedLineageNode5.build());
    JsonPatch jsonPatch4 = patchOperations4.build();

    DataJobInputOutput result4 = dataJobInputOutputTemplate.applyPatch(result3, jsonPatch4);
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
    assertEquals(result4.getFineGrainedLineages().get(3), fineGrainedLineage4);

    // Remove
    JsonPatchBuilder removeOperations = Json.createPatchBuilder();
    removeOperations.remove(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)/NONE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)");
    removeOperations.remove(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:someQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)");
    removeOperations.remove(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)");
    removeOperations.remove(
        "/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)");

    JsonPatch removePatch = removeOperations.build();
    DataJobInputOutput finalResult = dataJobInputOutputTemplate.applyPatch(result4, removePatch);
    assertEquals(finalResult, dataJobInputOutputTemplate.getDefault());
  }

  @Test
  public void testPatchWithFieldWithForwardSlash() throws JsonProcessingException {

    String downstreamUrn =
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)";
    String unescapedUpstreamUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),slash/column)";
    String escapedUpstreamUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),slash~1column)";
    String lineagePath = downstreamUrn + "//" + escapedUpstreamUrn;

    DataJobInputOutputTemplate dataJobInputOutputTemplate = new DataJobInputOutputTemplate();
    DataJobInputOutput dataJobInputOutput = dataJobInputOutputTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);

    jsonPatchBuilder.add(lineagePath, fineGrainedLineageNode.build());

    // Initial population test
    DataJobInputOutput result =
        dataJobInputOutputTemplate.applyPatch(dataJobInputOutput, jsonPatchBuilder.build());

    assertEquals(
        result.getFineGrainedLineages().get(0).getUpstreams().get(0).toString(),
        unescapedUpstreamUrn);
  }

  @Test
  public void testPatchWithFieldWithTilde() throws JsonProcessingException {

    String downstreamUrn =
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)";
    String unescapedUpstreamUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),tilde~column)";
    String escapedUpstreamUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),tilde~0column)";
    String lineagePath = downstreamUrn + "//" + escapedUpstreamUrn;

    DataJobInputOutputTemplate dataJobInputOutputTemplate = new DataJobInputOutputTemplate();
    DataJobInputOutput dataJobInputOutput = dataJobInputOutputTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);

    jsonPatchBuilder.add(lineagePath, fineGrainedLineageNode.build());

    // Initial population test
    DataJobInputOutput result =
        dataJobInputOutputTemplate.applyPatch(dataJobInputOutput, jsonPatchBuilder.build());
    assertEquals(
        result.getFineGrainedLineages().get(0).getUpstreams().get(0).toString(),
        unescapedUpstreamUrn);
  }
}
