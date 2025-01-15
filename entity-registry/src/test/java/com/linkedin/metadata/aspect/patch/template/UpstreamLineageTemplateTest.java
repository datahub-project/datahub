package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.utils.GenericRecordUtils.JSON;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.DataMap;
import com.linkedin.dataset.FineGrainedLineage;
import com.linkedin.dataset.FineGrainedLineageDownstreamType;
import com.linkedin.dataset.FineGrainedLineageUpstreamType;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.aspect.patch.template.dataset.UpstreamLineageTemplate;
import com.linkedin.metadata.utils.GenericRecordUtils;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import jakarta.json.JsonValue;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.testng.annotations.Test;

public class UpstreamLineageTemplateTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testPatchUpstream() throws Exception {
    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);

    jsonPatchBuilder.add(
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c1)//urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)",
        fineGrainedLineageNode.build());

    // Initial population test
    UpstreamLineage result =
        upstreamLineageTemplate.applyPatch(upstreamLineage, jsonPatchBuilder.build());
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
    assertEquals(result4.getFineGrainedLineages().get(3), fineGrainedLineage4);

    JsonPatchBuilder patchOperations5 = Json.createPatchBuilder();
    String urn4 =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,test-bucket/hive/folder_1/folder_2/my_dataset,DEV),c2)";
    UrnArray downstreamUrns5 = new UrnArray();
    downstreamUrns5.add(Urn.createFromString(urn4));
    patchOperations5.add(
        "/fineGrainedLineages/TRANSFORM/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,test-bucket~1hive~1folder_1~1folder_2~1my_dataset,DEV),c2)/urn:li:query:anotherQuery/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_1,PROD),c2)",
        finegrainedLineageNode5.build());
    JsonPatch jsonPatch5 = patchOperations5.build();
    UpstreamLineage result5 = upstreamLineageTemplate.applyPatch(result4, jsonPatch5);
    // Hack because Jackson parses values to doubles instead of floats
    DataMap dataMap5 = new DataMap();
    dataMap5.put("confidenceScore", 1.0);
    FineGrainedLineage fineGrainedLineage5 = new FineGrainedLineage(dataMap5);
    fineGrainedLineage5.setUpstreams(upstreamUrns3);
    fineGrainedLineage5.setDownstreams(downstreamUrns5);
    fineGrainedLineage5.setTransformOperation("TRANSFORM");
    fineGrainedLineage5.setUpstreamType(FineGrainedLineageUpstreamType.FIELD_SET);
    fineGrainedLineage5.setDownstreamType(FineGrainedLineageDownstreamType.FIELD);
    fineGrainedLineage5.setQuery(UrnUtils.getUrn("urn:li:query:anotherQuery"));
    // New entry in array because of new transformation type
    assertEquals(result5.getFineGrainedLineages().get(4), fineGrainedLineage5);

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
    UpstreamLineage finalResult = upstreamLineageTemplate.applyPatch(result4, removePatch);

    assertEquals(finalResult, upstreamLineageTemplate.getDefault());
  }

  @Test
  public void testLargePatchStandard() throws Exception {
    // Load patch operations from fixture
    String patchStr =
        OBJECT_MAPPER
            .readTree(
                new GzipCompressorInputStream(
                    this.getClass()
                        .getResourceAsStream("/patch/large_upstream_lineage_mcp.json.gz")))
            .get("aspect")
            .get("com.linkedin.pegasus2avro.mxe.GenericAspect")
            .get("value")
            .asText();

    JsonPatchBuilder patchBuilder =
        Json.createPatchBuilder(Json.createReader(new StringReader(patchStr)).readArray());

    // Overall the patch is a no-op, adding change to assert difference after application
    patchBuilder.remove(
        "/upstreams/urn:li:dataset:(urn:li:dataPlatform:snowflake,road_curated_nrt.db_3134_dbo.lineitem,PROD)");

    JsonPatch jsonPatch = patchBuilder.build();
    assertEquals(jsonPatch.toJsonArray().size(), 7491);

    // Load existing aspect
    String aspectStr =
        OBJECT_MAPPER
            .readTree(
                new GzipCompressorInputStream(
                    this.getClass()
                        .getResourceAsStream("/patch/large_upstream_lineage_aspect.json.gz")))
            .get("select")
            .get(0)
            .get("metadata")
            .asText();
    UpstreamLineage upstreamLineage =
        GenericRecordUtils.deserializeAspect(
            ByteString.copyString(aspectStr, StandardCharsets.UTF_8), JSON, UpstreamLineage.class);
    assertEquals(upstreamLineage.getUpstreams().size(), 188);
    assertEquals(upstreamLineage.getFineGrainedLineages().size(), 607);

    // Apply patch standard
    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();

    long start = System.currentTimeMillis();
    UpstreamLineage result = upstreamLineageTemplate.applyPatch(upstreamLineage, jsonPatch);
    long end = System.currentTimeMillis();
    assertTrue(
        end - start < 20000,
        String.format("Expected less then 20 seconds patch actual %s ms", end - start));

    assertEquals(result.getUpstreams().size(), 187, "Expected 1 less upstream");
    assertEquals(result.getFineGrainedLineages().size(), 607);
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

    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);

    jsonPatchBuilder.add(lineagePath, fineGrainedLineageNode.build());

    // Initial population test
    UpstreamLineage result =
        upstreamLineageTemplate.applyPatch(upstreamLineage, jsonPatchBuilder.build());

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

    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);

    jsonPatchBuilder.add(lineagePath, fineGrainedLineageNode.build());

    // Initial population test
    UpstreamLineage result =
        upstreamLineageTemplate.applyPatch(upstreamLineage, jsonPatchBuilder.build());
    assertEquals(
        result.getFineGrainedLineages().get(0).getUpstreams().get(0).toString(),
        unescapedUpstreamUrn);
  }

  @Test
  public void testPatchRemoveWithFields() throws JsonProcessingException {

    String downstreamUrn =
        "/fineGrainedLineages/CREATE/urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:s3,~1tmp~1test.parquet,PROD),c1)";
    String upstreamUrn =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c1)";
    String upstreamUrn2 =
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:bigquery,upstream_table_2,PROD),c2)";

    String lineagePath1 = downstreamUrn + "/NONE/" + upstreamUrn;
    String lineagePath2 = downstreamUrn + "/NONE/" + upstreamUrn2;

    UpstreamLineageTemplate upstreamLineageTemplate = new UpstreamLineageTemplate();
    UpstreamLineage upstreamLineage = upstreamLineageTemplate.getDefault();
    JsonPatchBuilder jsonPatchBuilder = Json.createPatchBuilder();

    JsonObjectBuilder fineGrainedLineageNode = Json.createObjectBuilder();
    JsonValue upstreamConfidenceScore = Json.createValue(1.0f);
    fineGrainedLineageNode.add("confidenceScore", upstreamConfidenceScore);

    jsonPatchBuilder.add(lineagePath1, fineGrainedLineageNode.build());
    jsonPatchBuilder.add(lineagePath2, fineGrainedLineageNode.build());

    // Initial population test
    UpstreamLineage result =
        upstreamLineageTemplate.applyPatch(upstreamLineage, jsonPatchBuilder.build());
    assertEquals(
        result.getFineGrainedLineages().get(0).getUpstreams().get(0).toString(), upstreamUrn);
    assertEquals(
        result.getFineGrainedLineages().get(0).getUpstreams().get(1).toString(), upstreamUrn2);

    assertEquals(result.getFineGrainedLineages().get(0).getUpstreams().size(), 2);
  }
}
