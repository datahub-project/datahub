package com.linkedin.metadata.search.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

public class PostgresSearchSystemMetadataJsonTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void buildPayloadMapsAspectNamesToSystemMetadata() throws Exception {
    String doc =
        "{\"_aspects\":{\"datasetProperties\":{\"name\":\"n\","
            + "\"_systemmetadata\":{\"runId\":\"r1\",\"version\":3}},"
            + "\"ownership\":{\"owners\":[],\"_systemmetadata\":{\"version\":1}}}}";
    String out = PostgresSearchSystemMetadataJson.buildAspectSystemMetadataPayload(doc);
    JsonNode n = MAPPER.readTree(out);
    assertTrue(n.has("datasetProperties"));
    assertTrue(n.get("datasetProperties").has("runId"));
    assertTrue(n.has("ownership"));
  }

  @Test
  public void buildPayloadReturnsNullWithoutAspects() {
    assertNull(
        PostgresSearchSystemMetadataJson.buildAspectSystemMetadataPayload(
            "{\"urn\":\"x\",\"name\":\"y\"}"));
  }

  @Test
  public void buildPayloadReturnsNullWhenNoSystemMetadataOnAspects() {
    String doc = "{\"_aspects\":{\"datasetProperties\":{\"name\":\"only\"}}}";
    assertNull(PostgresSearchSystemMetadataJson.buildAspectSystemMetadataPayload(doc));
  }

  @Test
  public void mergePreservesPerAspectSystemMetadataWhenNewDocumentOmitsIt() throws Exception {
    String oldCol =
        "{\"siblings\":{\"version\":\"1\",\"lastObserved\":1777854812641,\"schemaVersion\":1}}";
    String newDoc =
        "{\"urn\":\"urn:li:dataset:(urn:li:dataPlatform:bigquery,cypress_project.jaffle_shop.raw_customers,PROD)\","
            + "\"_aspects\":{\"siblings\":{\"siblings\":[],\"hasSiblings\":true}},"
            + "\"_entityType\":\"dataset\"}";
    String merged =
        PostgresSearchSystemMetadataJson.mergeAspectSystemMetadataForUpsert(oldCol, newDoc);
    JsonNode n = MAPPER.readTree(merged);
    assertTrue(n.has("siblings"));
    assertTrue(n.get("siblings").has("version"));
    assertTrue(n.get("siblings").get("version").asText().equals("1"));
  }

  @Test
  public void mergeReplacesWhenNewDocumentIncludesSystemMetadata() throws Exception {
    String oldCol = "{\"siblings\":{\"version\":\"0\"}}";
    String newDoc =
        "{\"_aspects\":{\"siblings\":{\"_systemmetadata\":{\"version\":\"2\",\"lastObserved\":9}}}}";
    String merged =
        PostgresSearchSystemMetadataJson.mergeAspectSystemMetadataForUpsert(oldCol, newDoc);
    JsonNode n = MAPPER.readTree(merged);
    assertTrue(n.get("siblings").get("version").asText().equals("2"));
  }

  @Test
  public void mergeDropsAspectNoLongerInDocument() throws Exception {
    String oldCol = "{\"datasetProperties\":{\"version\":1},\"siblings\":{\"version\":\"1\"}}";
    String newDoc = "{\"_aspects\":{\"siblings\":{\"hasSiblings\":true}}}";
    String merged =
        PostgresSearchSystemMetadataJson.mergeAspectSystemMetadataForUpsert(oldCol, newDoc);
    JsonNode n = MAPPER.readTree(merged);
    assertTrue(n.has("siblings"));
    assertFalse(n.has("datasetProperties"));
  }

  @Test
  public void mergeLeavesExistingColumnWhenNewDocHasNoAspectsObject() {
    String oldCol = "{\"siblings\":{\"version\":1}}";
    String newDoc = "{\"urn\":\"x\",\"name\":\"y\"}";
    assertEquals(
        oldCol,
        PostgresSearchSystemMetadataJson.mergeAspectSystemMetadataForUpsert(oldCol, newDoc));
  }
}
