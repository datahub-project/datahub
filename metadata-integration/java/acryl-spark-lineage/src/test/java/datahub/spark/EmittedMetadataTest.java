package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Fast, Docker-free unit test for {@link EmittedMetadata}'s query helpers. Verifies the parsing of
 * the Spark file-emitter format (a JSON array of MCPs whose {@code aspect.value} is a stringified
 * JSON document) so feature smoke tests can assert on lineage edges and aspect content instead of
 * raw substring matching.
 */
class EmittedMetadataTest {

  // Mirrors the real file-emitter output: the dataset URNs live inside the dataJobInputOutput
  // edges, and aspect values are double-serialized (a JSON string within the MCP).
  private static final String SAMPLE =
      "["
          + "{\"entityType\":\"dataJob\",\"aspectName\":\"dataJobInputOutput\","
          + "\"aspect\":{\"contentType\":\"application/json\",\"value\":\"{"
          + "\\\"inputDatasetEdges\\\":[{\\\"destinationUrn\\\":\\\"urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse_a.orders,PROD)\\\"}],"
          + "\\\"outputDatasetEdges\\\":[{\\\"destinationUrn\\\":\\\"urn:li:dataset:(urn:li:dataPlatform:file,/tmp/out,PROD)\\\"}],"
          + "\\\"fineGrainedLineages\\\":[{\\\"confidenceScore\\\":0.5}]"
          + "}\"}},"
          + "{\"entityType\":\"dataJob\",\"aspectName\":\"dataPlatformInstance\","
          + "\"aspect\":{\"contentType\":\"application/json\",\"value\":\"{"
          + "\\\"instance\\\":\\\"urn:li:dataPlatformInstance:(urn:li:dataPlatform:spark,my_instance)\\\"}\"}}"
          + "]";

  @Test
  void parsesInputAndOutputEdges() {
    EmittedMetadata md = EmittedMetadata.parse(SAMPLE);
    assertTrue(
        md.inputDatasetUrns()
            .contains("urn:li:dataset:(urn:li:dataPlatform:postgres,warehouse_a.orders,PROD)"));
    assertTrue(
        md.outputDatasetUrns().contains("urn:li:dataset:(urn:li:dataPlatform:file,/tmp/out,PROD)"));
    assertEquals(2, md.datasetEdgeUrns().size());
  }

  @Test
  void filtersEdgesByPlatform() {
    EmittedMetadata md = EmittedMetadata.parse(SAMPLE);
    Set<String> pg = md.datasetUrnsOnPlatform("postgres");
    assertEquals(1, pg.size());
    assertTrue(pg.iterator().next().contains("warehouse_a.orders"));
    assertEquals(1, md.datasetUrnsOnPlatform("file").size());
  }

  @Test
  void exposesParsedAspectContent() {
    EmittedMetadata md = EmittedMetadata.parse(SAMPLE);
    assertTrue(md.hasAspect("dataPlatformInstance"));
    JsonNode inst = md.aspect("dataPlatformInstance").orElseThrow();
    assertEquals(
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:spark,my_instance)",
        inst.get("instance").asText());
    assertEquals(
        1, md.aspect("dataJobInputOutput").orElseThrow().get("fineGrainedLineages").size());
  }

  @Test
  void emptyOnMissing() {
    EmittedMetadata md = EmittedMetadata.parse(SAMPLE);
    assertFalse(md.aspect("nonexistent").isPresent());
    assertTrue(md.datasetUrnsOnPlatform("snowflake").isEmpty());
  }

  @Test
  void detectsEntityTypes() {
    EmittedMetadata md = EmittedMetadata.parse(SAMPLE);
    assertTrue(md.hasEntity("dataJob"));
    assertFalse(md.hasEntity("dataset"));
  }

  @Test
  void toleratesEmptyOrMalformed() {
    assertTrue(EmittedMetadata.parse("").inputDatasetUrns().isEmpty());
    assertTrue(EmittedMetadata.parse("not json").outputDatasetUrns().isEmpty());
  }
}
