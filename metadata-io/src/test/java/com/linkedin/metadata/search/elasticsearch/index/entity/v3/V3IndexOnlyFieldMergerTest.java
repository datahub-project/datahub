package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.mxe.SystemMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class V3IndexOnlyFieldMergerTest {

  @Test
  public void testMergePreservesRunIdRingAndOperationalFields() {
    V3IndexOnlyFieldMerger merger = new V3IndexOnlyFieldMerger(() -> 999L);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    List<String> existingRunIds =
        new ArrayList<>(
            IntStream.range(0, 24).mapToObj(i -> "old-" + i).collect(Collectors.toList()));
    SystemMetadata latestMetadata =
        new SystemMetadata().setRunId("stored-run").setLastRunId("current-run");

    merger.merge(
        document,
        Optional.of(Map.of("runId", existingRunIds, "systemCreated", 123L)),
        List.of("current-run", "new-run"),
        List.of(latestMetadata));

    JsonNode runIds = document.get("runId");
    assertEquals(runIds.size(), V3IndexOnlyFieldMerger.MAX_RUN_IDS_INDEXED);
    assertEquals(runIds.get(0).asText(), "old-2");
    assertEquals(runIds.get(22).asText(), "stored-run");
    assertEquals(runIds.get(23).asText(), "current-run");
    assertEquals(runIds.get(24).asText(), "new-run");
    assertEquals(document.get("systemCreated").asLong(), 123L);
    assertEquals(document.get("systemIndexModified").asLong(), 999L);
  }

  @Test
  public void testExistingSystemCreatedWinsOverDocument() {
    V3IndexOnlyFieldMerger merger = new V3IndexOnlyFieldMerger(() -> 999L);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put("systemCreated", 456L);

    merger.merge(document, Optional.of(Map.of("systemCreated", 123L)), List.of(), List.of());

    assertEquals(document.get("systemCreated").asLong(), 123L);
  }

  @Test
  public void testDocumentSystemCreatedKeptWhenExistingSourceMissing() {
    V3IndexOnlyFieldMerger merger = new V3IndexOnlyFieldMerger(() -> 999L);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.put("systemCreated", 456L);

    merger.merge(document, Optional.empty(), List.of(), List.of());

    assertEquals(document.get("systemCreated").asLong(), 456L);
  }

  @Test
  public void testStampIndexModifiedLeavesRunIdsUntouched() {
    V3IndexOnlyFieldMerger merger = new V3IndexOnlyFieldMerger(() -> 999L);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.putArray("runId").add("kept");

    merger.stampIndexModified(document);

    assertEquals(document.get("systemIndexModified").asLong(), 999L);
    assertEquals(document.get("runId").get(0).asText(), "kept");
  }

  @Test
  public void testMergeRemovesEmptyRunIdField() {
    V3IndexOnlyFieldMerger merger = new V3IndexOnlyFieldMerger(() -> 999L);
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    document.putArray("runId").add("stale");

    merger.merge(document, Optional.empty(), List.of(), List.of());

    assertFalse(document.has("runId"));
    assertEquals(document.get("systemIndexModified").asLong(), 999L);
  }
}
