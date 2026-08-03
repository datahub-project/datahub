package com.linkedin.metadata.search.write;

import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.testng.annotations.Test;

public class AbstractEntitySearchWriteSinkMergeRunIdTest {

  @Test
  public void mergeAddsRunIdToEmptyDocument() throws Exception {
    String merged =
        AbstractEntitySearchWriteSink.mergeRunIdIntoDocumentJson(
            null, "urn:li:dataset:(a,b,c)", "r1");
    JsonNode root = AbstractEntitySearchWriteSink.MAPPER.readTree(merged);
    assertEquals(root.get("urn").asText(), "urn:li:dataset:(a,b,c)");
    ArrayNode runIds = (ArrayNode) root.get("runId");
    assertEquals(runIds.size(), 1);
    assertEquals(runIds.get(0).asText(), "r1");
  }

  @Test
  public void mergeDedupesAndTrimsToMax() throws Exception {
    StringBuilder existing = new StringBuilder("{\"urn\":\"x\",\"runId\":[");
    for (int i = 0; i < AbstractEntitySearchWriteSink.MAX_RUN_IDS_INDEXED; i++) {
      if (i > 0) {
        existing.append(',');
      }
      existing.append('"').append("old").append(i).append('"');
    }
    existing.append("]}");
    String merged =
        AbstractEntitySearchWriteSink.mergeRunIdIntoDocumentJson(
            existing.toString(), "x", "newRun");
    JsonNode root = AbstractEntitySearchWriteSink.MAPPER.readTree(merged);
    ArrayNode runIds = (ArrayNode) root.get("runId");
    assertEquals(runIds.size(), AbstractEntitySearchWriteSink.MAX_RUN_IDS_INDEXED);
    assertEquals(runIds.get(runIds.size() - 1).asText(), "newRun");
    assertEquals(runIds.get(0).asText(), "old1");
  }

  @Test
  public void mergeBlankRunIdLeavesExisting() {
    String in = "{\"urn\":\"u\",\"runId\":[\"a\"]}";
    String merged = AbstractEntitySearchWriteSink.mergeRunIdIntoDocumentJson(in, "u", "  ");
    assertEquals(merged, in);
  }
}
