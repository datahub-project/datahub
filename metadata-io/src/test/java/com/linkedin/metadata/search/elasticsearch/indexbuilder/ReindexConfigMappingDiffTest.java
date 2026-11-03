package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ReindexConfigMappingDiffTest {

  @Test
  public void testUnchangedCopyToArrayMatchesTheEngineList() {
    Map<String, Object> currentName = new HashMap<>();
    currentName.put("type", "keyword");
    currentName.put("copy_to", new String[] {"name", "other"});

    List<String> diff =
        ReindexConfig.compactMappingDiff(
            Map.of("name", currentName),
            Map.of("name", Map.of("type", "keyword", "copy_to", List.of("name", "other"))));

    assertTrue(diff.isEmpty(), diff.toString());
  }

  @Test
  public void testDiffNamesAddedAndRemovedFieldsAndLeafChanges() {
    List<String> diff =
        ReindexConfig.compactMappingDiff(
            Map.of(
                "name",
                Map.of(
                    "type",
                    "keyword",
                    "ignore_above",
                    256,
                    "fields",
                    Map.of("text", Map.of("type", "text", "analyzer", "standard"))),
                "old",
                Map.of("type", "keyword")),
            Map.of(
                "name",
                Map.of(
                    "type",
                    "keyword",
                    "ignore_above",
                    "512",
                    "fields",
                    Map.of("text", Map.of("type", "text", "analyzer", "datahub"))),
                "fresh",
                Map.of("type", "boolean")));

    assertEquals(
        diff,
        List.of(
            "- old",
            "+ fresh: {type=boolean}",
            "name.fields.text.analyzer: standard -> datahub",
            "name.ignore_above: 256 -> 512"));
  }
}
