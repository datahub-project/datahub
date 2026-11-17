package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.testng.Assert.assertEquals;

import java.util.Map;
import org.testng.annotations.Test;

public class SearchGroupMappingAuditTest {

  @Test
  public void countMappedFieldsIncludesMultifields() {
    // name: text + 2 multifields (.keyword, .ngram) = 3; description: keyword = 1
    Map<String, Object> mappings =
        Map.of(
            "properties",
            Map.of(
                "name",
                Map.of(
                    "type",
                    "text",
                    "fields",
                    Map.of(
                        "keyword", Map.of("type", "keyword"),
                        "ngram", Map.of("type", "search_as_you_type"))),
                "description",
                Map.of("type", "keyword")));

    assertEquals(SearchGroupMappingAudit.countMappedFields(mappings), 4);
  }

  @Test
  public void countMappedFieldsIncludesNestedObjectChildrenAndTheirMultifields() {
    // _aspects: 1 (object node) + child: 1 + child multifield: 1 = 3
    Map<String, Object> mappings =
        Map.of(
            "properties",
            Map.of(
                "_aspects",
                Map.of(
                    "properties",
                    Map.of(
                        "field",
                        Map.of(
                            "type",
                            "text",
                            "fields",
                            Map.of("keyword", Map.of("type", "keyword")))))));

    assertEquals(SearchGroupMappingAudit.countMappedFields(mappings), 3);
  }
}
