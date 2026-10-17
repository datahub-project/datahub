package com.linkedin.metadata.search.elasticsearch.query;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.common.document.DocumentField;
import org.testng.annotations.Test;

public class ESSearchDAOAspectFieldNamesTest {

  @Test
  public void testAScriptFieldValueIsReadAsAspectNamesToFieldNames() {
    // The engine wraps a map-valued script field in a one-element list.
    DocumentField field =
        new DocumentField(
            "aspectFieldNames",
            List.of(
                Map.of(
                    "datasetProperties", List.of("name", "_systemMetadata"),
                    "structuredProperties", List.of("structuredProperties"),
                    "odd", "not a list")));

    Map<String, Set<String>> names = ESSearchDAO.aspectFieldNames(field);

    assertEquals(names.get("datasetProperties"), Set.of("name", "_systemMetadata"));
    assertEquals(names.get("structuredProperties"), Set.of("structuredProperties"));
    assertEquals(names.get("odd"), Set.of());
  }

  @Test
  public void testAMissingOrMalformedScriptFieldReadsAsNoAspects() {
    assertEquals(ESSearchDAO.aspectFieldNames(null), Map.of());
    assertEquals(
        ESSearchDAO.aspectFieldNames(new DocumentField("aspectFieldNames", List.of("junk"))),
        Map.of());
  }
}
