package com.linkedin.metadata.search.elasticsearch.query;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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
                    "structuredProperties",
                        List.of("structuredProperties.my_property", "structuredProperties.other"),
                    "odd", "not a list")));

    Map<String, Set<String>> names = ESSearchDAO.aspectFieldNames(field);

    assertEquals(names.get("datasetProperties"), Set.of("name", "_systemMetadata"));
    assertEquals(
        names.get("structuredProperties"),
        Set.of("structuredProperties.my_property", "structuredProperties.other"));
    assertEquals(names.get("odd"), Set.of());
  }

  @Test
  public void testTheScriptReportsDottedStructuredPropertyKeys() {
    String script = ESSearchDAO.aspectFieldNamesScript();

    assertTrue(
        script.contains("startsWith('structuredProperties.')"),
        "top-level dotted keys are collected");
    assertTrue(
        script.contains("'structuredProperties.' + child"),
        "children of a nested structuredProperties object are collected as dotted keys");
    assertTrue(
        script.contains(ESSearchDAO.STALE_ASPECTS_STRUCTURED_PROPERTIES),
        "a leftover _aspects.structuredProperties entry is flagged separately");
    assertFalse(
        script.contains("= ['structuredProperties']"),
        "the aspect is no longer reported as the single sentinel field");
    assertTrue(
        script.contains(ESSearchDAO.NESTED_FIELD_SEPARATOR),
        "keys inside an object field are reported separately from dotted field names");
    assertTrue(
        script.contains("child.getValue() instanceof Map"),
        "object fields are walked so a dropped map entry can be cleared");
  }

  @Test
  public void testAMissingOrMalformedScriptFieldReadsAsNoAspects() {
    assertEquals(ESSearchDAO.aspectFieldNames(null), Map.of());
    assertEquals(
        ESSearchDAO.aspectFieldNames(new DocumentField("aspectFieldNames", List.of("junk"))),
        Map.of());
  }
}
