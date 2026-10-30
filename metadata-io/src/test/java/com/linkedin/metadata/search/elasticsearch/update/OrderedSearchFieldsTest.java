package com.linkedin.metadata.search.elasticsearch.update;

import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class OrderedSearchFieldsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSplitMovesRunFieldsIntoTheScript() {
    ObjectNode document = MAPPER.createObjectNode();
    document.put("urn", "urn:li:dataHubIngestionSource:a");
    document.put("lastRunRequestedAtMillis", 200L);
    document.put("lastRunStatus", "SUCCESS");
    document.put("lastSuccessfulRunRequestedAtMillis", 200L);
    document.putNull("lastFailedRunRequestedAtMillis");
    document
        .putObject("_aspects")
        .putObject(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME)
        .put("status", "SUCCESS");

    OrderedSearchFields.Update update =
        OrderedSearchFields.split(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, document).orElseThrow();

    assertEquals(document.toString(), "{\"urn\":\"urn:li:dataHubIngestionSource:a\"}");
    Map<String, Object> params = update.params();
    assertEquals(((Number) params.get("orderValue")).longValue(), 200L);
    assertEquals(
        params.get("ordered"),
        Map.of("lastRunRequestedAtMillis", 200L, "lastRunStatus", "SUCCESS"));
    @SuppressWarnings("unchecked")
    Map<String, Object> max = (Map<String, Object>) params.get("max");
    assertEquals(((Number) max.get("lastSuccessfulRunRequestedAtMillis")).longValue(), 200L);
    assertNull(max.get("lastFailedRunRequestedAtMillis"));
    assertEquals(params.get("aspect"), Map.of("status", "SUCCESS"));
    assertEquals(
        update.upsert().keySet(),
        Set.of(
            "lastRunRequestedAtMillis",
            "lastRunStatus",
            "lastSuccessfulRunRequestedAtMillis",
            "_aspects"));
  }

  @Test
  public void testOtherAspectsAreUnchanged() {
    ObjectNode document = MAPPER.createObjectNode().put("lastRunStatus", "SUCCESS");

    assertTrue(OrderedSearchFields.split("datasetProperties", document).isEmpty());
    assertFalse(OrderedSearchFields.isOrdered("datasetProperties"));
    assertEquals(document.get("lastRunStatus").asText(), "SUCCESS");
  }

  @Test
  public void testRuleFieldsAreTheAspectsSearchableFields() {
    // Renaming a searchable field in the aspect would otherwise silently drop it from the script.
    AspectSpec aspectSpec =
        TestOperationContexts.defaultEntityRegistry()
            .getEntitySpec("dataHubIngestionSource")
            .getAspectSpec(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME);
    Set<String> searchable =
        aspectSpec.getSearchableFieldSpecs().stream()
            .map(SearchableFieldSpec::getSearchableAnnotation)
            .map(annotation -> annotation.getFieldName())
            .collect(Collectors.toSet());

    ObjectNode document = MAPPER.createObjectNode();
    searchable.forEach(field -> document.put(field, 1L));
    OrderedSearchFields.split(INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME, document);

    assertTrue(document.isEmpty(), "Fields not covered by the rule: " + document);
    assertEquals(
        searchable,
        Set.of(
            "lastRunRequestedAtMillis",
            "lastRunStatus",
            "lastSuccessfulRunRequestedAtMillis",
            "lastFailedRunRequestedAtMillis"));
  }
}
