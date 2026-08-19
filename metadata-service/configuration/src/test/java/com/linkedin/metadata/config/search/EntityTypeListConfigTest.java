package com.linkedin.metadata.config.search;

import static org.testng.Assert.*;

import java.util.List;
import org.testng.annotations.Test;

public class EntityTypeListConfigTest {

  @Test
  public void testEmptyDefaults() {
    EntityTypeListConfig config = EntityTypeListConfig.builder().build();
    assertTrue(config.isEmpty());
    assertEquals(config.parsedValue(), List.of());
    assertEquals(config.parsedAdd(), List.of());
    assertEquals(config.parsedRemove(), List.of());
  }

  @Test
  public void testParseCsvTrimsAndLowercases() {
    assertEquals(
        EntityTypeListConfig.parseCsv(" schemaField , Document,,DATASET "),
        List.of("schemafield", "document", "dataset"));
  }

  @Test
  public void testParseCsvDedupesPreservingOrder() {
    assertEquals(
        EntityTypeListConfig.parseCsv("dataset,Dashboard,chart,dataset,CHART,metric"),
        List.of("dataset", "dashboard", "chart", "metric"));
  }

  @Test
  public void testValueAddRemoveCanCombine() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard,schemaField")
            .add("metric")
            .remove("schemaField")
            .build();
    assertFalse(config.isEmpty());
    assertEquals(config.parsedValue(), List.of("dataset", "dashboard", "schemafield"));
    assertEquals(config.parsedAdd(), List.of("metric"));
    assertEquals(config.parsedRemove(), List.of("schemafield"));
  }

  @Test
  public void testDefaultCsvConstantsParseNonEmpty() {
    assertFalse(
        EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_SEARCH_ENTITY_TYPES).isEmpty());
    assertFalse(
        EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_AUTOCOMPLETE_ENTITY_TYPES)
            .isEmpty());
    assertFalse(
        EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_BROWSE_ENTITY_TYPES).isEmpty());
    assertFalse(
        EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_PRIORITIZED_SOURCE_ENTITY_TYPES)
            .isEmpty());
    assertFalse(
        EntityTypeListConfig.parseCsv(EntityTypeListConfig.DEFAULT_PRIORITIZED_DATAHUB_ENTITY_TYPES)
            .isEmpty());
  }
}
