package com.linkedin.metadata.search.utils;

import com.linkedin.metadata.config.search.EntityTypeListConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EntityTypeListResolverTest {

  @Test
  public void testEmptyConfigReturnsEmpty() {
    Assert.assertEquals(
        EntityTypeListResolver.resolve(EntityTypeListConfig.builder().build(), null), List.of());
  }

  @Test
  public void testNullConfigReturnsEmpty() {
    Assert.assertEquals(EntityTypeListResolver.resolve(null, null), List.of());
  }

  @Test
  public void testRemove() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard,chart,schemaField,document,container")
            .remove("schemaField,document")
            .build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("dataset", "dashboard", "chart", "container"));
  }

  @Test
  public void testAdd() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("dataset,dashboard").add("metric").build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("dataset", "dashboard", "metric"));
  }

  @Test
  public void testValueOnly() {
    EntityTypeListConfig config = EntityTypeListConfig.builder().value("dataset,dashboard").build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("dataset", "dashboard"));
  }

  @Test
  public void testValueWithAddRemove() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard,schemaField")
            .add("metric")
            .remove("schemaField")
            .build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("dataset", "dashboard", "metric"));
  }

  @Test
  public void testUnknownEntityDropped() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec datasetSpec = searchableSpec("dataset");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("dataset", datasetSpec));

    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("dataset,notARealEntity").build();
    List<String> result = EntityTypeListResolver.resolve(config, registry);
    Assert.assertEquals(result, List.of("dataset"));
  }

  @Test
  public void testAllUnknownEntitiesResolveToEmpty() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec datasetSpec = searchableSpec("dataset");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("dataset", datasetSpec));

    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("notARealEntity,alsoFake").build();
    List<String> result = EntityTypeListResolver.resolve(config, registry);
    Assert.assertEquals(result, List.of());
  }

  @Test
  public void testAddDoesNotDuplicateExisting() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard")
            .add("DATASET,metric,dashboard")
            .build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("dataset", "dashboard", "metric"));
  }

  @Test
  public void testValueDuplicatesPreserveOrder() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("chart,dataset,dashboard,chart,DATASET").build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("chart", "dataset", "dashboard"));
  }

  @Test
  public void testAddAppendsAfterValuePreservingOrder() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("dataset,dashboard").add("chart,container").build();
    List<String> result = EntityTypeListResolver.resolve(config, null);
    Assert.assertEquals(result, List.of("dataset", "dashboard", "chart", "container"));
  }

  @Test
  public void testRegistryLookupIsCaseInsensitive() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec datasetSpec = searchableSpec("schemaField");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("schemafield", datasetSpec));

    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("SCHEMAFIELD,SchemaField").build();
    List<String> result = EntityTypeListResolver.resolve(config, registry);
    Assert.assertEquals(result, List.of("schemaField"));
  }

  @Test
  public void testNonSearchableStillIncludedWithWarning() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec emptySpec = Mockito.mock(EntitySpec.class);
    Mockito.when(emptySpec.getName()).thenReturn("customentity");
    Mockito.when(emptySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("customentity", emptySpec));

    EntityTypeListConfig config = EntityTypeListConfig.builder().value("CustomEntity").build();
    List<String> result = EntityTypeListResolver.resolve(config, registry);
    Assert.assertEquals(result, List.of("customentity"));
  }

  private static EntitySpec searchableSpec(String name) {
    EntitySpec spec = Mockito.mock(EntitySpec.class);
    Mockito.when(spec.getName()).thenReturn(name);
    Mockito.when(spec.getSearchableFieldSpecs())
        .thenReturn(List.of(Mockito.mock(SearchableFieldSpec.class)));
    return spec;
  }
}
