package com.linkedin.metadata.search.utils;

import com.datahub.authorization.config.ViewUnrestrictedEntityTypes;
import com.linkedin.metadata.config.search.EntityTypeListConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EntityTypeUtilsTest {

  @Test
  public void testSearchListEmptyConfigReturnsEmpty() {
    Assert.assertEquals(
        EntityTypeUtils.resolve(EntityTypeListConfig.builder().build(), null), List.of());
  }

  @Test
  public void testSearchListNullConfigReturnsEmpty() {
    Assert.assertEquals(EntityTypeUtils.resolve((EntityTypeListConfig) null, null), List.of());
  }

  @Test
  public void testSearchListRemove() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard,chart,schemaField,document,container")
            .remove("schemaField,document")
            .build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null),
        List.of("dataset", "dashboard", "chart", "container"));
  }

  @Test
  public void testSearchListAdd() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("dataset,dashboard").add("metric").build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null), List.of("dataset", "dashboard", "metric"));
  }

  @Test
  public void testSearchListValueOnly() {
    EntityTypeListConfig config = EntityTypeListConfig.builder().value("dataset,dashboard").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, null), List.of("dataset", "dashboard"));
  }

  @Test
  public void testSearchListValueWithAddRemove() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard,schemaField")
            .add("metric")
            .remove("schemaField")
            .build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null), List.of("dataset", "dashboard", "metric"));
  }

  @Test
  public void testSearchListUnknownEntityDropped() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec datasetSpec = searchableSpec("dataset");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("dataset", datasetSpec));

    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("dataset,notARealEntity").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), List.of("dataset"));
  }

  @Test
  public void testSearchListAllUnknownEntitiesResolveToEmpty() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec datasetSpec = searchableSpec("dataset");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("dataset", datasetSpec));

    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("notARealEntity,alsoFake").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), List.of());
  }

  @Test
  public void testSearchListAddDoesNotDuplicateExisting() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder()
            .value("dataset,dashboard")
            .add("DATASET,metric,dashboard")
            .build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null), List.of("dataset", "dashboard", "metric"));
  }

  @Test
  public void testSearchListValueDuplicatesPreserveOrder() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("chart,dataset,dashboard,chart,DATASET").build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null), List.of("chart", "dataset", "dashboard"));
  }

  @Test
  public void testSearchListAddAppendsAfterValuePreservingOrder() {
    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("dataset,dashboard").add("chart,container").build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null),
        List.of("dataset", "dashboard", "chart", "container"));
  }

  @Test
  public void testSearchListRegistryLookupIsCaseInsensitive() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec datasetSpec = searchableSpec("schemaField");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("schemafield", datasetSpec));

    EntityTypeListConfig config =
        EntityTypeListConfig.builder().value("SCHEMAFIELD,SchemaField").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), List.of("schemaField"));
  }

  @Test
  public void testSearchListNonSearchableStillIncluded() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec emptySpec = Mockito.mock(EntitySpec.class);
    Mockito.when(emptySpec.getName()).thenReturn("customentity");
    Mockito.when(emptySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("customentity", emptySpec));

    EntityTypeListConfig config = EntityTypeListConfig.builder().value("CustomEntity").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), List.of("customentity"));
  }

  @Test
  public void testViewUnrestrictedEmptyConfigWithNullRegistryReturnsEmpty() {
    Assert.assertEquals(
        EntityTypeUtils.resolve(ViewUnrestrictedEntityTypes.builder().build(), null), Set.of());
  }

  @Test
  public void testViewUnrestrictedNullConfigWithNullRegistryReturnsEmpty() {
    Assert.assertEquals(
        EntityTypeUtils.resolve((ViewUnrestrictedEntityTypes) null, null), Set.of());
  }

  @Test
  public void testViewUnrestrictedEmptyConfigUsesRegistryBaseline() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec userSpec = viewUnrestrictedSpec("corpuser");
    EntitySpec datasetSpec = namedSpec("dataset");
    Mockito.when(datasetSpec.isViewUnrestricted()).thenReturn(false);
    Mockito.when(registry.getEntitySpecs())
        .thenReturn(Map.of("corpuser", userSpec, "dataset", datasetSpec));

    Assert.assertEquals(
        EntityTypeUtils.resolve(ViewUnrestrictedEntityTypes.builder().build(), registry),
        Set.of("corpuser"));
  }

  @Test
  public void testViewUnrestrictedAddOverlaysRegistryBaseline() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec userSpec = viewUnrestrictedSpec("corpuser");
    EntitySpec appSpec = namedSpec("application");
    Mockito.when(appSpec.isViewUnrestricted()).thenReturn(false);
    Mockito.when(registry.getEntitySpecs())
        .thenReturn(Map.of("corpuser", userSpec, "application", appSpec));

    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().add("application").build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, registry), Set.of("corpuser", "application"));
  }

  @Test
  public void testViewUnrestrictedValueReplacesRegistryBaseline() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec userSpec = viewUnrestrictedSpec("corpuser");
    EntitySpec groupSpec = namedSpec("corpGroup");
    Mockito.when(groupSpec.isViewUnrestricted()).thenReturn(false);
    Mockito.when(registry.getEntitySpecs())
        .thenReturn(Map.of("corpuser", userSpec, "corpgroup", groupSpec));

    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("corpGroup").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), Set.of("corpGroup"));
  }

  @Test
  public void testViewUnrestrictedRemove() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup,container,actionRequest")
            .remove("container,actionRequest")
            .build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, null), Set.of("corpuser", "corpgroup"));
  }

  @Test
  public void testViewUnrestrictedAdd() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup")
            .add("application")
            .build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null), Set.of("corpuser", "corpgroup", "application"));
  }

  @Test
  public void testViewUnrestrictedConfiguredValue() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("corpuser,corpGroup").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, null), Set.of("corpuser", "corpgroup"));
  }

  @Test
  public void testViewUnrestrictedConfiguredValueWithAddRemove() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup,container")
            .add("actionRequest")
            .remove("container")
            .build();
    Assert.assertEquals(
        EntityTypeUtils.resolve(config, null), Set.of("corpuser", "corpgroup", "actionrequest"));
  }

  @Test
  public void testViewUnrestrictedUnknownEntityDropped() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec userSpec = namedSpec("corpuser");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("corpuser", userSpec));

    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("corpuser,notARealEntity").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), Set.of("corpuser"));
  }

  @Test
  public void testViewUnrestrictedAddDoesNotDuplicateExisting() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup")
            .add("CORPUSER,application,corpgroup")
            .build();
    Assert.assertEquals(
        new ArrayList<>(EntityTypeUtils.resolve(config, null)),
        List.of("corpuser", "corpgroup", "application"));
  }

  @Test
  public void testViewUnrestrictedValueDuplicatesPreserveOrder() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("container,corpuser,corpGroup,container,CORPUSER")
            .build();
    Assert.assertEquals(
        new ArrayList<>(EntityTypeUtils.resolve(config, null)),
        List.of("container", "corpuser", "corpgroup"));
  }

  @Test
  public void testViewUnrestrictedRegistryLookupIsCaseInsensitive() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec groupSpec = namedSpec("corpGroup");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("corpgroup", groupSpec));

    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("CORPGROUP,CorpGroup").build();
    Assert.assertEquals(EntityTypeUtils.resolve(config, registry), Set.of("corpGroup"));
  }

  private static EntitySpec searchableSpec(String name) {
    EntitySpec spec = Mockito.mock(EntitySpec.class);
    Mockito.when(spec.getName()).thenReturn(name);
    Mockito.when(spec.getSearchableFieldSpecs())
        .thenReturn(List.of(Mockito.mock(SearchableFieldSpec.class)));
    return spec;
  }

  private static EntitySpec namedSpec(String name) {
    EntitySpec spec = Mockito.mock(EntitySpec.class);
    Mockito.when(spec.getName()).thenReturn(name);
    return spec;
  }

  private static EntitySpec viewUnrestrictedSpec(String name) {
    EntitySpec spec = namedSpec(name);
    Mockito.when(spec.isViewUnrestricted()).thenReturn(true);
    return spec;
  }
}
