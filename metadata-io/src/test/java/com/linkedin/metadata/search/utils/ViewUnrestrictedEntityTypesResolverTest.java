package com.linkedin.metadata.search.utils;

import com.datahub.authorization.config.ViewUnrestrictedEntityTypes;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ViewUnrestrictedEntityTypesResolverTest {

  @Test
  public void testEmptyConfigReturnsEmpty() {
    Assert.assertEquals(
        ViewUnrestrictedEntityTypesResolver.resolve(
            ViewUnrestrictedEntityTypes.builder().build(), null),
        Set.of());
  }

  @Test
  public void testNullConfigReturnsEmpty() {
    Assert.assertEquals(ViewUnrestrictedEntityTypesResolver.resolve(null, null), Set.of());
  }

  @Test
  public void testRemove() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup,container,actionRequest")
            .remove("container,actionRequest")
            .build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, null);
    Assert.assertEquals(result, Set.of("corpuser", "corpgroup"));
  }

  @Test
  public void testAdd() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup")
            .add("application")
            .build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, null);
    Assert.assertEquals(result, Set.of("corpuser", "corpgroup", "application"));
  }

  @Test
  public void testConfiguredDefault() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("corpuser,corpGroup").build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, null);
    Assert.assertEquals(result, Set.of("corpuser", "corpgroup"));
  }

  @Test
  public void testConfiguredDefaultWithAddRemove() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup,container")
            .add("actionRequest")
            .remove("container")
            .build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, null);
    Assert.assertEquals(result, Set.of("corpuser", "corpgroup", "actionrequest"));
  }

  @Test
  public void testUnknownEntityDropped() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec userSpec = namedSpec("corpuser");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("corpuser", userSpec));

    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("corpuser,notARealEntity").build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, registry);
    Assert.assertEquals(result, Set.of("corpuser"));
  }

  @Test
  public void testAddDoesNotDuplicateExisting() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("corpuser,corpGroup")
            .add("CORPUSER,application,corpgroup")
            .build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, null);
    Assert.assertEquals(new ArrayList<>(result), List.of("corpuser", "corpgroup", "application"));
  }

  @Test
  public void testValueDuplicatesPreserveOrder() {
    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder()
            .value("container,corpuser,corpGroup,container,CORPUSER")
            .build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, null);
    Assert.assertEquals(new ArrayList<>(result), List.of("container", "corpuser", "corpgroup"));
  }

  @Test
  public void testRegistryLookupIsCaseInsensitive() {
    EntityRegistry registry = Mockito.mock(EntityRegistry.class);
    EntitySpec groupSpec = namedSpec("corpGroup");
    Mockito.when(registry.getEntitySpecs()).thenReturn(Map.of("corpgroup", groupSpec));

    ViewUnrestrictedEntityTypes config =
        ViewUnrestrictedEntityTypes.builder().value("CORPGROUP,CorpGroup").build();
    Set<String> result = ViewUnrestrictedEntityTypesResolver.resolve(config, registry);
    Assert.assertEquals(result, Set.of("corpGroup"));
  }

  private static EntitySpec namedSpec(String name) {
    EntitySpec spec = Mockito.mock(EntitySpec.class);
    Mockito.when(spec.getName()).thenReturn(name);
    return spec;
  }
}
