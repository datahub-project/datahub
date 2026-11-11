package com.linkedin.datahub.graphql.util;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.AspectLoadContext;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.SelectedField;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class AspectUtilsTest {

  private static final Set<String> ALL_ASPECTS =
      ImmutableSet.of("aspect1", "aspect2", "aspect3", "aspect4");
  private static final String ENTITY_TYPE = "Dataset";
  private static final String KEY_ASPECT = "datasetKey";

  @Test
  public void testOptimizedAspectsWhenLoadContextPresent() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(ImmutableSet.of("aspect1", "aspect2")));

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 3);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains("aspect2"));
    assertTrue(result.contains(KEY_ASPECT));
  }

  @Test
  public void testFallbackWhenAspectLoadContextNull() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE)).thenReturn(null);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertEquals(result, ALL_ASPECTS);
  }

  @Test
  public void testFallbackWhenLoadContextFetchAll() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE)).thenReturn(AspectLoadContext.fetchAll());

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertEquals(result, ALL_ASPECTS);
  }

  @Test
  public void testAlwaysIncludesKeyAspect() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(ImmutableSet.of("aspect1")));

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains(KEY_ASPECT));
  }

  @Test
  public void testHandlesEmptyRequiredAspects() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(Collections.emptySet()));

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertTrue(result.contains(KEY_ASPECT));
  }

  @Test
  public void testMultipleAlwaysIncludeAspects() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(ImmutableSet.of("aspect1")));

    Set<String> result =
        AspectUtils.getOptimizedAspects(
            mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT, "status");

    assertNotNull(result);
    assertEquals(result.size(), 3);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains(KEY_ASPECT));
    assertTrue(result.contains("status"));
  }

  @Test
  public void testNoAlwaysIncludeAspects() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(ImmutableSet.of("aspect1", "aspect2")));

    Set<String> result = AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains("aspect2"));
  }

  @Test
  public void testDeduplicatesAspects() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(ImmutableSet.of("aspect1", KEY_ASPECT)));

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains(KEY_ASPECT));
  }

  @Test
  public void testComputeLoadContextFromRegistry() {
    AspectMappingRegistry registry = mock(AspectMappingRegistry.class);
    List<SelectedField> fields = Collections.emptyList();
    when(registry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(ImmutableSet.of("aspect1"));

    AspectLoadContext loadContext = AspectUtils.computeLoadContext(registry, ENTITY_TYPE, fields);

    assertFalse(loadContext.isFetchAll());
    assertEquals(loadContext.getRequiredAspects(), ImmutableSet.of("aspect1"));
  }

  @Test
  public void testComputeLoadContextFallbackWhenRegistryNull() {
    AspectLoadContext loadContext =
        AspectUtils.computeLoadContext(null, ENTITY_TYPE, Collections.emptyList());
    assertTrue(loadContext.isFetchAll());
  }

  @Test
  public void testUnionKeyContexts() {
    AspectLoadContext a = AspectLoadContext.of(ImmutableSet.of("ownership"));
    AspectLoadContext b = AspectLoadContext.of(ImmutableSet.of("dataPlatformInstance"));
    AspectLoadContext union = AspectUtils.unionKeyContexts(List.of(a, b));
    assertNotNull(union);
    assertTrue(union.getRequiredAspects().contains("ownership"));
    assertTrue(union.getRequiredAspects().contains("dataPlatformInstance"));
  }

  /**
   * A batch entry without an AspectLoadContext means an unknown selection: the union must degrade
   * to fetch-all rather than underserve that load with the other entries' needs.
   */
  @Test
  public void testUnionKeyContextsDegradesToFetchAllOnContextlessEntry() {
    AspectLoadContext a = AspectLoadContext.of(ImmutableSet.of("ownership"));
    AspectLoadContext mixed = AspectUtils.unionKeyContexts(java.util.Arrays.asList(a, null));
    assertNotNull(mixed);
    assertTrue(mixed.isFetchAll());

    AspectLoadContext allBare = AspectUtils.unionKeyContexts(java.util.Arrays.asList(null, null));
    assertNotNull(allBare);
    assertTrue(allBare.isFetchAll());
  }

  @Test
  public void testAspectLoadContextUnionShortCircuits() {
    AspectLoadContext ownership = AspectLoadContext.of(ImmutableSet.of("ownership"));
    AspectLoadContext ownershipAndTags =
        AspectLoadContext.of(ImmutableSet.of("ownership", "globalTags"));

    assertSame(ownership.union(null), ownership);
    assertSame(ownership.union(ownership), ownership);
    assertSame(AspectLoadContext.fetchAll().union(ownership), AspectLoadContext.fetchAll());
    assertSame(ownership.union(AspectLoadContext.fetchAll()), AspectLoadContext.fetchAll());
    assertSame(ownershipAndTags.union(ownership), ownershipAndTags);
    assertSame(ownership.union(ownershipAndTags), ownershipAndTags);
  }

  @Test
  public void testAspectLoadContextEqualsAndCacheKeySignature() {
    AspectLoadContext a1 = AspectLoadContext.of(ImmutableSet.of("ownership", "globalTags"));
    AspectLoadContext a2 = AspectLoadContext.of(ImmutableSet.of("globalTags", "ownership"));
    AspectLoadContext b = AspectLoadContext.of(ImmutableSet.of("ownership"));

    assertEquals(a1, a2);
    assertEquals(a1.hashCode(), a2.hashCode());
    assertEquals(a1.cacheKeySignature(), a2.cacheKeySignature());
    assertNotEquals(a1, b);
    assertEquals(AspectLoadContext.fetchAll().cacheKeySignature(), "FETCH_ALL");
    assertEquals(AspectLoadContext.fetchAll(), AspectLoadContext.fetchAll());
  }

  @Test
  public void testEnsureFetchAllForDirectLoadWidensNarrowContext() {
    QueryContext mockContext = mock(QueryContext.class);
    when(mockContext.getAspectLoadContext(ENTITY_TYPE))
        .thenReturn(AspectLoadContext.of(ImmutableSet.of("aspect1")))
        .thenReturn(AspectLoadContext.fetchAll());

    // Without widening, optimization would under-fetch.
    Set<String> before =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);
    assertEquals(before, ImmutableSet.of("aspect1", KEY_ASPECT));

    AspectUtils.ensureFetchAllForDirectLoad(mockContext, ENTITY_TYPE);
    verify(mockContext).mergeAspectLoadContext(ENTITY_TYPE, AspectLoadContext.fetchAll());

    Set<String> after =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);
    assertEquals(after, ALL_ASPECTS);
  }
}
