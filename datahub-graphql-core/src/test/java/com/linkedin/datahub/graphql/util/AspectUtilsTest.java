package com.linkedin.datahub.graphql.util;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.AspectMappingRegistry;
import com.linkedin.datahub.graphql.QueryContext;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.DataFetchingFieldSelectionSet;
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
  public void testOptimizedAspectsWhenContextComplete() {
    QueryContext mockContext = mock(QueryContext.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();
    Set<String> requiredAspects = ImmutableSet.of("aspect1", "aspect2");

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(requiredAspects);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 3);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains("aspect2"));
    assertTrue(result.contains(KEY_ASPECT));
  }

  @Test
  public void testFallbackWhenDataFetchingEnvironmentNull() {
    QueryContext mockContext = mock(QueryContext.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    when(mockContext.getDataFetchingEnvironment()).thenReturn(null);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertEquals(result, ALL_ASPECTS);
    verify(mockRegistry, never()).getRequiredAspects(anyString(), anyList());
  }

  @Test
  public void testFallbackWhenAspectMappingRegistryNull() {
    QueryContext mockContext = mock(QueryContext.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(null);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertEquals(result, ALL_ASPECTS);
  }

  @Test
  public void testFallbackWhenRegistryReturnsNull() {
    QueryContext mockContext = mock(QueryContext.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(null);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertEquals(result, ALL_ASPECTS);
  }

  @Test
  public void testAlwaysIncludesKeyAspect() {
    QueryContext mockContext = mock(QueryContext.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();
    Set<String> requiredAspects = ImmutableSet.of("aspect1");

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(requiredAspects);

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
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();
    Set<String> requiredAspects = Collections.emptySet();

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(requiredAspects);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 1);
    assertTrue(result.contains(KEY_ASPECT));
  }

  @Test
  public void testMultipleAlwaysIncludeAspects() {
    QueryContext mockContext = mock(QueryContext.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();
    Set<String> requiredAspects = ImmutableSet.of("aspect1");

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(requiredAspects);

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
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();
    Set<String> requiredAspects = ImmutableSet.of("aspect1", "aspect2");

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(requiredAspects);

    Set<String> result = AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains("aspect2"));
  }

  @Test
  public void testDeduplicatesAspects() {
    QueryContext mockContext = mock(QueryContext.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);
    DataFetchingFieldSelectionSet mockSelectionSet = mock(DataFetchingFieldSelectionSet.class);
    AspectMappingRegistry mockRegistry = mock(AspectMappingRegistry.class);

    List<SelectedField> fields = Collections.emptyList();
    Set<String> requiredAspects = ImmutableSet.of("aspect1", KEY_ASPECT);

    when(mockContext.getDataFetchingEnvironment()).thenReturn(mockEnv);
    when(mockContext.getAspectMappingRegistry()).thenReturn(mockRegistry);
    when(mockEnv.getSelectionSet()).thenReturn(mockSelectionSet);
    when(mockSelectionSet.getFields()).thenReturn(fields);
    when(mockRegistry.getRequiredAspects(ENTITY_TYPE, fields)).thenReturn(requiredAspects);

    Set<String> result =
        AspectUtils.getOptimizedAspects(mockContext, ENTITY_TYPE, ALL_ASPECTS, KEY_ASPECT);

    assertNotNull(result);
    assertEquals(result.size(), 2);
    assertTrue(result.contains("aspect1"));
    assertTrue(result.contains(KEY_ASPECT));
  }
}
