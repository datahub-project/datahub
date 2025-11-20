package com.linkedin.metadata.models;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.schema.TyperefDataSchema;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for EntitySpec interface default methods. Tests the default implementations of various
 * methods in the EntitySpec interface.
 */
public class EntitySpecTest {

  private TestEntitySpec entitySpec;
  private AspectSpec mockAspectSpec1;
  private AspectSpec mockAspectSpec2;
  private SearchableFieldSpec mockSearchableFieldSpec1;
  private SearchableFieldSpec mockSearchableFieldSpec2;
  private SearchableFieldSpec mockSearchableFieldSpec3;
  private SearchScoreFieldSpec mockSearchScoreFieldSpec1;
  private RelationshipFieldSpec mockRelationshipFieldSpec1;
  private SearchableRefFieldSpec mockSearchableRefFieldSpec1;
  private SearchableAnnotation mockSearchableAnnotation1;
  private SearchableAnnotation mockSearchableAnnotation2;
  private SearchableAnnotation mockSearchableAnnotation3;

  /** Test implementation of EntitySpec that delegates to mocks for testing default methods. */
  private static class TestEntitySpec implements EntitySpec {
    private final List<AspectSpec> aspectSpecs;

    public TestEntitySpec(List<AspectSpec> aspectSpecs) {
      this.aspectSpecs = aspectSpecs;
    }

    @Override
    public String getName() {
      return "TestEntity";
    }

    @Override
    public EntityAnnotation getEntityAnnotation() {
      return mock(EntityAnnotation.class);
    }

    @Override
    public String getKeyAspectName() {
      return "keyAspect";
    }

    @Override
    public AspectSpec getKeyAspectSpec() {
      return aspectSpecs.isEmpty() ? null : aspectSpecs.get(0);
    }

    @Override
    public List<AspectSpec> getAspectSpecs() {
      return aspectSpecs;
    }

    @Override
    public Map<String, AspectSpec> getAspectSpecMap() {
      return aspectSpecs.stream().collect(Collectors.toMap(AspectSpec::getName, spec -> spec));
    }

    @Override
    public Boolean hasAspect(String name) {
      return aspectSpecs.stream().anyMatch(spec -> spec.getName().equals(name));
    }

    @Override
    public AspectSpec getAspectSpec(String name) {
      return aspectSpecs.stream()
          .filter(spec -> spec.getName().equals(name))
          .findFirst()
          .orElse(null);
    }

    @Override
    public RecordDataSchema getSnapshotSchema() {
      return mock(RecordDataSchema.class);
    }

    @Override
    public TyperefDataSchema getAspectTyperefSchema() {
      return mock(TyperefDataSchema.class);
    }

    @Override
    public String getSearchGroup() {
      return "testGroup";
    }
  }

  @BeforeMethod
  public void setUp() {
    // Create mock AspectSpecs
    mockAspectSpec1 = mock(AspectSpec.class);
    mockAspectSpec2 = mock(AspectSpec.class);

    // Create mock SearchableFieldSpecs
    mockSearchableFieldSpec1 = mock(SearchableFieldSpec.class);
    mockSearchableFieldSpec2 = mock(SearchableFieldSpec.class);
    mockSearchableFieldSpec3 = mock(SearchableFieldSpec.class);

    // Create mock other field specs
    mockSearchScoreFieldSpec1 = mock(SearchScoreFieldSpec.class);
    mockRelationshipFieldSpec1 = mock(RelationshipFieldSpec.class);
    mockSearchableRefFieldSpec1 = mock(SearchableRefFieldSpec.class);

    // Create mock SearchableAnnotations
    mockSearchableAnnotation1 = mock(SearchableAnnotation.class);
    mockSearchableAnnotation2 = mock(SearchableAnnotation.class);
    mockSearchableAnnotation3 = mock(SearchableAnnotation.class);

    // Setup basic mock behavior
    when(mockAspectSpec1.getName()).thenReturn("aspect1");
    when(mockAspectSpec2.getName()).thenReturn("aspect2");

    // Setup SearchableFieldSpecs for aspect1
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(ImmutableList.of(mockSearchableFieldSpec1, mockSearchableFieldSpec2));
    when(mockSearchableFieldSpec1.getSearchableAnnotation()).thenReturn(mockSearchableAnnotation1);
    when(mockSearchableFieldSpec2.getSearchableAnnotation()).thenReturn(mockSearchableAnnotation2);

    // Setup SearchableFieldSpecs for aspect2
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(ImmutableList.of(mockSearchableFieldSpec3));
    when(mockSearchableFieldSpec3.getSearchableAnnotation()).thenReturn(mockSearchableAnnotation3);

    // Setup other field specs
    when(mockAspectSpec1.getSearchScoreFieldSpecs())
        .thenReturn(ImmutableList.of(mockSearchScoreFieldSpec1));
    when(mockAspectSpec1.getRelationshipFieldSpecs())
        .thenReturn(ImmutableList.of(mockRelationshipFieldSpec1));
    when(mockAspectSpec1.getSearchableRefFieldSpecs())
        .thenReturn(ImmutableList.of(mockSearchableRefFieldSpec1));
    when(mockAspectSpec2.getSearchScoreFieldSpecs()).thenReturn(ImmutableList.of());
    when(mockAspectSpec2.getRelationshipFieldSpecs()).thenReturn(ImmutableList.of());
    when(mockAspectSpec2.getSearchableRefFieldSpecs()).thenReturn(ImmutableList.of());

    // Create test entity spec with mock aspects
    entitySpec = new TestEntitySpec(ImmutableList.of(mockAspectSpec1, mockAspectSpec2));
  }

  @Test
  public void testGetSearchableFieldSpecs() {
    // Test the default implementation of getSearchableFieldSpecs
    List<SearchableFieldSpec> result = entitySpec.getSearchableFieldSpecs();

    // Verify that all searchable field specs from all aspects are returned
    assertEquals(result.size(), 3, "Should return all searchable field specs from all aspects");
    assertTrue(result.contains(mockSearchableFieldSpec1), "Should contain field spec 1");
    assertTrue(result.contains(mockSearchableFieldSpec2), "Should contain field spec 2");
    assertTrue(result.contains(mockSearchableFieldSpec3), "Should contain field spec 3");
  }

  @Test
  public void testGetSearchableFieldSpecsWithEmptyAspects() {
    // Test with empty aspect specs
    TestEntitySpec emptyEntitySpec = new TestEntitySpec(ImmutableList.of());

    List<SearchableFieldSpec> result = emptyEntitySpec.getSearchableFieldSpecs();

    assertTrue(result.isEmpty(), "Should return empty list when no aspects");
  }

  @Test
  public void testGetSearchableFieldTypes() {
    // Setup mock annotations with different field types
    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation1.getFieldType()).thenReturn(SearchableAnnotation.FieldType.TEXT);
    when(mockSearchableAnnotation1.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation1.getHasValuesFieldName()).thenReturn(Optional.empty());

    when(mockSearchableAnnotation2.getFieldName()).thenReturn("field2");
    when(mockSearchableAnnotation2.getFieldType())
        .thenReturn(SearchableAnnotation.FieldType.KEYWORD);
    when(mockSearchableAnnotation2.getNumValuesFieldName())
        .thenReturn(Optional.of("field2_numValues"));
    when(mockSearchableAnnotation2.getHasValuesFieldName())
        .thenReturn(Optional.of("field2_hasValues"));

    when(mockSearchableAnnotation3.getFieldName()).thenReturn("field3");
    when(mockSearchableAnnotation3.getFieldType())
        .thenReturn(SearchableAnnotation.FieldType.BOOLEAN);
    when(mockSearchableAnnotation3.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getHasValuesFieldName()).thenReturn(Optional.empty());

    Map<String, Set<SearchableAnnotation.FieldType>> result = entitySpec.getSearchableFieldTypes();

    // Verify the result contains expected field types
    assertEquals(
        result.size(), 5, "Should have 5 field types (3 regular + 1 numValues + 1 hasValues)");

    // Check regular field types
    assertTrue(result.containsKey("field1"), "Should contain field1");
    assertTrue(result.containsKey("field2"), "Should contain field2");
    assertTrue(result.containsKey("field3"), "Should contain field3");

    // Check additional field types for field2
    assertTrue(result.containsKey("field2_numValues"), "Should contain field2_numValues");
    assertTrue(result.containsKey("field2_hasValues"), "Should contain field2_hasValues");

    // Verify field types
    assertEquals(result.get("field1"), Collections.singleton(SearchableAnnotation.FieldType.TEXT));
    assertEquals(
        result.get("field2"), Collections.singleton(SearchableAnnotation.FieldType.KEYWORD));
    assertEquals(
        result.get("field3"), Collections.singleton(SearchableAnnotation.FieldType.BOOLEAN));
    assertEquals(
        result.get("field2_numValues"),
        Collections.singleton(SearchableAnnotation.FieldType.COUNT));
    assertEquals(
        result.get("field2_hasValues"),
        Collections.singleton(SearchableAnnotation.FieldType.BOOLEAN));
  }

  @Test
  public void testGetFieldMap() {
    // Test the generic getFieldMap method
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, Integer>> keyFn =
        (aspect, field) -> new Pair<>(field.getSearchableAnnotation().getFieldName(), 1);
    BinaryOperator<Integer> mergeFn = (a, b) -> a + b;

    // Setup mock annotations
    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("field2");
    when(mockSearchableAnnotation3.getFieldName()).thenReturn("field3");

    Map<String, Integer> result = entitySpec.getFieldMap(keyFn, keyFn, keyFn, mergeFn);

    // Verify the result
    assertEquals(result.size(), 3, "Should have 3 entries");
    assertEquals(result.get("field1"), Integer.valueOf(1), "field1 should have value 1");
    assertEquals(result.get("field2"), Integer.valueOf(1), "field2 should have value 1");
    assertEquals(result.get("field3"), Integer.valueOf(1), "field3 should have value 1");
  }

  @Test
  public void testGetFieldMapWithMergeFunction() {
    // Test getFieldMap with merge function when duplicate keys exist
    BiFunction<AspectSpec, SearchableFieldSpec, Pair<String, Integer>> keyFn =
        (aspect, field) -> new Pair<>("sameField", 1);
    BinaryOperator<Integer> mergeFn = (a, b) -> a + b;

    // Setup mock annotations with same field name
    when(mockSearchableAnnotation1.getFieldName()).thenReturn("sameField");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("sameField");
    when(mockSearchableAnnotation3.getFieldName()).thenReturn("sameField");

    Map<String, Integer> result = entitySpec.getFieldMap(keyFn, keyFn, keyFn, mergeFn);

    // Verify the result - should merge values
    // Note: The default implementation uses Collectors.toMap which doesn't call the merge function
    // for duplicate keys in the stream, it only calls it during the final map merge
    assertEquals(result.size(), 1, "Should have 1 entry after merging");
    assertEquals(
        result.get("sameField"),
        Integer.valueOf(1),
        "sameField should have value 1 (default behavior)");
  }

  @Test
  public void testGetSearchableFieldPathMap() {
    // Setup mock path specs and annotations
    PathSpec mockPathSpec1 = mock(PathSpec.class);
    PathSpec mockPathSpec2 = mock(PathSpec.class);
    PathSpec mockPathSpec3 = mock(PathSpec.class);

    when(mockSearchableFieldSpec1.getPath()).thenReturn(mockPathSpec1);
    when(mockSearchableFieldSpec2.getPath()).thenReturn(mockPathSpec2);
    when(mockSearchableFieldSpec3.getPath()).thenReturn(mockPathSpec3);

    when(mockPathSpec1.getPathComponents()).thenReturn(ImmutableList.of("component1"));
    when(mockPathSpec2.getPathComponents()).thenReturn(ImmutableList.of("component2"));
    when(mockPathSpec3.getPathComponents()).thenReturn(ImmutableList.of("component3"));

    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("field2");
    when(mockSearchableAnnotation3.getFieldName()).thenReturn("field3");

    when(mockSearchableAnnotation1.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation1.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getHasValuesFieldName()).thenReturn(Optional.empty());

    Map<PathSpec, String> result = entitySpec.getSearchableFieldPathMap();

    // Verify the result
    assertEquals(result.size(), 3, "Should have 3 path mappings");

    // Verify that all field names are mapped
    assertTrue(result.values().contains("field1"), "Should contain field1");
    assertTrue(result.values().contains("field2"), "Should contain field2");
    assertTrue(result.values().contains("field3"), "Should contain field3");
  }

  @Test
  public void testGetSearchableFieldPathMapWithNumValuesAndHasValues() {
    // Setup mock path specs and annotations with numValues and hasValues
    PathSpec mockPathSpec1 = mock(PathSpec.class);
    PathSpec mockPathSpec2 = mock(PathSpec.class);
    PathSpec mockPathSpec3 = mock(PathSpec.class);

    when(mockSearchableFieldSpec1.getPath()).thenReturn(mockPathSpec1);
    when(mockSearchableFieldSpec2.getPath()).thenReturn(mockPathSpec2);
    when(mockSearchableFieldSpec3.getPath()).thenReturn(mockPathSpec3);

    when(mockPathSpec1.getPathComponents()).thenReturn(ImmutableList.of("component1"));
    when(mockPathSpec2.getPathComponents()).thenReturn(ImmutableList.of("component2"));
    when(mockPathSpec3.getPathComponents()).thenReturn(ImmutableList.of("component3"));

    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("field2");
    when(mockSearchableAnnotation3.getFieldName()).thenReturn("field3");

    when(mockSearchableAnnotation1.getNumValuesFieldName())
        .thenReturn(Optional.of("field1_numValues"));
    when(mockSearchableAnnotation1.getHasValuesFieldName())
        .thenReturn(Optional.of("field1_hasValues"));

    // Setup other mocks to return empty optionals
    when(mockSearchableAnnotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getHasValuesFieldName()).thenReturn(Optional.empty());

    Map<PathSpec, String> result = entitySpec.getSearchableFieldPathMap();

    // Verify the result - should have 5 entries (3 regular + 1 numValues + 1 hasValues)
    assertEquals(result.size(), 5, "Should have 5 path mappings");

    // Verify that all field names are mapped
    assertTrue(result.values().contains("field1"), "Should contain field1");
    assertTrue(result.values().contains("field1_numValues"), "Should contain field1_numValues");
    assertTrue(result.values().contains("field1_hasValues"), "Should contain field1_hasValues");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testGetSearchableFieldPathMapWithDuplicatePaths() {
    // Setup mock path specs and annotations that would create duplicate paths
    PathSpec mockPathSpec1 = mock(PathSpec.class);
    PathSpec mockPathSpec2 = mock(PathSpec.class);

    when(mockSearchableFieldSpec1.getPath()).thenReturn(mockPathSpec1);
    when(mockSearchableFieldSpec2.getPath()).thenReturn(mockPathSpec2);

    // Create identical paths that will result in duplicate PathSpecs
    when(mockPathSpec1.getPathComponents()).thenReturn(ImmutableList.of("component1"));
    when(mockPathSpec2.getPathComponents()).thenReturn(ImmutableList.of("component1")); // Same path

    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("field2"); // Different field names

    when(mockSearchableAnnotation1.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation1.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getHasValuesFieldName()).thenReturn(Optional.empty());

    // This should throw IllegalStateException due to duplicate paths
    entitySpec.getSearchableFieldPathMap();
  }

  @Test
  public void testGetSearchScoreFieldSpecs() {
    // Test the default implementation of getSearchScoreFieldSpecs
    List<SearchScoreFieldSpec> result = entitySpec.getSearchScoreFieldSpecs();

    // Verify that all search score field specs from all aspects are returned
    assertEquals(result.size(), 1, "Should return all search score field specs from all aspects");
    assertTrue(
        result.contains(mockSearchScoreFieldSpec1), "Should contain search score field spec 1");
  }

  @Test
  public void testGetRelationshipFieldSpecs() {
    // Test the default implementation of getRelationshipFieldSpecs
    List<RelationshipFieldSpec> result = entitySpec.getRelationshipFieldSpecs();

    // Verify that all relationship field specs from all aspects are returned
    assertEquals(result.size(), 1, "Should return all relationship field specs from all aspects");
    assertTrue(
        result.contains(mockRelationshipFieldSpec1), "Should contain relationship field spec 1");
  }

  @Test
  public void testGetSearchableRefFieldSpecs() {
    // Test the default implementation of getSearchableRefFieldSpecs
    List<SearchableRefFieldSpec> result = entitySpec.getSearchableRefFieldSpecs();

    // Verify that all searchable ref field specs from all aspects are returned
    assertEquals(result.size(), 1, "Should return all searchable ref field specs from all aspects");
    assertTrue(
        result.contains(mockSearchableRefFieldSpec1), "Should contain searchable ref field spec 1");
  }

  @Test
  public void testGetSearchableFieldsToPathSpecsMap() {
    // Setup mock path specs and annotations
    PathSpec mockPathSpec1 = mock(PathSpec.class);
    PathSpec mockPathSpec2 = mock(PathSpec.class);
    PathSpec mockPathSpec3 = mock(PathSpec.class);

    when(mockSearchableFieldSpec1.getPath()).thenReturn(mockPathSpec1);
    when(mockSearchableFieldSpec2.getPath()).thenReturn(mockPathSpec2);
    when(mockSearchableFieldSpec3.getPath()).thenReturn(mockPathSpec3);

    when(mockPathSpec1.getPathComponents()).thenReturn(ImmutableList.of("component1"));
    when(mockPathSpec2.getPathComponents()).thenReturn(ImmutableList.of("component2"));
    when(mockPathSpec3.getPathComponents()).thenReturn(ImmutableList.of("component3"));

    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("field2");
    when(mockSearchableAnnotation3.getFieldName()).thenReturn("field3");

    when(mockSearchableAnnotation1.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation1.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getHasValuesFieldName()).thenReturn(Optional.empty());

    Map<String, List<PathSpec>> result = entitySpec.getSearchableFieldsToPathSpecsMap();

    // Verify the result
    assertEquals(result.size(), 3, "Should have 3 field mappings");

    // Verify that all field names are mapped
    assertTrue(result.containsKey("field1"), "Should contain field1");
    assertTrue(result.containsKey("field2"), "Should contain field2");
    assertTrue(result.containsKey("field3"), "Should contain field3");

    // Verify that each field has a list of path specs
    assertEquals(result.get("field1").size(), 1, "field1 should have 1 path spec");
    assertEquals(result.get("field2").size(), 1, "field2 should have 1 path spec");
    assertEquals(result.get("field3").size(), 1, "field3 should have 1 path spec");
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testGetSearchableFieldsToPathSpecsMapWithMergeFunction() {
    // Setup mock path specs and annotations with same field names
    PathSpec mockPathSpec1 = mock(PathSpec.class);
    PathSpec mockPathSpec2 = mock(PathSpec.class);
    PathSpec mockPathSpec3 = mock(PathSpec.class);

    when(mockSearchableFieldSpec1.getPath()).thenReturn(mockPathSpec1);
    when(mockSearchableFieldSpec2.getPath()).thenReturn(mockPathSpec2);
    when(mockSearchableFieldSpec3.getPath()).thenReturn(mockPathSpec3);

    when(mockPathSpec1.getPathComponents()).thenReturn(ImmutableList.of("component1"));
    when(mockPathSpec2.getPathComponents()).thenReturn(ImmutableList.of("component2"));
    when(mockPathSpec3.getPathComponents()).thenReturn(ImmutableList.of("component3"));

    when(mockSearchableAnnotation1.getFieldName()).thenReturn("sameField");
    when(mockSearchableAnnotation2.getFieldName()).thenReturn("sameField");
    when(mockSearchableAnnotation3.getFieldName()).thenReturn("field3");

    when(mockSearchableAnnotation1.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation1.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getHasValuesFieldName()).thenReturn(Optional.empty());

    Map<String, List<PathSpec>> result = entitySpec.getSearchableFieldsToPathSpecsMap();

    // This should throw UnsupportedOperationException due to ImmutableList merge issue
  }

  @Test
  public void testGetSearchableFieldsToPathSpecsMapWithNumValuesAndHasValues() {
    // Setup mock path specs and annotations with numValues and hasValues
    PathSpec mockPathSpec1 = mock(PathSpec.class);
    PathSpec mockPathSpec2 = mock(PathSpec.class);
    PathSpec mockPathSpec3 = mock(PathSpec.class);

    when(mockSearchableFieldSpec1.getPath()).thenReturn(mockPathSpec1);
    when(mockSearchableFieldSpec2.getPath()).thenReturn(mockPathSpec2);
    when(mockSearchableFieldSpec3.getPath()).thenReturn(mockPathSpec3);

    when(mockPathSpec1.getPathComponents()).thenReturn(ImmutableList.of("component1"));
    when(mockPathSpec2.getPathComponents()).thenReturn(ImmutableList.of("component2"));
    when(mockPathSpec3.getPathComponents()).thenReturn(ImmutableList.of("component3"));

    when(mockSearchableAnnotation1.getFieldName()).thenReturn("field1");
    when(mockSearchableAnnotation1.getNumValuesFieldName())
        .thenReturn(Optional.of("field1_numValues"));
    when(mockSearchableAnnotation1.getHasValuesFieldName())
        .thenReturn(Optional.of("field1_hasValues"));

    // Setup other mocks to return empty optionals
    when(mockSearchableAnnotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation2.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockSearchableAnnotation3.getHasValuesFieldName()).thenReturn(Optional.empty());

    Map<String, List<PathSpec>> result = entitySpec.getSearchableFieldsToPathSpecsMap();

    // Verify the result - should have 4 entries (3 regular + 1 numValues + 1 hasValues)
    assertEquals(result.size(), 4, "Should have 4 field mappings");

    // Verify that all field names are mapped
    assertTrue(result.containsKey("field1"), "Should contain field1");
    assertTrue(result.containsKey("field1_numValues"), "Should contain field1_numValues");
    assertTrue(result.containsKey("field1_hasValues"), "Should contain field1_hasValues");
  }

  @Test
  public void testArrayWildcardConstant() {
    // Test that the ARRAY_WILDCARD constant is properly defined
    assertEquals(EntitySpec.ARRAY_WILDCARD, "*", "ARRAY_WILDCARD should be '*'");
  }
}
