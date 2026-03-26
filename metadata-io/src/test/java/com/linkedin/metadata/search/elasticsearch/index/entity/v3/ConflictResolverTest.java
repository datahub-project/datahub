package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ConflictResolverTest {

  @Mock private EntitySpec mockEntitySpec1;

  @Mock private EntitySpec mockEntitySpec2;

  @Mock private AspectSpec mockAspectSpec1;

  @Mock private AspectSpec mockAspectSpec2;

  @Mock private SearchableFieldSpec mockFieldSpec1;

  @Mock private SearchableFieldSpec mockFieldSpec2;

  @Mock private SearchableFieldSpec mockFieldSpec3;

  @Mock private SearchableAnnotation mockAnnotation1;

  @Mock private SearchableAnnotation mockAnnotation2;

  @Mock private SearchableAnnotation mockAnnotation3;

  @BeforeEach
  public void setUp() {
    // Setup common mocks
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockAspectSpec1.getName()).thenReturn("aspect1");
    when(mockAspectSpec2.getName()).thenReturn("aspect2");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec2));
    when(mockFieldSpec1.getSearchableAnnotation()).thenReturn(mockAnnotation1);
    when(mockFieldSpec2.getSearchableAnnotation()).thenReturn(mockAnnotation2);
    when(mockAnnotation1.getFieldName()).thenReturn("field1");
    when(mockAnnotation2.getFieldName()).thenReturn("field2");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
  }

  @Test
  public void testDetectConflictsWithNoConflicts() {
    // Test with no conflicts
    Collection<EntitySpec> entitySpecs = Collections.singletonList(mockEntitySpec1);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertFalse(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().isEmpty());
    assertTrue(result.getFieldNameAliasConflicts().isEmpty());
  }

  @Test
  public void testDetectConflictsWithFieldNameConflicts() {
    // Test with field name conflicts (same field name in different aspects)
    when(mockAnnotation2.getFieldName()).thenReturn("field1"); // Same field name
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());

    Collection<EntitySpec> entitySpecs = Arrays.asList(mockEntitySpec1, mockEntitySpec2);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertTrue(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().containsKey("field1"));
    assertTrue(result.getFieldNameAliasConflicts().isEmpty());

    Set<String> field1Paths = result.getFieldNameConflicts().get("field1");
    assertEquals(2, field1Paths.size());
    assertTrue(field1Paths.contains("_aspects.aspect1.field1"));
    assertTrue(field1Paths.contains("_aspects.aspect2.field1"));
  }

  @Test
  public void testDetectConflictsWithFieldNameAliasConflicts() {
    // Test with field name alias conflicts (same alias in different aspects)
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("alias1"));
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.singletonList("alias1"));

    Collection<EntitySpec> entitySpecs = Arrays.asList(mockEntitySpec1, mockEntitySpec2);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertTrue(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().isEmpty());
    assertTrue(result.getFieldNameAliasConflicts().containsKey("alias1"));

    Set<String> alias1Paths = result.getFieldNameAliasConflicts().get("alias1");
    assertEquals(2, alias1Paths.size());
    assertTrue(alias1Paths.contains("_aspects.aspect1.field1"));
    assertTrue(alias1Paths.contains("_aspects.aspect2.field2"));
  }

  @Test
  public void testDetectConflictsWithFieldNameAndAliasConflicts() {
    // Test with conflicts between field names and field name aliases
    when(mockAnnotation1.getFieldName()).thenReturn("conflictingName");
    when(mockAnnotation2.getFieldName()).thenReturn("otherField");
    when(mockAnnotation2.getFieldNameAliases())
        .thenReturn(Collections.singletonList("conflictingName"));

    Collection<EntitySpec> entitySpecs = Arrays.asList(mockEntitySpec1, mockEntitySpec2);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertTrue(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().containsKey("conflictingName"));
    assertTrue(result.getFieldNameAliasConflicts().containsKey("conflictingName"));

    // Both should have the same paths
    Set<String> fieldNamePaths = result.getFieldNameConflicts().get("conflictingName");
    Set<String> aliasPaths = result.getFieldNameAliasConflicts().get("conflictingName");
    assertEquals(fieldNamePaths, aliasPaths);
    assertEquals(2, fieldNamePaths.size());
    assertTrue(fieldNamePaths.contains("_aspects.aspect1.conflictingName"));
    assertTrue(fieldNamePaths.contains("_aspects.aspect2.otherField"));
  }

  @Test
  public void testDetectConflictsWithMultipleAliases() {
    // Test with multiple aliases on same field
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Arrays.asList("alias1", "alias2"));

    Collection<EntitySpec> entitySpecs = Collections.singletonList(mockEntitySpec1);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertFalse(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().isEmpty());
    assertTrue(result.getFieldNameAliasConflicts().isEmpty());
  }

  @Test
  public void testDetectConflictsWithNullAliases() {
    // Test with null aliases
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(null);

    Collection<EntitySpec> entitySpecs = Collections.singletonList(mockEntitySpec1);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertFalse(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().isEmpty());
    assertTrue(result.getFieldNameAliasConflicts().isEmpty());
  }

  @Test
  public void testDetectConflictsWithEmptyAliases() {
    // Test with empty aliases
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());

    Collection<EntitySpec> entitySpecs = Collections.singletonList(mockEntitySpec1);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertFalse(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().isEmpty());
    assertTrue(result.getFieldNameAliasConflicts().isEmpty());
  }

  @Test
  public void testDetectConflictsWithMultipleFieldsInSameAspect() {
    // Test with multiple fields in same aspect
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Arrays.asList(mockFieldSpec1, mockFieldSpec3));
    when(mockFieldSpec3.getSearchableAnnotation()).thenReturn(mockAnnotation3);
    when(mockAnnotation3.getFieldName()).thenReturn("field3");
    when(mockAnnotation3.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation3.getFieldNameAliases()).thenReturn(Collections.emptyList());

    Collection<EntitySpec> entitySpecs = Collections.singletonList(mockEntitySpec1);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertFalse(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().isEmpty());
    assertTrue(result.getFieldNameAliasConflicts().isEmpty());
  }

  @Test
  public void testDetectConflictsWithComplexScenario() {
    // Test with complex scenario: multiple entities, aspects, and conflicts
    when(mockAnnotation1.getFieldName()).thenReturn("commonField");
    when(mockAnnotation2.getFieldName()).thenReturn("commonField");
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("alias1"));
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.singletonList("alias2"));

    Collection<EntitySpec> entitySpecs = Arrays.asList(mockEntitySpec1, mockEntitySpec2);

    ConflictResolver.ConflictResult result = ConflictResolver.detectConflicts(entitySpecs);

    assertNotNull(result);
    assertTrue(result.hasConflicts());
    assertTrue(result.getFieldNameConflicts().containsKey("commonField"));
    assertTrue(result.getFieldNameAliasConflicts().isEmpty()); // No alias conflicts

    Set<String> commonFieldPaths = result.getFieldNameConflicts().get("commonField");
    assertEquals(2, commonFieldPaths.size());
    assertTrue(commonFieldPaths.contains("_aspects.aspect1.commonField"));
    assertTrue(commonFieldPaths.contains("_aspects.aspect2.commonField"));
  }

  @Test
  public void testCollectFieldNameAliasPaths() {
    // Test collecting field name alias paths from entity spec
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Arrays.asList("alias1", "alias2"));

    Map<String, Set<String>> result = ConflictResolver.collectFieldNameAliasPaths(mockEntitySpec1);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.containsKey("alias1"));
    assertTrue(result.containsKey("alias2"));
    assertTrue(result.get("alias1").contains("_aspects.aspect1.field1"));
    assertTrue(result.get("alias2").contains("_aspects.aspect1.field1"));
  }

  @Test
  public void testCollectFieldNameAliasPathsWithNullAliases() {
    // Test with null aliases
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(null);

    Map<String, Set<String>> result = ConflictResolver.collectFieldNameAliasPaths(mockEntitySpec1);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCollectFieldNameAliasPathsWithEmptyAliases() {
    // Test with empty aliases
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());

    Map<String, Set<String>> result = ConflictResolver.collectFieldNameAliasPaths(mockEntitySpec1);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCollectFieldNameAliasPathsWithMultipleAspects() {
    // Test with multiple aspects
    when(mockEntitySpec1.getAspectSpecs())
        .thenReturn(Arrays.asList(mockAspectSpec1, mockAspectSpec2));
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("alias1"));
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.singletonList("alias2"));

    Map<String, Set<String>> result = ConflictResolver.collectFieldNameAliasPaths(mockEntitySpec1);

    assertNotNull(result);
    assertEquals(2, result.size());
    assertTrue(result.containsKey("alias1"));
    assertTrue(result.containsKey("alias2"));
    assertTrue(result.get("alias1").contains("_aspects.aspect1.field1"));
    assertTrue(result.get("alias2").contains("_aspects.aspect2.field2"));
  }

  @Test
  public void testConflictResultConstructor() {
    // Test ConflictResult constructor and getters
    Map<String, Set<String>> fieldNameConflicts = new HashMap<>();
    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    fieldNameConflicts.put("field1", Set.of("path1", "path2"));
    fieldNameAliasConflicts.put("alias1", Set.of("path3", "path4"));

    ConflictResolver.ConflictResult result =
        new ConflictResolver.ConflictResult(
            fieldNameConflicts, fieldNameAliasConflicts, new HashMap<>(), new HashMap<>());

    assertNotNull(result);
    assertEquals(fieldNameConflicts, result.getFieldNameConflicts());
    assertEquals(fieldNameAliasConflicts, result.getFieldNameAliasConflicts());
    assertTrue(result.hasConflicts());
  }

  @Test
  public void testConflictResultHasConflicts() {
    // Test hasConflicts method
    Map<String, Set<String>> emptyConflicts = new HashMap<>();
    Map<String, Set<String>> nonEmptyConflicts = new HashMap<>();
    nonEmptyConflicts.put("field1", Set.of("path1"));

    ConflictResolver.ConflictResult emptyResult =
        new ConflictResolver.ConflictResult(
            emptyConflicts, emptyConflicts, new HashMap<>(), new HashMap<>());
    ConflictResolver.ConflictResult nonEmptyResult =
        new ConflictResolver.ConflictResult(
            nonEmptyConflicts, emptyConflicts, new HashMap<>(), new HashMap<>());

    assertFalse(emptyResult.hasConflicts());
    assertTrue(nonEmptyResult.hasConflicts());
  }
}
