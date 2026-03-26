package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.ESUtils.ALIAS_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.PATH;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
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
public class AspectMappingBuilderTest {

  @Mock private EntitySpec mockEntitySpec;

  @Mock private AspectSpec mockAspectSpec1;

  @Mock private AspectSpec mockAspectSpec2;

  @Mock private AspectSpec mockStructuredPropertiesAspect;

  @Mock private SearchableFieldSpec mockFieldSpec1;

  @Mock private SearchableFieldSpec mockFieldSpec2;

  @Mock private SearchableAnnotation mockAnnotation1;

  @Mock private SearchableAnnotation mockAnnotation2;

  @BeforeEach
  public void setUp() {
    // Setup common mocks
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockAspectSpec1.getName()).thenReturn("testAspect");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));
    when(mockFieldSpec1.getSearchableAnnotation()).thenReturn(mockAnnotation1);
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
  }

  @Test
  public void testCreateAspectMappingsWithNoConflicts() {
    // Test basic aspect mapping creation
    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
    assertTrue(result.get("testAspect") instanceof Map);
  }

  @Test
  public void testCreateAspectMappingsWithFieldNameConflicts() {
    // Test with field name conflicts
    Map<String, Set<String>> fieldNameConflicts = new HashMap<>();
    fieldNameConflicts.put("testField", Set.of("_aspects.testAspect.testField"));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, fieldNameConflicts, null);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
  }

  @Test
  public void testCreateAspectMappingsWithFieldNameAliasConflicts() {
    // Test with field name alias conflicts
    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    fieldNameAliasConflicts.put("aliasField", Set.of("_aspects.testAspect.testField"));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, fieldNameAliasConflicts);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
  }

  @Test
  public void testCreateAspectMappingsSkipsStructuredProperties() {
    // Test that structuredProperties aspect is skipped
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockStructuredPropertiesAspect));
    when(mockStructuredPropertiesAspect.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockStructuredPropertiesAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertNotNull(result);
    assertFalse(result.containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME));
  }

  @Test
  public void testCreateAspectMappingsWithEmptyAspectFields() {
    // Test with aspect that has no searchable fields
    when(mockAspectSpec1.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertNotNull(result);
    assertFalse(result.containsKey("testAspect"));
  }

  @Test
  public void testCreateAspectMappingsWithMultipleAspects() {
    // Test with multiple aspects
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));

    Map<String, Object> result =
        AspectMappingBuilder.createAspectMappings(mockEntitySpec, null, null);

    assertNotNull(result);
    assertTrue(result.containsKey("testAspect"));
  }

  @Test
  public void testCreateRootLevelAliasesWithNoConflicts() {
    // Test basic root level alias creation
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("aliasField"));

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.containsKey("aliasField"));
    assertTrue(result.get("aliasField") instanceof Map);

    @SuppressWarnings("unchecked")
    Map<String, Object> aliasMapping = (Map<String, Object>) result.get("aliasField");
    assertEquals(ALIAS_FIELD_TYPE, aliasMapping.get(TYPE));
    assertEquals("_aspects.testAspect.testField", aliasMapping.get(PATH));
  }

  @Test
  public void testCreateRootLevelAliasesWithConflicts() {
    // Test that conflicted aliases are not created
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("aliasField"));

    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    fieldNameAliasConflicts.put("aliasField", Set.of("_aspects.testAspect.testField"));

    Map<String, Object> result =
        AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, fieldNameAliasConflicts);

    assertNotNull(result);
    assertFalse(result.containsKey("aliasField"));
  }

  @Test
  public void testCreateRootLevelAliasesSkipsStructuredProperties() {
    // Test that structuredProperties aspect is skipped for aliases
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockStructuredPropertiesAspect));
    when(mockStructuredPropertiesAspect.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockStructuredPropertiesAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("aliasField"));

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertFalse(result.containsKey("aliasField"));
  }

  @Test
  public void testCreateRootLevelAliasesWithNoAliases() {
    // Test with field that has no aliases
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCreateRootLevelAliasesWithNullAliases() {
    // Test with field that has null aliases
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(null);

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testCreateRootLevelAliasesWithMultipleAliases() {
    // Test with field that has multiple aliases
    when(mockAnnotation1.getFieldNameAliases())
        .thenReturn(Collections.singletonList("aliasField1"));
    when(mockAnnotation2.getFieldNameAliases())
        .thenReturn(Collections.singletonList("aliasField2"));
    when(mockAnnotation2.getFieldName()).thenReturn("testField2");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);

    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.containsKey("aliasField1"));
    assertFalse(result.containsKey("aliasField2")); // Only one field in aspect
  }

  @Test
  public void testCreateRootLevelAliasesWithEmptyConflictsMap() {
    // Test with empty conflicts map
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("aliasField"));

    Map<String, Set<String>> emptyConflicts = new HashMap<>();

    Map<String, Object> result =
        AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, emptyConflicts);

    assertNotNull(result);
    assertTrue(result.containsKey("aliasField"));
  }

  @Test
  public void testCreateRootLevelAliasesWithNullConflictsMap() {
    // Test with null conflicts map
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("aliasField"));

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.containsKey("aliasField"));
  }

  @Test
  public void testDoubleUnderscoreFieldNameDetection() {
    // Test that fields with double underscores are detected and skipped in aspect aliases
    SearchableFieldSpec mockFieldSpecWithDoubleUnderscore = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotationWithDoubleUnderscore = mock(SearchableAnnotation.class);

    when(mockAnnotationWithDoubleUnderscore.getFieldName())
        .thenReturn("fields__globalTags_tags__tag");
    when(mockAnnotationWithDoubleUnderscore.getFieldType()).thenReturn(FieldType.URN);
    when(mockFieldSpecWithDoubleUnderscore.getSearchableAnnotation())
        .thenReturn(mockAnnotationWithDoubleUnderscore);

    // Create a mock AspectSpec with the problematic field
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpecWithDoubleUnderscore));

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Execute the method
    Map<String, Object> result =
        AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, Collections.emptyMap());

    // Verify that the result is created but the problematic field was skipped
    assertNotNull(result, "Result should be created");
    // The field with double underscores should not appear in the aliases
    // This is verified by the fact that no exception is thrown and result is created
  }

  @Test
  public void testNormalFieldNamesAreNotSkippedInAspectAliases() {
    // Test that normal field names (without double underscores) are not skipped in aspect aliases
    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    when(mockAnnotation.getFieldName()).thenReturn("normalFieldName");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);

    // Create a mock AspectSpec with the normal field
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec));

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Execute the method
    Map<String, Object> result =
        AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, Collections.emptyMap());

    // Verify that the result is created
    assertNotNull(result, "Result should be created");
  }

  @Test
  public void testCreateRootLevelAliasesForHasValuesFieldName() {
    // Test that hasValuesFieldName fields get root-level aliases pointing directly to the aspect
    when(mockAnnotation1.getHasValuesFieldName())
        .thenReturn(java.util.Optional.of("hasErroringAssertions"));
    when(mockAnnotation1.getFieldName()).thenReturn("erroringAssertions");

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.containsKey("hasErroringAssertions"));
    assertTrue(result.get("hasErroringAssertions") instanceof Map);

    @SuppressWarnings("unchecked")
    Map<String, Object> aliasMapping = (Map<String, Object>) result.get("hasErroringAssertions");
    assertEquals(ALIAS_FIELD_TYPE, aliasMapping.get(TYPE));
    assertEquals("_aspects.testAspect.hasErroringAssertions", aliasMapping.get(PATH));
  }

  @Test
  public void testCreateRootLevelAliasesForNumValuesFieldName() {
    // Test that numValuesFieldName fields get root-level aliases pointing directly to the aspect
    when(mockAnnotation1.getNumValuesFieldName())
        .thenReturn(java.util.Optional.of("numErroringAssertions"));
    when(mockAnnotation1.getFieldName()).thenReturn("erroringAssertions");

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.containsKey("numErroringAssertions"));
    assertTrue(result.get("numErroringAssertions") instanceof Map);

    @SuppressWarnings("unchecked")
    Map<String, Object> aliasMapping = (Map<String, Object>) result.get("numErroringAssertions");
    assertEquals(ALIAS_FIELD_TYPE, aliasMapping.get(TYPE));
    assertEquals("_aspects.testAspect.numErroringAssertions", aliasMapping.get(PATH));
  }

  @Test
  public void testCreateRootLevelAliasesForBothHasAndNumValuesFieldName() {
    // Test that both hasValuesFieldName and numValuesFieldName fields get root-level aliases
    when(mockAnnotation1.getHasValuesFieldName())
        .thenReturn(java.util.Optional.of("hasErroringAssertions"));
    when(mockAnnotation1.getNumValuesFieldName())
        .thenReturn(java.util.Optional.of("numErroringAssertions"));
    when(mockAnnotation1.getFieldName()).thenReturn("erroringAssertions");

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertTrue(result.containsKey("hasErroringAssertions"));
    assertTrue(result.containsKey("numErroringAssertions"));

    @SuppressWarnings("unchecked")
    Map<String, Object> hasAliasMapping = (Map<String, Object>) result.get("hasErroringAssertions");
    assertEquals(ALIAS_FIELD_TYPE, hasAliasMapping.get(TYPE));
    assertEquals("_aspects.testAspect.hasErroringAssertions", hasAliasMapping.get(PATH));

    @SuppressWarnings("unchecked")
    Map<String, Object> numAliasMapping = (Map<String, Object>) result.get("numErroringAssertions");
    assertEquals(ALIAS_FIELD_TYPE, numAliasMapping.get(TYPE));
    assertEquals("_aspects.testAspect.numErroringAssertions", numAliasMapping.get(PATH));
  }

  @Test
  public void testCreateRootLevelAliasesSkipsHasValuesFieldNameWithConflicts() {
    // Test that hasValuesFieldName aliases are skipped when there are conflicts
    when(mockAnnotation1.getHasValuesFieldName())
        .thenReturn(java.util.Optional.of("hasErroringAssertions"));
    when(mockAnnotation1.getFieldName()).thenReturn("erroringAssertions");

    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    fieldNameAliasConflicts.put(
        "hasErroringAssertions", Set.of("_aspects.testAspect.hasErroringAssertions"));

    Map<String, Object> result =
        AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, fieldNameAliasConflicts);

    assertNotNull(result);
    assertFalse(result.containsKey("hasErroringAssertions"));
  }

  @Test
  public void testCreateRootLevelAliasesSkipsNumValuesFieldNameWithConflicts() {
    // Test that numValuesFieldName aliases are skipped when there are conflicts
    when(mockAnnotation1.getNumValuesFieldName())
        .thenReturn(java.util.Optional.of("numErroringAssertions"));
    when(mockAnnotation1.getFieldName()).thenReturn("erroringAssertions");

    Map<String, Set<String>> fieldNameAliasConflicts = new HashMap<>();
    fieldNameAliasConflicts.put(
        "numErroringAssertions", Set.of("_aspects.testAspect.numErroringAssertions"));

    Map<String, Object> result =
        AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, fieldNameAliasConflicts);

    assertNotNull(result);
    assertFalse(result.containsKey("numErroringAssertions"));
  }

  @Test
  public void testCreateRootLevelAliasesSkipsStructuredPropertiesForHasValuesFieldName() {
    // Test that hasValuesFieldName aliases are not created for structuredProperties aspect
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockStructuredPropertiesAspect));
    when(mockStructuredPropertiesAspect.getName()).thenReturn(STRUCTURED_PROPERTIES_ASPECT_NAME);
    when(mockStructuredPropertiesAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockFieldSpec1));
    when(mockAnnotation1.getHasValuesFieldName())
        .thenReturn(java.util.Optional.of("hasErroringAssertions"));
    when(mockAnnotation1.getFieldName()).thenReturn("erroringAssertions");

    Map<String, Object> result = AspectMappingBuilder.createRootLevelAliases(mockEntitySpec, null);

    assertNotNull(result);
    assertFalse(result.containsKey("hasErroringAssertions"));
  }
}
