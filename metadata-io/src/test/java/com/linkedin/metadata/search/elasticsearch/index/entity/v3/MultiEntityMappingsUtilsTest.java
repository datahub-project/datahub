package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.search.utils.ESUtils.ALIAS_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.PATH;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.Test;

public class MultiEntityMappingsUtilsTest {

  @Test
  public void testCreateEntityNameAliasMapping() {
    Map<String, Object> result = MultiEntityMappingsUtils.createEntityNameAliasMapping();

    assertEquals(result.size(), 2);
    assertEquals(result.get(TYPE), ALIAS_FIELD_TYPE);
    assertEquals(result.get(PATH), "_search.entityName");
  }

  @Test
  public void testIsEntityNameField() {
    assertTrue(MultiEntityMappingsUtils.isEntityNameField("_entityName"));
    assertFalse(MultiEntityMappingsUtils.isEntityNameField("_qualifiedName"));
    assertFalse(MultiEntityMappingsUtils.isEntityNameField("name"));
    assertFalse(MultiEntityMappingsUtils.isEntityNameField(""));
    assertFalse(MultiEntityMappingsUtils.isEntityNameField(null));
  }

  @Test
  public void testCreateQualifiedNameAliasMapping() {
    Map<String, Object> result = MultiEntityMappingsUtils.createQualifiedNameAliasMapping();

    assertEquals(result.size(), 2);
    assertEquals(result.get(TYPE), ALIAS_FIELD_TYPE);
    assertEquals(result.get(PATH), "_search.qualifiedName");
  }

  @Test
  public void testIsQualifiedNameField() {
    assertTrue(MultiEntityMappingsUtils.isQualifiedNameField("_qualifiedName"));
    assertFalse(MultiEntityMappingsUtils.isQualifiedNameField("_entityName"));
    assertFalse(MultiEntityMappingsUtils.isQualifiedNameField("qualifiedName"));
    assertFalse(MultiEntityMappingsUtils.isQualifiedNameField(""));
    assertFalse(MultiEntityMappingsUtils.isQualifiedNameField(null));
  }

  @Test
  public void testCreateAliasMapping() {
    String targetPath = "_search.custom_field";
    Map<String, Object> result = MultiEntityMappingsUtils.createAliasMapping(targetPath);

    assertEquals(result.size(), 2);
    assertEquals(result.get(TYPE), ALIAS_FIELD_TYPE);
    assertEquals(result.get(PATH), targetPath);
  }

  @Test
  public void testHasOtherCopyToDestinationsWithSortLabel() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Collections.emptyList(), "searchLabel", null, null);

    assertTrue(MultiEntityMappingsUtils.hasOtherCopyToDestinations(fieldSpec));
  }

  @Test
  public void testHasOtherCopyToDestinationsWithBoostLabel() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Collections.emptyList(), "searchLabel", null, null);

    assertTrue(MultiEntityMappingsUtils.hasOtherCopyToDestinations(fieldSpec));
  }

  @Test
  public void testHasOtherCopyToDestinationsWithEntityFieldName() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Collections.emptyList(), null, "entityFieldName", null);

    assertTrue(MultiEntityMappingsUtils.hasOtherCopyToDestinations(fieldSpec));
  }

  @Test
  public void testHasOtherCopyToDestinationsWithMultipleLabels() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD,
            "testField",
            Collections.emptyList(),
            "searchLabel",
            "entityFieldName",
            null);

    assertTrue(MultiEntityMappingsUtils.hasOtherCopyToDestinations(fieldSpec));
  }

  @Test
  public void testHasOtherCopyToDestinationsWithoutLabels() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Collections.emptyList(), null, null, null);

    assertFalse(MultiEntityMappingsUtils.hasOtherCopyToDestinations(fieldSpec));
  }

  @Test
  public void testHasOtherCopyToDestinationsWithEmptyLabels() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Collections.emptyList(), "", "", null);

    assertFalse(MultiEntityMappingsUtils.hasOtherCopyToDestinations(fieldSpec));
  }

  @Test
  public void testHasFieldAliasesWithAliases() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Arrays.asList("alias1", "alias2"), null, null, null);

    assertTrue(MultiEntityMappingsUtils.hasFieldAliases(fieldSpec));
  }

  @Test
  public void testHasFieldAliasesWithoutAliases() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(
            FieldType.KEYWORD, "testField", Collections.emptyList(), null, null, null);

    assertFalse(MultiEntityMappingsUtils.hasFieldAliases(fieldSpec));
  }

  @Test
  public void testHasFieldAliasesWithNullAliases() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpec(FieldType.KEYWORD, "testField", null, null, null, null);

    assertFalse(MultiEntityMappingsUtils.hasFieldAliases(fieldSpec));
  }

  @Test
  public void testFindFieldTypeForFieldName() {
    // Create mock entity specs
    EntitySpec entitySpec =
        createMockEntitySpec("dataset", "DatasetProperties", "name", FieldType.WORD_GRAM);
    Collection<EntitySpec> entitySpecs = Collections.singletonList(entitySpec);

    FieldType result =
        MultiEntityMappingsUtils.findFieldTypeForFieldName(
            entitySpecs, "name", "DatasetProperties");

    assertEquals(result, FieldType.WORD_GRAM);
  }

  @Test
  public void testFindFieldTypeForFieldNameNotFound() {
    // Create mock entity specs with different field
    EntitySpec entitySpec =
        createMockEntitySpec("dataset", "DatasetProperties", "description", FieldType.TEXT);
    Collection<EntitySpec> entitySpecs = Collections.singletonList(entitySpec);

    FieldType result =
        MultiEntityMappingsUtils.findFieldTypeForFieldName(
            entitySpecs, "name", "DatasetProperties");

    // Should default to KEYWORD when not found
    assertEquals(result, FieldType.KEYWORD);
  }

  @Test
  public void testFindFieldTypeForFieldNameWrongAspect() {
    // Create mock entity specs with different aspect
    EntitySpec entitySpec =
        createMockEntitySpec("dataset", "DatasetProperties", "name", FieldType.WORD_GRAM);
    Collection<EntitySpec> entitySpecs = Collections.singletonList(entitySpec);

    FieldType result =
        MultiEntityMappingsUtils.findFieldTypeForFieldName(entitySpecs, "name", "ChartInfo");

    // Should default to KEYWORD when aspect not found
    assertEquals(result, FieldType.KEYWORD);
  }

  @Test
  public void testFindFieldTypeForFieldNameMultipleEntities() {
    // Create multiple entity specs with same field name but different types
    EntitySpec entitySpec1 =
        createMockEntitySpec("dataset", "DatasetProperties", "name", FieldType.WORD_GRAM);
    EntitySpec entitySpec2 = createMockEntitySpec("chart", "ChartInfo", "name", FieldType.TEXT);
    Collection<EntitySpec> entitySpecs = Arrays.asList(entitySpec1, entitySpec2);

    FieldType result =
        MultiEntityMappingsUtils.findFieldTypeForFieldName(
            entitySpecs, "name", "DatasetProperties");

    assertEquals(result, FieldType.WORD_GRAM);
  }

  // Helper method to create SearchableFieldSpec for testing
  private SearchableFieldSpec createSearchableFieldSpec(
      FieldType fieldType,
      String fieldName,
      Collection<String> fieldNameAliases,
      String searchLabel,
      String entityFieldName,
      Boolean eagerGlobalOrdinals) {

    Map<String, Object> annotationMap = new HashMap<>();
    annotationMap.put("fieldType", fieldType.name());
    annotationMap.put("fieldName", fieldName);

    if (fieldNameAliases != null && !fieldNameAliases.isEmpty()) {
      annotationMap.put("fieldNameAliases", fieldNameAliases);
    }

    if (searchLabel != null) {
      annotationMap.put("searchLabel", searchLabel);
    }

    if (entityFieldName != null) {
      annotationMap.put("entityFieldName", entityFieldName);
    }

    if (eagerGlobalOrdinals != null) {
      annotationMap.put("eagerGlobalOrdinals", eagerGlobalOrdinals);
    }

    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(annotation.getFieldName()).thenReturn(fieldName);

    when(annotation.getSearchLabel()).thenReturn(Optional.ofNullable(searchLabel));
    when(annotation.getEntityFieldName()).thenReturn(Optional.ofNullable(entityFieldName));
    when(annotation.getEagerGlobalOrdinals()).thenReturn(Optional.ofNullable(eagerGlobalOrdinals));
    when(annotation.getFieldNameAliases())
        .thenReturn(
            fieldNameAliases != null ? new ArrayList<>(fieldNameAliases) : Collections.emptyList());

    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);

    return fieldSpec;
  }

  // Helper method to create mock EntitySpec for testing
  private EntitySpec createMockEntitySpec(
      String entityName, String aspectName, String fieldName, FieldType fieldType) {
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn(entityName);
    when(entitySpec.getSearchGroup()).thenReturn("test");

    // Create the searchable field spec first
    SearchableFieldSpec searchableFieldSpec =
        createSearchableFieldSpec(fieldType, fieldName, null, null, null, null);

    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn(aspectName);
    when(aspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(searchableFieldSpec));

    when(entitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(aspectSpec));

    return entitySpec;
  }

  // Tests for relocated utility methods

  @Test
  public void testGetElasticsearchTypeForFieldWithSearchIndexedTrue() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpecWithSearchIndexed(FieldType.TEXT, "testField", true);

    String result = MultiEntityMappingsUtils.getElasticsearchTypeForField(fieldSpec);

    assertEquals(result, ESUtils.KEYWORD_FIELD_TYPE);
  }

  @Test
  public void testGetElasticsearchTypeForFieldWithSearchIndexedFalse() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpecWithSearchIndexed(FieldType.TEXT, "testField", false);

    String result = MultiEntityMappingsUtils.getElasticsearchTypeForField(fieldSpec);

    // When searchIndexed is false, it should return the original field type (TEXT -> keyword)
    assertEquals(result, ESUtils.KEYWORD_FIELD_TYPE);
  }

  @Test
  public void testGetElasticsearchTypeForFieldWithoutSearchIndexed() {
    SearchableFieldSpec fieldSpec =
        createSearchableFieldSpecWithSearchIndexed(FieldType.KEYWORD, "testField", null);

    String result = MultiEntityMappingsUtils.getElasticsearchTypeForField(fieldSpec);

    assertEquals(result, ESUtils.KEYWORD_FIELD_TYPE);
  }

  @Test
  public void testMergeMappingsWithNullExisting() {
    Map<String, Object> configMappings = new HashMap<>();
    configMappings.put("dynamic", true);
    configMappings.put("properties", new HashMap<>());

    Map<String, Object> result = MultiEntityMappingsUtils.mergeMappings(null, configMappings);

    assertEquals(result, configMappings);
  }

  @Test
  public void testMergeMappingsWithNullConfig() {
    Map<String, Object> existingMappings = new HashMap<>();
    existingMappings.put("dynamic", false);
    existingMappings.put("properties", new HashMap<>());

    Map<String, Object> result = MultiEntityMappingsUtils.mergeMappings(existingMappings, null);

    assertEquals(result, existingMappings);
  }

  @Test
  public void testMergeMappingsWithBothNull() {
    Map<String, Object> result = MultiEntityMappingsUtils.mergeMappings(null, null);

    assertTrue(result.isEmpty());
  }

  @Test
  public void testMergeMappingsWithDynamicTemplates() {
    Map<String, Object> existingMappings = new HashMap<>();
    existingMappings.put("dynamic", false);

    Map<String, Object> configMappings = new HashMap<>();
    configMappings.put(
        "dynamic_templates",
        Arrays.asList(
            Map.of("string_template", Map.of("match", "*", "mapping", Map.of("type", "keyword")))));

    Map<String, Object> result =
        MultiEntityMappingsUtils.mergeMappings(existingMappings, configMappings);

    assertTrue(result.containsKey("dynamic_templates"));
    assertEquals(result.get("dynamic"), false); // existing value preserved
  }

  @Test
  public void testMergeMappingsWithAspects() {
    Map<String, Object> existingMappings = new HashMap<>();
    Map<String, Object> existingProperties = new HashMap<>();
    Map<String, Object> existingAspects = new HashMap<>();
    Map<String, Object> existingAspectsProperties = new HashMap<>();
    existingAspectsProperties.put("existingField", Map.of("type", "keyword"));
    existingAspects.put("properties", existingAspectsProperties);
    existingProperties.put("_aspects", existingAspects);
    existingMappings.put("properties", existingProperties);

    Map<String, Object> configMappings = new HashMap<>();
    Map<String, Object> configProperties = new HashMap<>();
    Map<String, Object> configAspects = new HashMap<>();
    Map<String, Object> configAspectsProperties = new HashMap<>();
    configAspectsProperties.put("newField", Map.of("type", "text"));
    configAspects.put("properties", configAspectsProperties);
    configProperties.put("_aspects", configAspects);
    configMappings.put("properties", configProperties);

    Map<String, Object> result =
        MultiEntityMappingsUtils.mergeMappings(existingMappings, configMappings);

    @SuppressWarnings("unchecked")
    Map<String, Object> resultProperties = (Map<String, Object>) result.get("properties");
    @SuppressWarnings("unchecked")
    Map<String, Object> resultAspects = (Map<String, Object>) resultProperties.get("_aspects");
    @SuppressWarnings("unchecked")
    Map<String, Object> resultAspectsProperties =
        (Map<String, Object>) resultAspects.get("properties");

    assertTrue(resultAspectsProperties.containsKey("existingField"));
    assertTrue(resultAspectsProperties.containsKey("newField"));
  }

  @Test
  public void testBuildSearchSectionWithSortLabel() {
    EntitySpec entitySpec = createMockEntitySpecWithCopyToDestinations();
    Collection<EntitySpec> entitySpecs = Collections.singletonList(entitySpec);

    Map<String, Object> result =
        MultiEntityMappingsUtils.buildSearchSection(entitySpecs, new HashMap<>());

    assertTrue(result.containsKey("dynamic"));
    assertTrue(result.containsKey("properties"));
    assertEquals(result.get("dynamic"), true);

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertTrue(properties.containsKey("name"));
    assertTrue(properties.containsKey("score"));
    // Note: tier_1 field is no longer explicitly created - it will be created dynamically
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuildSearchSectionWithTypeConflicts() {
    // Create entity specs with conflicting types for the same destination field
    // Use BOOLEAN and KEYWORD which map to different Elasticsearch types
    EntitySpec entitySpec1 = createMockEntitySpecWithFieldType("sort_name", FieldType.KEYWORD);
    EntitySpec entitySpec2 = createMockEntitySpecWithFieldType("sort_name", FieldType.BOOLEAN);
    Collection<EntitySpec> entitySpecs = Arrays.asList(entitySpec1, entitySpec2);

    // This should throw an IllegalArgumentException due to type conflicts
    MultiEntityMappingsUtils.buildSearchSection(entitySpecs, new HashMap<>());
  }

  @Test
  public void testBuildSearchSectionWithEmptyEntitySpecs() {
    Collection<EntitySpec> entitySpecs = Collections.emptyList();

    Map<String, Object> result =
        MultiEntityMappingsUtils.buildSearchSection(entitySpecs, new HashMap<>());

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertTrue(
        properties.isEmpty(), "Properties should be empty when no entity specs are provided");
    assertEquals(result.get("dynamic"), true, "Dynamic should be true");
  }

  @Test
  public void testBuildSearchSectionComprehensive() {
    // Create a comprehensive test that directly tests the buildSearchSection utility method
    EntitySpec entitySpec = createMockEntitySpecWithComprehensiveCopyToDestinations();
    Collection<EntitySpec> entitySpecs = Collections.singletonList(entitySpec);

    Map<String, Object> result =
        MultiEntityMappingsUtils.buildSearchSection(entitySpecs, new HashMap<>());

    // Verify basic structure
    assertNotNull(result, "Result should not be null");
    assertTrue(result.containsKey("dynamic"), "Should contain dynamic property");
    assertTrue(result.containsKey("properties"), "Should contain properties");
    assertEquals(result.get("dynamic"), true, "Dynamic should be true");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) result.get("properties");
    assertNotNull(properties, "Properties should not be null");

    // Verify search label destination fields
    assertTrue(properties.containsKey("name"), "Should contain name field");
    assertTrue(properties.containsKey("title"), "Should contain title field");

    @SuppressWarnings("unchecked")
    Map<String, Object> nameField = (Map<String, Object>) properties.get("name");
    assertEquals(nameField.get("type"), "keyword", "name should be keyword type");
    assertEquals(nameField.get("normalizer"), "keyword_normalizer", "name should have normalizer");

    // Verify search label destination fields
    assertTrue(properties.containsKey("score"), "Should contain score field");
    assertTrue(properties.containsKey("relevance"), "Should contain relevance field");

    @SuppressWarnings("unchecked")
    Map<String, Object> scoreField = (Map<String, Object>) properties.get("score");
    assertEquals(scoreField.get("type"), "keyword", "score should be keyword type");
    assertEquals(
        scoreField.get("normalizer"), "keyword_normalizer", "score should have normalizer");

    // Note: tier destination fields are no longer explicitly created in _search section
    // They will be created dynamically when data is copied to them via copy_to

    // Verify entity field name destinations
    assertTrue(properties.containsKey("customField"), "Should contain customField field");

    @SuppressWarnings("unchecked")
    Map<String, Object> customField = (Map<String, Object>) properties.get("customField");
    assertEquals(customField.get("type"), "keyword", "customField should be keyword type");
    assertEquals(
        customField.get("normalizer"), "keyword_normalizer", "customField should have normalizer");
  }

  // Helper methods for testing relocated utility methods

  private SearchableFieldSpec createSearchableFieldSpecWithSearchIndexed(
      FieldType fieldType, String fieldName, Boolean searchIndexed) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(annotation.getFieldName()).thenReturn(fieldName);
    when(annotation.getSearchIndexed()).thenReturn(Optional.ofNullable(searchIndexed));

    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);

    return fieldSpec;
  }

  private EntitySpec createMockEntitySpecWithCopyToDestinations() {
    EntitySpec entitySpec = mock(EntitySpec.class);

    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn("testAspect");

    // Create fields with different copy_to destinations
    SearchableFieldSpec sortField =
        createSearchableFieldSpecWithCopyTo("sortField", FieldType.KEYWORD, "name", null, null);
    SearchableFieldSpec boostField =
        createSearchableFieldSpecWithCopyTo("boostField", FieldType.KEYWORD, "score", null, null);
    SearchableFieldSpec tierField =
        createSearchableFieldSpecWithCopyTo("tierField", FieldType.KEYWORD, null, 1, null);

    when(aspectSpec.getSearchableFieldSpecs())
        .thenReturn(Arrays.asList(sortField, boostField, tierField));
    when(entitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(aspectSpec));

    return entitySpec;
  }

  private EntitySpec createMockEntitySpecWithFieldType(String searchLabel, FieldType fieldType) {
    EntitySpec entitySpec = mock(EntitySpec.class);

    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn("testAspect");

    SearchableFieldSpec field =
        createSearchableFieldSpecWithCopyTo("testField", fieldType, searchLabel, null, null);

    when(aspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field));
    when(entitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(aspectSpec));

    return entitySpec;
  }

  private SearchableFieldSpec createSearchableFieldSpecWithCopyTo(
      String fieldName,
      FieldType fieldType,
      String searchLabel,
      Integer searchTier,
      String entityFieldName) {
    SearchableAnnotation annotation = mock(SearchableAnnotation.class);
    when(annotation.getFieldType()).thenReturn(fieldType);
    when(annotation.getFieldName()).thenReturn(fieldName);

    when(annotation.getSearchLabel()).thenReturn(Optional.ofNullable(searchLabel));
    when(annotation.getSearchTier()).thenReturn(Optional.ofNullable(searchTier));
    when(annotation.getEntityFieldName()).thenReturn(Optional.ofNullable(entityFieldName));
    when(annotation.getSearchIndexed()).thenReturn(Optional.empty());

    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(annotation);

    return fieldSpec;
  }

  private EntitySpec createMockEntitySpecWithComprehensiveCopyToDestinations() {
    EntitySpec entitySpec = mock(EntitySpec.class);

    AspectSpec aspectSpec = mock(AspectSpec.class);
    when(aspectSpec.getName()).thenReturn("testAspect");

    // Create fields with various copy_to destinations
    SearchableFieldSpec sortNameField =
        createSearchableFieldSpecWithCopyTo("sortNameField", FieldType.KEYWORD, "name", null, null);
    SearchableFieldSpec sortTitleField =
        createSearchableFieldSpecWithCopyTo(
            "sortTitleField", FieldType.KEYWORD, "title", null, null);
    SearchableFieldSpec boostScoreField =
        createSearchableFieldSpecWithCopyTo(
            "boostScoreField", FieldType.KEYWORD, "score", null, null);
    SearchableFieldSpec boostRelevanceField =
        createSearchableFieldSpecWithCopyTo(
            "boostRelevanceField", FieldType.KEYWORD, "relevance", null, null);
    SearchableFieldSpec tier1Field =
        createSearchableFieldSpecWithCopyTo("tier1Field", FieldType.KEYWORD, null, 1, null);
    SearchableFieldSpec tier2Field =
        createSearchableFieldSpecWithCopyTo("tier2Field", FieldType.KEYWORD, null, 2, null);
    SearchableFieldSpec customField =
        createSearchableFieldSpecWithCopyTo(
            "customField", FieldType.KEYWORD, null, null, "customField");

    when(aspectSpec.getSearchableFieldSpecs())
        .thenReturn(
            Arrays.asList(
                sortNameField,
                sortTitleField,
                boostScoreField,
                boostRelevanceField,
                tier1Field,
                tier2Field,
                customField));
    when(entitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(aspectSpec));

    return entitySpec;
  }
}
