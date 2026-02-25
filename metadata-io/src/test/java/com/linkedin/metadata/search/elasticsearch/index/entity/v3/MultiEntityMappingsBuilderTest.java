package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MultiEntityMappingsBuilderTest {

  private MultiEntityMappingsBuilder mappingsBuilder;
  private EntityIndexConfiguration mockConfig;
  private EntityIndexVersionConfiguration mockV3Config;
  private OperationContext operationContext;
  private EntityRegistry mockEntityRegistry;
  private EntitySpec mockEntitySpec;

  @BeforeMethod
  public void setUp() throws IOException {
    // Setup mock configuration
    mockConfig = mock(EntityIndexConfiguration.class);
    mockV3Config = mock(EntityIndexVersionConfiguration.class);
    when(mockV3Config.isEnabled()).thenReturn(true);
    when(mockV3Config.getMappingConfig()).thenReturn(null);
    when(mockConfig.getV3()).thenReturn(mockV3Config);

    // Setup mock entity registry and specs
    mockEntityRegistry = mock(EntityRegistry.class);
    mockEntitySpec = createMockEntitySpec();

    operationContext = TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    // Note: operationContext is a real object, not a mock, so we can't mock its methods
    // We'll work with the real SearchContext it provides

    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig);
  }

  @Test
  public void testConstructorWithValidConfiguration() {
    assertNotNull(mappingsBuilder, "MultiEntityMappingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithNullConfiguration() {
    try {
      new MultiEntityMappingsBuilder(null);
      fail("Constructor should not accept null EntityIndexConfiguration");
    } catch (Exception e) {
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null configuration");
    }
  }

  @Test
  public void testGetIndexMappingsWithV3Enabled() {
    // Setup: V3 enabled with valid entity specs
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Verify we get one mapping per search group
    assertEquals(mappings.size(), 1, "Should have one mapping for the default search group");

    IndexMapping mapping = mappings.iterator().next();
    assertNotNull(mapping.getIndexName(), "Index name should not be null");
    assertNotNull(mapping.getMappings(), "Mappings should not be null");
  }

  @Test
  public void testGetIndexMappingsWithV3Disabled() throws IOException {
    // Setup: V3 disabled
    when(mockV3Config.isEnabled()).thenReturn(false);
    when(mockConfig.getV3()).thenReturn(mockV3Config);

    mappingsBuilder = new MultiEntityMappingsBuilder(mockConfig);

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty when v3 is disabled");
  }

  @Test
  public void testGetIndexMappingsWithStructuredProperties() {
    // Setup: V3 enabled with structured properties
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create structured property
    StructuredPropertyDefinition property = createMockStructuredProperty();
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test:property");
    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(Pair.of(propertyUrn, property));

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(operationContext, structuredProperties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify structured properties are included
    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingProperties =
        (Map<String, Object>) mapping.getMappings().get("properties");
    assertTrue(
        mappingProperties.containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD),
        "Should include structured properties field");
  }

  @Test
  public void testGetIndexMappingsForStructuredProperty() {
    // Create structured property
    StructuredPropertyDefinition property = createMockStructuredProperty();
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test:property");
    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(Pair.of(propertyUrn, property));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(structuredProperties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should have mappings for structured property");

    // Verify the field name is properly formatted
    String expectedFieldName = "test_property";
    assertTrue(
        mappings.containsKey(expectedFieldName), "Should contain properly formatted field name");
  }

  /**
   * Regression test: valueType urn:li:dataType:datahub.urn must resolve to URN mapping so the field
   * has a type and reindex (BuildIndicesStep) does not fail with mapper_parsing_exception.
   */
  @Test
  public void testGetIndexMappingsForStructuredPropertyWithDatahubUrnValueType()
      throws URISyntaxException {
    StructuredPropertyDefinition propWithUrnType =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("com.example.domain.owner_urn")
            .setDisplayName("Owner URN")
            .setEntityTypes(
                new UrnArray(
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataset"),
                    UrnUtils.getUrn("urn:li:entityType:datahub.dataJob")))
            .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.urn"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:com.example.domain.owner_urn"),
                propWithUrnType));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(structuredProperties);

    assertFalse(mappings.isEmpty(), "Should have mappings for URN structured property");
    String fieldName = "com_example_domain_owner_urn";
    assertTrue(mappings.containsKey(fieldName), "Should contain sanitized field name");
    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertNotNull(fieldMapping.get("type"), "URN structured property must have type for reindex");
    assertEquals(fieldMapping.get("type"), "keyword", "URN type should map to keyword");
  }

  /**
   * Ensures every structured property field has a "type" so reindex/putMapping does not fail with
   * mapper_parsing_exception. Covers STRING, URN, RICH_TEXT, DATE to meet coverage of
   * getIndexMappingsForStructuredProperty branches.
   */
  @Test
  public void testGetIndexMappingsForStructuredPropertyEveryFieldHasTypeForReindex()
      throws URISyntaxException {
    List<Pair<Urn, StructuredPropertyDefinition>> properties =
        List.of(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:com.example.domain.owner_urn"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("com.example.domain.owner_urn")
                    .setDisplayName("Owner URN")
                    .setEntityTypes(
                        new UrnArray(
                            UrnUtils.getUrn("urn:li:entityType:datahub.dataJob"),
                            UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.urn"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:simpleString"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("simpleString")
                    .setDisplayName("Simple")
                    .setEntityTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.string"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:richTextProp"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("richTextProp")
                    .setDisplayName("Rich Text")
                    .setEntityTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.rich_text"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:dateProp"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("dateProp")
                    .setDisplayName("Date")
                    .setEntityTypes(
                        new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
                    .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub.date"))));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(properties);

    assertEquals(mappings.size(), 4, "Should have four field mappings");
    for (Map.Entry<String, Object> entry : mappings.entrySet()) {
      @SuppressWarnings("unchecked")
      Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
      assertNotNull(
          fieldMapping.get("type"),
          "Every structured property field must have type for reindex: " + entry.getKey());
    }
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() {
    // Setup: V3 enabled with entity spec
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));
    when(mockEntityRegistry.getEntitySpec("testEntity")).thenReturn(mockEntitySpec);

    // Create structured property
    StructuredPropertyDefinition property = createMockStructuredProperty();
    Urn propertyUrn = UrnUtils.getUrn("urn:li:structuredProperty:test:property");
    Urn entityUrn = UrnUtils.getUrn("urn:li:testEntity:test");

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
            operationContext, entityUrn, property);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should have mappings for new structured property");
  }

  @Test
  public void testConflictResolutionBetweenEntities() {
    // Setup: Two entities with conflicting field names
    EntitySpec entitySpec1 = createMockEntitySpec("entity1", "conflictingField");
    EntitySpec entitySpec2 = createMockEntitySpec("entity2", "conflictingField");

    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(ImmutableMap.of("entity1", entitySpec1, "entity2", entitySpec2));

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Should handle conflicting fields");

    // Verify conflict resolution creates root-level field with copy_to
    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingProperties =
        (Map<String, Object>) mapping.getMappings().get("properties");

    // Should have root-level field for conflicted field
    assertTrue(
        mappingProperties.containsKey("conflictingField"),
        "Should have root-level field for conflicted field");
  }

  @Test
  public void testMappingsConsistency() {
    // Setup: V3 enabled
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Call multiple times to verify idempotency
    Collection<IndexMapping> mappings1 = mappingsBuilder.getIndexMappings(operationContext);
    Collection<IndexMapping> mappings2 = mappingsBuilder.getIndexMappings(operationContext);

    assertEquals(mappings1.size(), mappings2.size(), "Mappings should be consistent across calls");

    // Verify mappings have consistent structure
    IndexMapping mapping1 = mappings1.iterator().next();
    IndexMapping mapping2 = mappings2.iterator().next();
    assertEquals(
        mapping1.getIndexName(), mapping2.getIndexName(), "Index names should be identical");

    // Check that both mappings have the same structure (properties, _aspects, etc.)
    Map<String, Object> mappings1Map = mapping1.getMappings();
    Map<String, Object> mappings2Map = mapping2.getMappings();
    assertEquals(
        mappings1Map.keySet(), mappings2Map.keySet(), "Mappings should have same top-level keys");

    // Verify _aspects structure is consistent
    if (mappings1Map.containsKey("properties")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> props1 = (Map<String, Object>) mappings1Map.get("properties");
      @SuppressWarnings("unchecked")
      Map<String, Object> props2 = (Map<String, Object>) mappings2Map.get("properties");
      assertEquals(props1.keySet(), props2.keySet(), "Properties should have same structure");
    }
  }

  @Test
  public void testConstructorWithInvalidMappingConfig() {
    // Setup: Invalid mapping configuration
    when(mockV3Config.getMappingConfig()).thenReturn("invalid-resource");
    when(mockConfig.getV3()).thenReturn(mockV3Config);

    try {
      new MultiEntityMappingsBuilder(mockConfig);
      fail("Constructor should fail with invalid mapping configuration");
    } catch (IOException e) {
      // Expected behavior - should throw IOException for invalid resource
      assertTrue(
          e.getMessage().contains("invalid-resource") || e.getMessage().contains("resource"),
          "Error message should mention the invalid resource");
    }
  }

  @Test
  public void testGetIndexMappingsWithEmptySearchGroups() {
    // Setup: No search groups
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.emptySet());

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Should return empty mappings when no search groups");
  }

  @Test
  public void testGetIndexMappingsWithEmptyEntitySpecs() {
    // Setup: Search group exists but has no entity specs
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.emptyMap());

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertEquals(1, mappings.size(), "Should have one index mapping for the search group");

    IndexMapping mapping = mappings.iterator().next();
    assertNotNull(mapping.getIndexName(), "Index name should not be null");
    assertTrue(mapping.getMappings().isEmpty(), "Mappings should be empty when no entity specs");
  }

  // Helper methods

  private EntitySpec createMockEntitySpec() {
    return createMockEntitySpec("testEntity", "testField");
  }

  private EntitySpec createMockEntitySpec(String entityName, String fieldName) {
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn(entityName);
    when(entitySpec.getSearchGroup()).thenReturn("default");

    // Create entity annotation
    EntityAnnotation entityAnnotation = mock(EntityAnnotation.class);
    when(entityAnnotation.getName()).thenReturn(entityName);
    when(entitySpec.getEntityAnnotation()).thenReturn(entityAnnotation);

    // Create aspect specs
    List<AspectSpec> aspectSpecs = createMockAspectSpecs(fieldName);
    when(entitySpec.getAspectSpecs()).thenReturn(aspectSpecs);

    // Create searchable field specs
    List<SearchableFieldSpec> searchableFields = createMockSearchableFieldSpecs(fieldName);
    when(entitySpec.getSearchableFieldSpecs()).thenReturn(searchableFields);

    // Create searchable ref field specs
    List<SearchableRefFieldSpec> searchableRefFields = new ArrayList<>();
    when(entitySpec.getSearchableRefFieldSpecs()).thenReturn(searchableRefFields);

    return entitySpec;
  }

  private List<AspectSpec> createMockAspectSpecs(String fieldName) {
    List<AspectSpec> aspectSpecs = new ArrayList<>();

    // Create a regular aspect spec
    AspectSpec regularAspect = mock(AspectSpec.class);
    when(regularAspect.getName()).thenReturn("datasetProperties");

    // Create mock searchable field specs for the regular aspect
    List<SearchableFieldSpec> searchableFields = new ArrayList<>();
    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation searchableAnnotation = mock(SearchableAnnotation.class);
    when(searchableAnnotation.getFieldName()).thenReturn(fieldName);
    when(searchableAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.KEYWORD);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(searchableAnnotation);
    searchableFields.add(fieldSpec);

    when(regularAspect.getSearchableFieldSpecs()).thenReturn(searchableFields);
    aspectSpecs.add(regularAspect);

    return aspectSpecs;
  }

  private List<SearchableFieldSpec> createMockSearchableFieldSpecs(String fieldName) {
    List<SearchableFieldSpec> searchableFields = new ArrayList<>();

    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation searchableAnnotation = mock(SearchableAnnotation.class);
    when(searchableAnnotation.getFieldName()).thenReturn(fieldName);
    when(searchableAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.KEYWORD);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(searchableAnnotation);
    searchableFields.add(fieldSpec);

    return searchableFields;
  }

  private StructuredPropertyDefinition createMockStructuredProperty() {
    StructuredPropertyDefinition property = mock(StructuredPropertyDefinition.class);

    // Mock the Urn that getValueType() returns
    Urn valueTypeUrn = mock(Urn.class);
    when(valueTypeUrn.getId()).thenReturn("STRING");
    when(property.getValueType()).thenReturn(valueTypeUrn);

    // Mock the qualifiedName
    when(property.getQualifiedName()).thenReturn("test.property");

    // Mock entity types - use production format with datahub. prefix
    UrnArray entityTypes = new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.testEntity"));
    when(property.getEntityTypes()).thenReturn(entityTypes);

    return property;
  }
}
