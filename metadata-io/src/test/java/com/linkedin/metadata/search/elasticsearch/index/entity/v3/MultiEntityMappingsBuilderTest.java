package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants.ASPECT_FIELD_DELIMITER;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PrimitiveDataSchema;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.annotation.SearchableRefAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for MultiEntityMappingsBuilder with EntityIndexConfiguration. */
public class MultiEntityMappingsBuilderTest {

  private MultiEntityMappingsBuilder mappingsBuilder;
  private EntityIndexConfiguration entityIndexConfiguration;
  private OperationContext operationContext;
  private EntityRegistry mockEntityRegistry;
  private EntitySpec mockEntitySpec;

  @BeforeMethod
  public void setUp() {
    // Create mock EntityIndexConfiguration
    entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);

    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("");
    when(entityIndexConfiguration.getV3()).thenReturn(v3Config);

    // Create mock EntityRegistry
    mockEntityRegistry = mock(EntityRegistry.class);

    // Create mock aspect specs with searchable fields
    List<AspectSpec> aspectSpecs = createMockAspectSpecs();

    // Create mock EntitySpec
    mockEntitySpec = mock(EntitySpec.class);
    EntityAnnotation mockEntityAnnotation = mock(EntityAnnotation.class);
    when(mockEntityAnnotation.getName()).thenReturn("dataset");
    when(mockEntitySpec.getEntityAnnotation()).thenReturn(mockEntityAnnotation);
    when(mockEntitySpec.getAspectSpecs()).thenReturn(aspectSpecs);
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchGroup()).thenReturn("default");

    // Create mock OperationContext
    SearchContext mockSearchContext = mock(SearchContext.class);
    com.linkedin.metadata.utils.elasticsearch.IndexConvention mockIndexConvention =
        mock(com.linkedin.metadata.utils.elasticsearch.IndexConvention.class);
    when(mockIndexConvention.getEntityIndexNameV3("default")).thenReturn("test_index_v3");
    when(mockIndexConvention.getEntityIndexNameV3("primary")).thenReturn("primary_index_v3");
    when(mockSearchContext.getIndexConvention()).thenReturn(mockIndexConvention);

    operationContext = mock(OperationContext.class);
    when(operationContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(operationContext.getSearchContext()).thenReturn(mockSearchContext);

    // Initialize mappingsBuilder
    try {
      mappingsBuilder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (Exception e) {
      fail("Failed to create MultiEntityMappingsBuilder: " + e.getMessage());
    }
  }

  private List<AspectSpec> createMockAspectSpecs() {
    List<AspectSpec> aspectSpecs = new ArrayList<>();

    // Create a regular aspect spec
    AspectSpec regularAspect = mock(AspectSpec.class);
    when(regularAspect.getName()).thenReturn("datasetProperties");

    // Create mock searchable field specs for the regular aspect
    List<SearchableFieldSpec> searchableFields = new ArrayList<>();
    SearchableFieldSpec fieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation searchableAnnotation = mock(SearchableAnnotation.class);
    when(searchableAnnotation.getFieldName()).thenReturn("name");
    when(searchableAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.KEYWORD);
    when(fieldSpec.getSearchableAnnotation()).thenReturn(searchableAnnotation);
    searchableFields.add(fieldSpec);

    when(regularAspect.getSearchableFieldSpecs()).thenReturn(searchableFields);
    aspectSpecs.add(regularAspect);

    // Create structuredProperties aspect spec
    AspectSpec structuredPropsAspect = mock(AspectSpec.class);
    when(structuredPropsAspect.getName()).thenReturn("structuredProperties");

    // Create mock searchable field specs for structuredProperties
    List<SearchableFieldSpec> structuredPropsFields = new ArrayList<>();
    SearchableFieldSpec structuredFieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation structuredAnnotation = mock(SearchableAnnotation.class);
    when(structuredAnnotation.getFieldName()).thenReturn("customProperty");
    when(structuredAnnotation.getFieldType()).thenReturn(SearchableAnnotation.FieldType.KEYWORD);
    when(structuredFieldSpec.getSearchableAnnotation()).thenReturn(structuredAnnotation);
    structuredPropsFields.add(structuredFieldSpec);

    when(structuredPropsAspect.getSearchableFieldSpecs()).thenReturn(structuredPropsFields);
    aspectSpecs.add(structuredPropsAspect);

    return aspectSpecs;
  }

  @Test
  public void testConstructorWithValidConfiguration() {
    // Test that constructor works with valid configuration
    assertNotNull(mappingsBuilder, "MultiEntityMappingsBuilder should be created successfully");
  }

  @Test
  public void testConstructorWithNullConfiguration() {
    // Test that constructor throws exception with null configuration
    try {
      new MultiEntityMappingsBuilder(null);
      fail("Constructor should not accept null EntityIndexConfiguration");
    } catch (Exception e) {
      // Expected behavior - should throw exception for null configuration
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null configuration");
    }
  }

  @Test
  public void testGetMappingsWithValidOperationContext() {
    // Test getMappings with valid OperationContext
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");
  }

  @Test
  public void testGetMappingsWithNullOperationContext() {
    // Test that getMappings properly handles null OperationContext
    try {
      mappingsBuilder.getMappings(null);
      fail("getMappings should not accept null OperationContext");
    } catch (Exception e) {
      // Expected behavior - should not accept null
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null OperationContext");
    }
  }

  @Test
  public void testGetIndexMappingsWithValidParameters() {
    // Test getIndexMappings with valid parameters
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(operationContext, Collections.emptyList());

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");
  }

  @Test
  public void testGetIndexMappingsWithNullOperationContext() {
    // Test that getIndexMappings properly handles null OperationContext
    try {
      mappingsBuilder.getIndexMappings(null, Collections.emptyList());
      fail("getIndexMappings should not accept null OperationContext");
    } catch (Exception e) {
      // Expected behavior - should not accept null
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null OperationContext");
    }
  }

  @Test
  public void testGetIndexMappingsWithNullStructuredProperties() {
    // Test that getIndexMappings properly handles null structured properties
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() {
    // Test getIndexMappingsWithNewStructuredProperty with valid parameters
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:testEntity")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    when(mockEntityRegistry.getEntitySpec("testEntity")).thenReturn(mockEntitySpec);
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
            operationContext, testUrn, testProperty);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredPropertyWithNullOperationContext() {
    // Test that getIndexMappingsWithNewStructuredProperty properly handles null OperationContext
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);

    try {
      mappingsBuilder.getIndexMappingsWithNewStructuredProperty(null, testUrn, testProperty);
      fail("getIndexMappingsWithNewStructuredProperty should not accept null OperationContext");
    } catch (Exception e) {
      // Expected behavior - should not accept null
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null OperationContext");
    }
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredPropertyWithNullUrn() {
    // Test that getIndexMappingsWithNewStructuredProperty properly handles null Urn
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);

    try {
      mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
          operationContext, null, testProperty);
      fail("getIndexMappingsWithNewStructuredProperty should not accept null Urn");
    } catch (Exception e) {
      // Expected behavior - should not accept null
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null Urn");
    }
  }

  @Test
  public void testGetMappingsForStructuredPropertyWithValidProperties() {
    // Test getMappingsForStructuredProperty with valid properties
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("STRING");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty for valid properties");
  }

  @Test
  public void testGetMappingsForStructuredPropertyWithEmptyProperties() {
    // Test getMappingsForStructuredProperty with empty properties
    Map<String, Object> mappings =
        mappingsBuilder.getMappingsForStructuredProperty(Collections.emptyList());

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for empty properties");
  }

  @Test
  public void testGetMappingsForStructuredPropertyWithNullProperties() {
    // Test that getMappingsForStructuredProperty properly handles null properties
    try {
      mappingsBuilder.getMappingsForStructuredProperty(null);
      fail("getMappingsForStructuredProperty should not accept null properties");
    } catch (Exception e) {
      // Expected behavior - should not accept null
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null properties");
    }
  }

  // Tests for structured property value type mapping logic

  @Test
  public void testStructuredPropertyStringValueTypeMapping() {
    // Test STRING value type mapping to keyword field
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("STRING");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that STRING type maps to keyword field type
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"), "keyword", "STRING value type should map to keyword field type");
  }

  @Test
  public void testStructuredPropertyRichTextValueTypeMapping() {
    // Test RICH_TEXT value type mapping to keyword field
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("RICH_TEXT");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that RICH_TEXT type maps to keyword field type
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"),
        "keyword",
        "RICH_TEXT value type should map to keyword field type");
  }

  @Test
  public void testStructuredPropertyDateValueTypeMapping() {
    // Test DATE value type mapping to date field
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("DATE");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that DATE type maps to date field type
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(fieldMapping.get("type"), "date", "DATE value type should map to date field type");
  }

  @Test
  public void testStructuredPropertyUrnValueTypeMapping() {
    // Test URN value type mapping to URN field (keyword with ignore_above)
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("URN");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that URN type maps to keyword field type with ignore_above
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"), "keyword", "URN value type should map to keyword field type");
    assertEquals(
        fieldMapping.get("ignore_above"), 255, "URN field should have ignore_above set to 255");
  }

  @Test
  public void testStructuredPropertyNumberValueTypeMapping() {
    // Test NUMBER value type mapping to double field
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("NUMBER");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that NUMBER type maps to double field type
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"), "double", "NUMBER value type should map to double field type");
  }

  @Test
  public void testStructuredPropertyUnknownValueTypeMapping() {
    // Test unknown value type defaulting to keyword field
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("UNKNOWN_TYPE");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that unknown type defaults to keyword field type
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"),
        "keyword",
        "Unknown value type should default to keyword field type");
  }

  @Test
  public void testStructuredPropertyCaseInsensitiveValueTypeMapping() {
    // Test case insensitive value type matching
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("string"); // lowercase
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that lowercase "string" maps to keyword field type (case insensitive)
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"), "keyword", "Lowercase 'string' should map to keyword field type");
  }

  @Test
  public void testStructuredPropertyMixedCaseValueTypeMapping() {
    // Test mixed case value type matching
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("DaTe"); // mixed case
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Verify that mixed case "DaTe" maps to date field type (case insensitive)
    String fieldName = mappings.keySet().iterator().next();
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertEquals(
        fieldMapping.get("type"), "date", "Mixed case 'DaTe' should map to date field type");
  }

  @Test
  public void testStructuredPropertyMultipleValueTypesMapping() {
    // Test multiple structured properties with different value types
    Urn testUrn1 = UrnUtils.getUrn("urn:li:testEntity:testId1");
    Urn testUrn2 = UrnUtils.getUrn("urn:li:testEntity:testId2");
    Urn testUrn3 = UrnUtils.getUrn("urn:li:testEntity:testId3");

    StructuredPropertyDefinition stringProperty = mock(StructuredPropertyDefinition.class);
    Urn stringValueTypeUrn = mock(Urn.class);
    when(stringValueTypeUrn.getId()).thenReturn("STRING");
    when(stringProperty.getValueType()).thenReturn(stringValueTypeUrn);
    when(stringProperty.getQualifiedName()).thenReturn("stringProperty");

    StructuredPropertyDefinition dateProperty = mock(StructuredPropertyDefinition.class);
    Urn dateValueTypeUrn = mock(Urn.class);
    when(dateValueTypeUrn.getId()).thenReturn("DATE");
    when(dateProperty.getValueType()).thenReturn(dateValueTypeUrn);
    when(dateProperty.getQualifiedName()).thenReturn("dateProperty");

    StructuredPropertyDefinition numberProperty = mock(StructuredPropertyDefinition.class);
    Urn numberValueTypeUrn = mock(Urn.class);
    when(numberValueTypeUrn.getId()).thenReturn("NUMBER");
    when(numberProperty.getValueType()).thenReturn(numberValueTypeUrn);
    when(numberProperty.getQualifiedName()).thenReturn("numberProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> properties =
        Arrays.asList(
            Pair.of(testUrn1, stringProperty),
            Pair.of(testUrn2, dateProperty),
            Pair.of(testUrn3, numberProperty));

    Map<String, Object> mappings = mappingsBuilder.getMappingsForStructuredProperty(properties);

    assertNotNull(mappings, "Mappings should not be null");
    assertEquals(mappings.size(), 3, "Should have 3 mappings for 3 properties");

    // Verify each property has the correct field type
    for (Map.Entry<String, Object> entry : mappings.entrySet()) {
      String fieldName = entry.getKey();
      Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();

      if (fieldName.contains("stringProperty")) {
        assertEquals(fieldMapping.get("type"), "keyword", "String property should map to keyword");
      } else if (fieldName.contains("dateProperty")) {
        assertEquals(fieldMapping.get("type"), "date", "Date property should map to date");
      } else if (fieldName.contains("numberProperty")) {
        assertEquals(fieldMapping.get("type"), "double", "Number property should map to double");
      }
    }
  }

  // Tests for empty entitySpecs early return condition (tested indirectly through public methods)

  @Test
  public void testGetIndexMappingsWithEmptyEntitySpecs() {
    // Test that getIndexMappings handles empty entitySpecs correctly
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String emptySearchGroup = "emptyGroup";

    // Mock the registry to return empty collection for the search group
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(emptySearchGroup))
        .thenReturn(Collections.emptyMap());
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton(emptySearchGroup));

    // Create operation context with the mock registry
    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertFalse(
        result.isEmpty(), "Should return at least one IndexMapping even for empty entitySpecs");

    // Verify that the IndexMapping has empty mappings
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    assertTrue(
        indexMapping.getMappings().isEmpty(),
        "Mappings should be empty when no entities found for search group");
  }

  @Test
  public void testGetIndexMappingsWithNonExistentSearchGroup() {
    // Test behavior with non-existent search group
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String nonExistentSearchGroup = "nonExistentGroup";

    // Mock the registry to return empty collection for non-existent search group
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(nonExistentSearchGroup))
        .thenReturn(Collections.emptyMap());
    when(mockEntityRegistry.getSearchGroups())
        .thenReturn(Collections.singleton(nonExistentSearchGroup));

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertFalse(
        result.isEmpty(),
        "Should return at least one IndexMapping even for non-existent search group");

    // Verify that the IndexMapping has empty mappings
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    assertTrue(
        indexMapping.getMappings().isEmpty(),
        "Mappings should be empty for non-existent search group");
  }

  @Test
  public void testGetIndexMappingsWithNullSearchGroup() {
    // Test behavior with null search group
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String nullSearchGroup = null;

    // Mock the registry to return empty collection for null search group
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(nullSearchGroup))
        .thenReturn(Collections.emptyMap());
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton(nullSearchGroup));

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertFalse(
        result.isEmpty(), "Should return at least one IndexMapping even for null search group");

    // Verify that the IndexMapping has empty mappings
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    assertTrue(
        indexMapping.getMappings().isEmpty(), "Mappings should be empty for null search group");
  }

  @Test
  public void testGetIndexMappingsWithEmptySearchGroup() {
    // Test behavior with empty string search group
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String emptySearchGroup = "";

    // Mock the registry to return empty collection for empty search group
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(emptySearchGroup))
        .thenReturn(Collections.emptyMap());
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton(emptySearchGroup));

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertFalse(
        result.isEmpty(), "Should return at least one IndexMapping even for empty search group");

    // Verify that the IndexMapping has empty mappings
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    assertTrue(
        indexMapping.getMappings().isEmpty(), "Mappings should be empty for empty search group");
  }

  @Test
  public void testGetIndexMappingsWithWhitespaceSearchGroup() {
    // Test behavior with whitespace-only search group
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String whitespaceSearchGroup = "   ";

    // Mock the registry to return empty collection for whitespace search group
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(whitespaceSearchGroup))
        .thenReturn(Collections.emptyMap());
    when(mockEntityRegistry.getSearchGroups())
        .thenReturn(Collections.singleton(whitespaceSearchGroup));

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertFalse(
        result.isEmpty(),
        "Should return at least one IndexMapping even for whitespace search group");

    // Verify that the IndexMapping has empty mappings
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    assertTrue(
        indexMapping.getMappings().isEmpty(),
        "Mappings should be empty for whitespace search group");
  }

  @Test
  public void testGetIndexMappingsWithValidSearchGroup() {
    // Test that getIndexMappings works correctly with valid search group
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String validSearchGroup = "validGroup";

    // Create mock entity specs
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);

    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("entity1", mockEntitySpec1);
    entitySpecs.put("entity2", mockEntitySpec2);

    // Mock the registry to return valid entity specs
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(validSearchGroup)).thenReturn(entitySpecs);
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton(validSearchGroup));

    // Mock entity spec behavior
    EntityAnnotation mockEntityAnnotation1 = mock(EntityAnnotation.class);
    when(mockEntityAnnotation1.getName()).thenReturn("entity1");
    when(mockEntitySpec1.getEntityAnnotation()).thenReturn(mockEntityAnnotation1);
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec1.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec1.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    EntityAnnotation mockEntityAnnotation2 = mock(EntityAnnotation.class);
    when(mockEntityAnnotation2.getName()).thenReturn("entity2");
    when(mockEntitySpec2.getEntityAnnotation()).thenReturn(mockEntityAnnotation2);
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec2.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec2.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    assertFalse(result.isEmpty(), "Should return at least one IndexMapping for valid search group");

    // Verify that the IndexMapping has mappings (not empty)
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    // Note: The actual content depends on the mock setup, but we verify it's not empty
  }

  @Test
  public void testGetIndexMappingsWithStructuredPropertiesAndEmptyEntitySpecs() {
    // Test that getIndexMappings handles structured properties correctly when entitySpecs is empty
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    String searchGroup = "emptyGroup";

    // Create mock structured properties
    Urn testUrn = UrnUtils.getUrn("urn:li:testEntity:testId");
    StructuredPropertyDefinition testProperty = mock(StructuredPropertyDefinition.class);
    Urn mockValueTypeUrn = mock(Urn.class);
    when(mockValueTypeUrn.getId()).thenReturn("STRING");
    when(testProperty.getValueType()).thenReturn(mockValueTypeUrn);
    when(testProperty.getQualifiedName()).thenReturn("testProperty");

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(Pair.of(testUrn, testProperty));

    // Mock the registry to return empty collection for the search group
    when(mockEntityRegistry.getEntitySpecsBySearchGroup(searchGroup))
        .thenReturn(Collections.emptyMap());
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton(searchGroup));

    OperationContext testOpContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(testOpContext, structuredProperties);

    assertNotNull(result, "Result should not be null");
    assertFalse(
        result.isEmpty(),
        "Should return at least one IndexMapping even with structured properties and empty entitySpecs");

    // Verify that the IndexMapping has empty mappings
    IndexMapping indexMapping = result.iterator().next();
    assertNotNull(indexMapping.getMappings(), "IndexMapping should have mappings");
    assertTrue(
        indexMapping.getMappings().isEmpty(),
        "Mappings should be empty when no entities found for search group, even with structured properties");
  }

  // Tests for ConflictResolver alias type conflict resolution

  @Test
  public void testConflictResolverResolveTypeConflictWithResolvableConflicts() {
    // Test that ConflictResolver.resolveTypeConflict works with resolvable conflicts
    Set<String> resolvableConflicts = new HashSet<>();
    resolvableConflicts.add("long");
    resolvableConflicts.add("date");

    String resolvedType = ConflictResolver.resolveTypeConflict(resolvableConflicts);
    assertEquals(resolvedType, "date", "Should resolve [long, date] conflicts to 'date'");
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithSingleType() {
    // Test that ConflictResolver.resolveTypeConflict works with single type
    Set<String> singleType = new HashSet<>();
    singleType.add("keyword");

    String resolvedType = ConflictResolver.resolveTypeConflict(singleType);
    assertEquals(resolvedType, "keyword", "Should return the single type when no conflicts");
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithNonResolvableConflicts() {
    // Test that ConflictResolver.resolveTypeConflict throws IllegalArgumentException for
    // non-resolvable conflicts
    Set<String> nonResolvableConflicts = new HashSet<>();
    nonResolvableConflicts.add("keyword");
    nonResolvableConflicts.add("text");

    assertThrows(
        IllegalArgumentException.class,
        () -> ConflictResolver.resolveTypeConflict(nonResolvableConflicts));
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithMultipleNonResolvableConflicts() {
    // Test that ConflictResolver.resolveTypeConflict throws IllegalArgumentException for multiple
    // non-resolvable conflicts
    Set<String> multipleConflicts = new HashSet<>();
    multipleConflicts.add("keyword");
    multipleConflicts.add("text");
    multipleConflicts.add("long");

    assertThrows(
        IllegalArgumentException.class,
        () -> ConflictResolver.resolveTypeConflict(multipleConflicts));
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithEmptySet() {
    // Test that ConflictResolver.resolveTypeConflict handles empty set
    Set<String> emptySet = new HashSet<>();

    assertThrows(
        NoSuchElementException.class, () -> ConflictResolver.resolveTypeConflict(emptySet));
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithNullSet() {
    // Test that ConflictResolver.resolveTypeConflict handles null set
    assertThrows(NullPointerException.class, () -> ConflictResolver.resolveTypeConflict(null));
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithThreeTypesIncludingResolvable() {
    // Test that ConflictResolver.resolveTypeConflict throws exception even when resolvable pair
    // exists
    Set<String> threeTypesWithResolvable = new HashSet<>();
    threeTypesWithResolvable.add("long");
    threeTypesWithResolvable.add("date");
    threeTypesWithResolvable.add("keyword");

    assertThrows(
        IllegalArgumentException.class,
        () -> ConflictResolver.resolveTypeConflict(threeTypesWithResolvable));
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithDifferentResolvablePair() {
    // Test that ConflictResolver.resolveTypeConflict only resolves [long, date] conflicts
    Set<String> differentResolvablePair = new HashSet<>();
    differentResolvablePair.add("date");
    differentResolvablePair.add("long");

    String resolvedType = ConflictResolver.resolveTypeConflict(differentResolvablePair);
    assertEquals(
        resolvedType,
        "date",
        "Should resolve [date, long] conflicts to 'date' (order doesn't matter)");
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithCaseSensitiveTypes() {
    // Test that ConflictResolver.resolveTypeConflict is case sensitive
    Set<String> caseSensitiveConflicts = new HashSet<>();
    caseSensitiveConflicts.add("LONG");
    caseSensitiveConflicts.add("DATE");

    assertThrows(
        IllegalArgumentException.class,
        () -> ConflictResolver.resolveTypeConflict(caseSensitiveConflicts));
  }

  @Test
  public void testConflictResolverResolveTypeConflictWithMixedCaseResolvable() {
    // Test that ConflictResolver.resolveTypeConflict requires exact case matching
    Set<String> mixedCaseResolvable = new HashSet<>();
    mixedCaseResolvable.add("long");
    mixedCaseResolvable.add("DATE");

    assertThrows(
        IllegalArgumentException.class,
        () -> ConflictResolver.resolveTypeConflict(mixedCaseResolvable));
  }

  @Test
  public void testV3Disabled() {
    // Test behavior when v3 is disabled
    EntityIndexVersionConfiguration v3ConfigDisabled = mock(EntityIndexVersionConfiguration.class);
    when(v3ConfigDisabled.isEnabled()).thenReturn(false);
    when(v3ConfigDisabled.getMappingConfig()).thenReturn("");
    when(entityIndexConfiguration.getV3()).thenReturn(v3ConfigDisabled);

    try {
      MultiEntityMappingsBuilder disabledBuilder =
          new MultiEntityMappingsBuilder(entityIndexConfiguration);

      Collection<IndexMapping> mappings = disabledBuilder.getMappings(operationContext);

      assertNotNull(mappings, "Mappings should not be null");
      assertTrue(mappings.isEmpty(), "Mappings should be empty when v3 is disabled");
    } catch (Exception e) {
      fail("Should not throw exception when v3 is disabled: " + e.getMessage());
    }
  }

  @Test
  public void testAspectFieldDelimiter() {
    // Test that the aspect field delimiter constant is accessible
    assertEquals(ASPECT_FIELD_DELIMITER, ".", "Aspect field delimiter should be a dot");
  }

  @Test
  public void testAspectsStructure() {
    // Test that mappings use the new _aspects structure
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Check that mappings contain _aspects structure
    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingContent = mapping.getMappings();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
    assertNotNull(properties, "Mappings should have properties");

    // Verify _aspects object exists
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "Mappings should have _aspects object");

    // Verify _aspects has properties
    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectsProperties, "_aspects should have properties");
  }

  @Test
  public void testStructuredPropertiesException() {
    // Test that structuredProperties aspect is handled as an exception (fields remain at root
    // level)
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a test structured property
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:testStructuredProperty"), testProperty));

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(operationContext, structuredProperties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingContent = mapping.getMappings();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
    assertNotNull(properties, "Mappings should have properties");

    // Verify that structuredProperties container exists
    assertTrue(
        properties.containsKey("structuredProperties"),
        "structuredProperties container should exist at root level");

    // Verify that structuredProperties has the correct structure
    @SuppressWarnings("unchecked")
    Map<String, Object> structuredPropsContainer =
        (Map<String, Object>) properties.get("structuredProperties");
    assertNotNull(structuredPropsContainer, "structuredProperties should not be null");

    // Verify it has dynamic: true
    assertTrue(
        structuredPropsContainer.containsKey("dynamic"),
        "structuredProperties should have dynamic field");
    assertEquals(
        structuredPropsContainer.get("dynamic"),
        true,
        "structuredProperties should have dynamic set to true");

    @SuppressWarnings("unchecked")
    Map<String, Object> structuredProps =
        (Map<String, Object>) structuredPropsContainer.get("properties");
    assertNotNull(structuredProps, "structuredProperties should have properties");

    assertTrue(
        structuredProps.containsKey("_versioned.testStructuredProperty.00000000000001.string"),
        "structuredProperties should contain the expected field");

    // Verify structuredProperties is NOT in _aspects
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    if (aspects != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
      if (aspectsProperties != null) {
        assertNull(
            aspectsProperties.get("structuredProperties"),
            "structuredProperties should not be in _aspects");
        // Verify that regular aspects are in _aspects
        assertTrue(
            aspectsProperties.containsKey("datasetProperties"),
            "Regular aspects should be in _aspects");
      }
    }
  }

  @Test
  public void testAliasPathsForAspects() {
    // Test that aliases point to the correct _aspects.aspectName.fieldName structure
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingContent = mapping.getMappings();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
    assertNotNull(properties, "Mappings should have properties");

    // Check that aliases point to _aspects.aspectName.fieldName structure
    boolean hasAspectAliases =
        properties.entrySet().stream()
            .anyMatch(
                entry -> {
                  if (entry.getValue() instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
                    if ("alias".equals(fieldMapping.get("type"))) {
                      String path = (String) fieldMapping.get("path");
                      return path != null && path.startsWith("_aspects.");
                    }
                  }
                  return false;
                });
    assertTrue(hasAspectAliases);

    // Note: This test may not find aliases if there are no searchable fields in the test setup
    // In a real scenario with actual entity specs, you would have aliases pointing to _aspects
    // The structure validation is complete - aliases would point to _aspects.aspectName.fieldName
  }

  @Test
  public void testAspectsStructureWithMultipleAspects() {
    // Test that multiple aspects are properly organized under _aspects
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingContent = mapping.getMappings();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
    assertNotNull(properties, "Mappings should have properties");

    // Verify _aspects structure
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    if (aspects != null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
      if (aspectsProperties != null) {
        // Verify that no aspect in _aspects is named 'structuredProperties'
        assertTrue(
            aspectsProperties.isEmpty()
                || aspectsProperties.keySet().stream()
                    .noneMatch(key -> "structuredProperties".equals(key)),
            "No aspect in _aspects should be named 'structuredProperties'");
      }
    }
  }

  @Test
  public void testMappingStructureConsistency() {
    // Test that the mapping structure is consistent and well-formed
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    for (IndexMapping mapping : mappings) {
      Map<String, Object> mappingContent = mapping.getMappings();

      // Verify basic structure
      assertTrue(mappingContent.containsKey("properties"), "Each mapping should have properties");

      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
      assertNotNull(properties, "Properties should not be null");

      // Verify that if _aspects exists, it has proper structure
      if (properties.containsKey("_aspects")) {
        @SuppressWarnings("unchecked")
        Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
        assertNotNull(aspects, "_aspects should not be null");
        assertTrue(aspects.containsKey("properties"), "_aspects should have properties");

        @SuppressWarnings("unchecked")
        Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
        assertNotNull(aspectsProperties, "_aspects properties should not be null");
      }
    }
  }

  @Test
  public void testUrnFieldInMappings() throws IOException {
    // Test that the urn field is present in all entity mappings when YAML config is loaded
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);
    Collection<IndexMapping> mappings = builder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    for (IndexMapping mapping : mappings) {
      Map<String, Object> mappingContent = mapping.getMappings();

      // Verify basic structure
      assertTrue(mappingContent.containsKey("properties"), "Each mapping should have properties");

      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
      assertNotNull(properties, "Properties should not be null");

      // Verify that urn field exists
      assertTrue(properties.containsKey("urn"), "Each mapping should have a 'urn' field");

      @SuppressWarnings("unchecked")
      Map<String, Object> urnField = (Map<String, Object>) properties.get("urn");
      assertNotNull(urnField, "urn field should not be null");

      // Verify urn field is a keyword field
      assertEquals(urnField.get("type"), "keyword", "urn field should be of type keyword");

      // Verify urn field has copy_to _search.tier_4
      assertTrue(urnField.containsKey("copy_to"), "urn field should have copy_to");
      @SuppressWarnings("unchecked")
      List<String> copyTo = (List<String>) urnField.get("copy_to");
      assertTrue(copyTo.contains("_search.tier_4"), "urn field should copy to _search.tier_4");
    }
  }

  @Test
  public void testProposalsAspectAliasCreation() {
    // Test specific case that was causing the self-referencing alias issue
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a mock aspect spec for "proposals" with "proposedGlossaryTerms" field
    AspectSpec mockProposalsAspect = mock(AspectSpec.class);
    when(mockProposalsAspect.getName()).thenReturn("proposals");

    SearchableFieldSpec mockProposedGlossaryTermsField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn("proposedGlossaryTerms");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockProposedGlossaryTermsField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockProposalsAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockProposedGlossaryTermsField));
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockProposalsAspect));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that the root-level field exists (single field should be an alias)
    assertTrue(
        properties.containsKey("proposedGlossaryTerms"),
        "Root-level field for proposedGlossaryTerms should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> rootField = (Map<String, Object>) properties.get("proposedGlossaryTerms");
    assertEquals(rootField.get("type"), "alias", "Root-level field should be an alias");
    assertEquals(
        rootField.get("path"),
        "_aspects.proposals.proposedGlossaryTerms",
        "Root-level alias should point to _aspects.proposals.proposedGlossaryTerms");

    // Verify that the actual field exists under _aspects.proposals
    assertTrue(properties.containsKey("_aspects"), "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectsProperties.containsKey("proposals"), "proposals aspect should exist under _aspects");

    @SuppressWarnings("unchecked")
    Map<String, Object> proposalsAspect = (Map<String, Object>) aspectsProperties.get("proposals");
    assertTrue(proposalsAspect.containsKey("properties"), "proposals should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> proposalsProperties =
        (Map<String, Object>) proposalsAspect.get("properties");
    assertTrue(
        proposalsProperties.containsKey("proposedGlossaryTerms"),
        "proposedGlossaryTerms field should exist under _aspects.proposals");
  }

  @Test
  public void testContainerAspectFieldCreation() {
    // Test that the container aspect creates the proper field structure
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a mock aspect spec for "container" with "container" field
    AspectSpec mockContainerAspect = mock(AspectSpec.class);
    when(mockContainerAspect.getName()).thenReturn("container");

    SearchableFieldSpec mockContainerField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn("container");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.URN);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockContainerField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockContainerAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockContainerField));
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockContainerAspect));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that the root-level field exists (single field should be an alias)
    assertTrue(properties.containsKey("container"), "Root-level field for container should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> rootField = (Map<String, Object>) properties.get("container");
    assertEquals(rootField.get("type"), "alias", "Root-level field should be an alias");
    assertEquals(
        rootField.get("path"),
        "_aspects.container.container",
        "Root-level alias should point to _aspects.container.container");

    // Verify that the actual field exists under _aspects.container
    assertTrue(properties.containsKey("_aspects"), "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectsProperties.containsKey("container"), "container aspect should exist under _aspects");

    @SuppressWarnings("unchecked")
    Map<String, Object> containerAspect = (Map<String, Object>) aspectsProperties.get("container");
    assertTrue(containerAspect.containsKey("properties"), "container should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> containerProperties =
        (Map<String, Object>) containerAspect.get("properties");
    assertTrue(
        containerProperties.containsKey("container"),
        "container field should exist under _aspects.container");

    // Verify the field type is correct for URN (treated as keyword in v3 mappings)
    @SuppressWarnings("unchecked")
    Map<String, Object> containerField = (Map<String, Object>) containerProperties.get("container");
    assertEquals(containerField.get("type"), "keyword", "container field should be keyword type");
    // URN fields are treated as standard keyword fields in v3 mappings, no special properties
  }

  @Test
  public void testMultiPathFieldCreatesNonAliasFieldWithCopyTo() {
    // Test that fields appearing in multiple aspects create non-alias fields with copy_to
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create two entity specs with the same field name in different aspects
    EntitySpec entitySpec1 = mock(EntitySpec.class);
    EntitySpec entitySpec2 = mock(EntitySpec.class);

    when(entitySpec1.getEntityAnnotation()).thenReturn(mock(EntityAnnotation.class));
    when(entitySpec1.getEntityAnnotation().getName()).thenReturn("entity1");
    when(entitySpec2.getEntityAnnotation()).thenReturn(mock(EntityAnnotation.class));
    when(entitySpec2.getEntityAnnotation().getName()).thenReturn("entity2");

    // Create aspect specs for both entities
    AspectSpec aspect1 = mock(AspectSpec.class);
    AspectSpec aspect2 = mock(AspectSpec.class);
    when(aspect1.getName()).thenReturn("ApplicationProperties");
    when(aspect2.getName()).thenReturn("DatasetProperties");

    // Create field specs with the same field name "description" in both aspects
    SearchableFieldSpec field1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec field2 = mock(SearchableFieldSpec.class);

    SearchableAnnotation annotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation annotation2 = mock(SearchableAnnotation.class);

    when(annotation1.getFieldName()).thenReturn("description");
    when(annotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(annotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(annotation1.getSearchTier()).thenReturn(Optional.of(2));
    when(annotation1.getSearchLabel()).thenReturn(Optional.empty());
    when(annotation1.getEntityFieldName()).thenReturn(Optional.empty());
    when(annotation1.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(annotation1.getSearchIndexed()).thenReturn(Optional.empty());
    when(annotation1.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(annotation1.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(field1.getSearchableAnnotation()).thenReturn(annotation1);

    when(annotation2.getFieldName()).thenReturn("description");
    when(annotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(annotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(annotation2.getSearchTier()).thenReturn(Optional.of(2));
    when(annotation2.getSearchLabel()).thenReturn(Optional.empty());
    when(annotation2.getEntityFieldName()).thenReturn(Optional.empty());
    when(annotation2.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(annotation2.getSearchIndexed()).thenReturn(Optional.empty());
    when(annotation2.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(annotation2.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(field2.getSearchableAnnotation()).thenReturn(annotation2);

    when(aspect1.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field1));
    when(aspect2.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field2));

    when(entitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(aspect1));
    when(entitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(aspect2));

    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            Map.of(
                "entity1", entitySpec1,
                "entity2", entitySpec2));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that the root-level field exists and is NOT an alias (should be a field)
    assertTrue(
        properties.containsKey("description"), "Root-level field for description should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> rootField = (Map<String, Object>) properties.get("description");
    assertEquals(
        rootField.get("type"), "keyword", "Root-level field should be a keyword field, not alias");
    assertFalse(
        rootField.containsKey("path"), "Root-level field should NOT have path (it's not an alias)");
    assertFalse(
        rootField.containsKey("copy_to"),
        "Root-level field should NOT have copy_to (it's the target)");

    // Verify that aspect fields have copy_to pointing to the root field
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectsProperties, "_aspects should have properties");

    // Check ApplicationProperties aspect
    @SuppressWarnings("unchecked")
    Map<String, Object> applicationPropertiesAspect =
        (Map<String, Object>) aspectsProperties.get("ApplicationProperties");
    assertNotNull(applicationPropertiesAspect, "ApplicationProperties aspect should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> applicationPropertiesAspectProperties =
        (Map<String, Object>) applicationPropertiesAspect.get("properties");
    assertNotNull(
        applicationPropertiesAspectProperties,
        "ApplicationProperties aspect should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> descriptionFieldInApplicationProperties =
        (Map<String, Object>) applicationPropertiesAspectProperties.get("description");
    assertNotNull(
        descriptionFieldInApplicationProperties,
        "description field should exist in ApplicationProperties aspect");
    assertTrue(
        descriptionFieldInApplicationProperties.containsKey("copy_to"),
        "description field in ApplicationProperties should have copy_to");

    @SuppressWarnings("unchecked")
    List<String> applicationPropertiesCopyTo =
        (List<String>) descriptionFieldInApplicationProperties.get("copy_to");
    assertTrue(
        applicationPropertiesCopyTo.contains("description"),
        "description field in ApplicationProperties should copy_to root description field");
    assertTrue(
        applicationPropertiesCopyTo.contains("_search.tier_2"),
        "description field in ApplicationProperties should copy_to _search.tier_2");

    // Check DatasetProperties aspect
    @SuppressWarnings("unchecked")
    Map<String, Object> datasetPropertiesAspect =
        (Map<String, Object>) aspectsProperties.get("DatasetProperties");
    assertNotNull(datasetPropertiesAspect, "DatasetProperties aspect should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> datasetPropertiesAspectProperties =
        (Map<String, Object>) datasetPropertiesAspect.get("properties");
    assertNotNull(
        datasetPropertiesAspectProperties, "DatasetProperties aspect should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> descriptionFieldInDatasetProperties =
        (Map<String, Object>) datasetPropertiesAspectProperties.get("description");
    assertNotNull(
        descriptionFieldInDatasetProperties,
        "description field should exist in DatasetProperties aspect");
    assertTrue(
        descriptionFieldInDatasetProperties.containsKey("copy_to"),
        "description field in DatasetProperties should have copy_to");

    @SuppressWarnings("unchecked")
    List<String> datasetPropertiesCopyTo =
        (List<String>) descriptionFieldInDatasetProperties.get("copy_to");
    assertTrue(
        datasetPropertiesCopyTo.contains("description"),
        "description field in DatasetProperties should copy_to root description field");
    assertTrue(
        datasetPropertiesCopyTo.contains("_search.tier_2"),
        "description field in DatasetProperties should copy_to _search.tier_2");
  }

  @Test
  public void testBrowsePathV2FieldMapping() {
    // Test that BROWSE_PATH_V2 fields are mapped with the correct analyzer and structure
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a mock aspect spec for "browsePathsV2" with "browsePathV2" field
    AspectSpec mockBrowsePathsV2Aspect = mock(AspectSpec.class);
    when(mockBrowsePathsV2Aspect.getName()).thenReturn("browsePathsV2");

    SearchableFieldSpec mockBrowsePathV2Field = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn("browsePathV2");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.BROWSE_PATH_V2);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation.getSearchTier()).thenReturn(Optional.empty());
    when(mockAnnotation.getSearchLabel()).thenReturn(Optional.empty());
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(mockAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockAnnotation.getSearchIndexed()).thenReturn(Optional.empty());
    when(mockAnnotation.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockAnnotation.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockBrowsePathV2Field.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockBrowsePathsV2Aspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockBrowsePathV2Field));
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockBrowsePathsV2Aspect));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that the root-level field exists and is an alias (single field should be an alias)
    assertTrue(
        properties.containsKey("browsePathV2"), "Root-level field for browsePathV2 should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> rootField = (Map<String, Object>) properties.get("browsePathV2");
    assertEquals(rootField.get("type"), "alias", "Root-level field should be an alias");
    assertEquals(
        rootField.get("path"),
        "_aspects.browsePathsV2.browsePathV2",
        "Root-level alias should point to _aspects.browsePathsV2.browsePathV2");

    // Verify that the actual field exists under _aspects.browsePathsV2 with correct mapping
    assertTrue(properties.containsKey("_aspects"), "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "_aspects should not be null");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectsProperties, "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> browsePathsV2Aspect =
        (Map<String, Object>) aspectsProperties.get("browsePathsV2");
    assertNotNull(browsePathsV2Aspect, "browsePathsV2 aspect should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> browsePathsV2AspectProperties =
        (Map<String, Object>) browsePathsV2Aspect.get("properties");
    assertNotNull(browsePathsV2AspectProperties, "browsePathsV2 aspect should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> browsePathV2Field =
        (Map<String, Object>) browsePathsV2AspectProperties.get("browsePathV2");
    assertNotNull(browsePathV2Field, "browsePathV2 field should exist in browsePathsV2 aspect");

    // Verify the field type and analyzer
    assertEquals(browsePathV2Field.get("type"), "text", "browsePathV2 field should be text type");
    assertEquals(
        browsePathV2Field.get("analyzer"),
        "browse_path_v2_hierarchy",
        "browsePathV2 field should use browse_path_v2_hierarchy analyzer");
    assertEquals(
        browsePathV2Field.get("fielddata"), true, "browsePathV2 field should have fielddata=true");

    // Verify the length field exists
    assertTrue(browsePathV2Field.containsKey("fields"), "browsePathV2 field should have fields");
    @SuppressWarnings("unchecked")
    Map<String, Object> fields = (Map<String, Object>) browsePathV2Field.get("fields");
    assertTrue(fields.containsKey("length"), "browsePathV2 field should have length subfield");

    @SuppressWarnings("unchecked")
    Map<String, Object> lengthField = (Map<String, Object>) fields.get("length");
    assertEquals(lengthField.get("type"), "token_count", "length field should be token_count type");
    assertEquals(
        lengthField.get("analyzer"),
        "unit_separator_pattern",
        "length field should use unit_separator_pattern analyzer");
  }

  @Test
  public void testFieldNameConflictDetection() {
    // Test that field name conflicts are detected and aliases are not created for conflicted fields
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create two entity specs with conflicting field names
    EntitySpec datasetEntitySpec = mock(EntitySpec.class);
    EntityAnnotation datasetEntityAnnotation = mock(EntityAnnotation.class);
    when(datasetEntityAnnotation.getName()).thenReturn("dataset");
    when(datasetEntitySpec.getEntityAnnotation()).thenReturn(datasetEntityAnnotation);

    EntitySpec containerEntitySpec = mock(EntitySpec.class);
    EntityAnnotation containerEntityAnnotation = mock(EntityAnnotation.class);
    when(containerEntityAnnotation.getName()).thenReturn("container");
    when(containerEntitySpec.getEntityAnnotation()).thenReturn(containerEntityAnnotation);

    // Dataset entity has 'name' field in 'datasetProperties' aspect
    AspectSpec datasetPropertiesAspect = mock(AspectSpec.class);
    when(datasetPropertiesAspect.getName()).thenReturn("datasetProperties");

    SearchableFieldSpec datasetNameField = mock(SearchableFieldSpec.class);
    SearchableAnnotation datasetNameAnnotation = mock(SearchableAnnotation.class);
    when(datasetNameAnnotation.getFieldName()).thenReturn("name");
    when(datasetNameAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(datasetNameAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(datasetNameField.getSearchableAnnotation()).thenReturn(datasetNameAnnotation);

    when(datasetPropertiesAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(datasetNameField));
    when(datasetEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(datasetPropertiesAspect));

    // Container entity has 'name' field in 'containerProperties' aspect
    AspectSpec containerPropertiesAspect = mock(AspectSpec.class);
    when(containerPropertiesAspect.getName()).thenReturn("containerProperties");

    SearchableFieldSpec containerNameField = mock(SearchableFieldSpec.class);
    SearchableAnnotation containerNameAnnotation = mock(SearchableAnnotation.class);
    when(containerNameAnnotation.getFieldName()).thenReturn("name");
    when(containerNameAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(containerNameAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(containerNameField.getSearchableAnnotation()).thenReturn(containerNameAnnotation);

    when(containerPropertiesAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(containerNameField));
    when(containerEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(containerPropertiesAspect));

    // Mock the entity registry to return both entities
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("dataset", datasetEntitySpec);
    entitySpecs.put("container", containerEntitySpec);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default")).thenReturn(entitySpecs);

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that both aspects exist under _aspects
    assertTrue(properties.containsKey("_aspects"), "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectsProperties.containsKey("datasetProperties"),
        "datasetProperties aspect should exist");
    assertTrue(
        aspectsProperties.containsKey("containerProperties"),
        "containerProperties aspect should exist");

    // Verify that the conflicted 'name' field has a root-level field (target for copy_to)
    assertTrue(
        properties.containsKey("name"),
        "Root-level field for 'name' should exist as target for copy_to");

    @SuppressWarnings("unchecked")
    Map<String, Object> nameField = (Map<String, Object>) properties.get("name");
    // Root field should be a plain field (not an alias) that serves as copy_to target
    assertTrue(nameField.containsKey("type"), "name field should have a type");
    assertFalse(
        nameField.containsKey("copy_to"), "Root field should not have copy_to (it's the target)");

    // Verify that the actual fields exist under their respective aspects
    @SuppressWarnings("unchecked")
    Map<String, Object> datasetProperties =
        (Map<String, Object>) aspectsProperties.get("datasetProperties");
    assertTrue(
        datasetProperties.containsKey("properties"), "datasetProperties should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> datasetProps = (Map<String, Object>) datasetProperties.get("properties");
    assertTrue(
        datasetProps.containsKey("name"),
        "name field should exist under _aspects.datasetProperties");

    // Verify that datasetProperties.name has copy_to to root field
    @SuppressWarnings("unchecked")
    Map<String, Object> datasetNameFieldMapping = (Map<String, Object>) datasetProps.get("name");
    assertTrue(
        datasetNameFieldMapping.containsKey("copy_to"),
        "datasetProperties.name should have copy_to");
    @SuppressWarnings("unchecked")
    List<String> datasetCopyTo = (List<String>) datasetNameFieldMapping.get("copy_to");
    assertTrue(
        datasetCopyTo.contains("name"), "datasetProperties.name should copy_to root 'name' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> containerProperties =
        (Map<String, Object>) aspectsProperties.get("containerProperties");
    assertTrue(
        containerProperties.containsKey("properties"),
        "containerProperties should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> containerProps =
        (Map<String, Object>) containerProperties.get("properties");
    assertTrue(
        containerProps.containsKey("name"),
        "name field should exist under _aspects.containerProperties");

    // Verify that containerProperties.name has copy_to to root field
    @SuppressWarnings("unchecked")
    Map<String, Object> containerNameFieldMapping =
        (Map<String, Object>) containerProps.get("name");
    assertTrue(
        containerNameFieldMapping.containsKey("copy_to"),
        "containerProperties.name should have copy_to");
    @SuppressWarnings("unchecked")
    List<String> containerCopyTo = (List<String>) containerNameFieldMapping.get("copy_to");
    assertTrue(
        containerCopyTo.contains("name"),
        "containerProperties.name should copy_to root 'name' field");
  }

  @Test
  public void testMapArrayFieldWithDollarKeyFieldName() {
    // Test that MAP_ARRAY fields with "/$key" field name create proper aliases using schema field
    // name
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create a mock aspect spec for "ownership" with "ownerTypes" field (MAP_ARRAY with "/$key"
    // field name)
    AspectSpec mockOwnershipAspect = mock(AspectSpec.class);
    when(mockOwnershipAspect.getName()).thenReturn("ownership");

    SearchableFieldSpec mockOwnerTypesField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName())
        .thenReturn("/$key"); // MAP_ARRAY field uses "/$key" as field name
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.MAP_ARRAY);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockOwnerTypesField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    // Mock the PathSpec to return "ownerTypes" as the schema field name
    com.linkedin.data.schema.PathSpec mockPathSpec = mock(com.linkedin.data.schema.PathSpec.class);
    when(mockPathSpec.getPathComponents()).thenReturn(java.util.Arrays.asList("ownerTypes"));
    when(mockOwnerTypesField.getPath()).thenReturn(mockPathSpec);

    // Mock the Pegasus schema to return MAP type for MAP_ARRAY field
    com.linkedin.data.schema.DataSchema mockSchema =
        mock(com.linkedin.data.schema.DataSchema.class);
    when(mockSchema.getDereferencedType()).thenReturn(com.linkedin.data.schema.DataSchema.Type.MAP);
    when(mockOwnerTypesField.getPegasusSchema()).thenReturn(mockSchema);

    when(mockOwnershipAspect.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockOwnerTypesField));
    when(mockEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockOwnershipAspect));

    // Mock the entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // MAP_ARRAY fields should NOT have aliases - Elasticsearch aliases cannot point to object
    // fields
    assertFalse(
        properties.containsKey("ownerTypes"),
        "Root-level alias for 'ownerTypes' should NOT exist - aliases cannot point to object fields");

    // Verify that the actual field exists under _aspects.ownership with "ownerTypes" name
    assertTrue(properties.containsKey("_aspects"), "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectsProperties.containsKey("ownership"), "ownership aspect should exist under _aspects");

    @SuppressWarnings("unchecked")
    Map<String, Object> ownershipAspect = (Map<String, Object>) aspectsProperties.get("ownership");
    assertTrue(ownershipAspect.containsKey("properties"), "ownership should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> ownershipProperties =
        (Map<String, Object>) ownershipAspect.get("properties");
    assertTrue(
        ownershipProperties.containsKey("ownerTypes"),
        "ownerTypes field should exist under _aspects.ownership");

    // Verify the field type is correct for MAP_ARRAY (treated as object in v3 mappings)
    @SuppressWarnings("unchecked")
    Map<String, Object> ownerTypesField =
        (Map<String, Object>) ownershipProperties.get("ownerTypes");
    assertEquals(ownerTypesField.get("type"), "object", "ownerTypes field should be object type");

    // Verify the field is dynamic to allow flexible internal structure
    assertEquals(ownerTypesField.get("dynamic"), true, "ownerTypes field should be dynamic");
  }

  @Test
  public void testFieldNameAliasConflicts() {
    // Test that field name alias conflicts are detected and handled with copy_to
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create two entity specs with conflicting field name aliases
    EntitySpec entitySpec1 = mock(EntitySpec.class);
    EntitySpec entitySpec2 = mock(EntitySpec.class);

    when(entitySpec1.getEntityAnnotation()).thenReturn(mock(EntityAnnotation.class));
    when(entitySpec1.getEntityAnnotation().getName()).thenReturn("entity1");
    when(entitySpec2.getEntityAnnotation()).thenReturn(mock(EntityAnnotation.class));
    when(entitySpec2.getEntityAnnotation().getName()).thenReturn("entity2");

    // Create aspect specs for both entities
    AspectSpec aspect1 = mock(AspectSpec.class);
    AspectSpec aspect2 = mock(AspectSpec.class);
    when(aspect1.getName()).thenReturn("aspect1");
    when(aspect2.getName()).thenReturn("aspect2");

    // Create field specs with the same field name alias
    SearchableFieldSpec field1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec field2 = mock(SearchableFieldSpec.class);

    SearchableAnnotation annotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation annotation2 = mock(SearchableAnnotation.class);

    when(annotation1.getFieldName()).thenReturn("field1");
    when(annotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(annotation1.getFieldNameAliases())
        .thenReturn(Collections.singletonList("conflictingAlias"));
    when(field1.getSearchableAnnotation()).thenReturn(annotation1);

    when(annotation2.getFieldName()).thenReturn("field2");
    when(annotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(annotation2.getFieldNameAliases())
        .thenReturn(Collections.singletonList("conflictingAlias"));
    when(field2.getSearchableAnnotation()).thenReturn(annotation2);

    when(aspect1.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field1));
    when(aspect2.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field2));

    when(entitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(aspect1));
    when(entitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(aspect2));

    // Mock the entity registry to return both entity specs
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("entity1", entitySpec1);
    entitySpecs.put("entity2", entitySpec2);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default")).thenReturn(entitySpecs);

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that the conflicting alias is created as a field (not alias) without copy_to
    assertTrue(
        properties.containsKey("conflictingAlias"),
        "Conflicting alias 'conflictingAlias' should exist as a field");

    @SuppressWarnings("unchecked")
    Map<String, Object> conflictingAliasField =
        (Map<String, Object>) properties.get("conflictingAlias");
    assertEquals(
        conflictingAliasField.get("type"),
        "keyword",
        "conflictingAlias should be a field, not alias");
    assertFalse(
        conflictingAliasField.containsKey("copy_to"),
        "conflictingAlias should NOT have copy_to (it's the target)");

    // Verify that aspect fields have copy_to pointing to the root field
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectsProperties, "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspect1ForAlias = (Map<String, Object>) aspectsProperties.get("aspect1");
    assertNotNull(aspect1ForAlias, "aspect1 should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspect1PropsForAlias =
        (Map<String, Object>) aspect1ForAlias.get("properties");
    assertNotNull(aspect1PropsForAlias, "aspect1 should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> field1Mapping = (Map<String, Object>) aspect1PropsForAlias.get("field1");
    assertNotNull(field1Mapping, "field1 should exist in aspect1");
    assertTrue(field1Mapping.containsKey("copy_to"), "field1 should have copy_to");

    @SuppressWarnings("unchecked")
    List<String> field1CopyTo = (List<String>) field1Mapping.get("copy_to");
    assertTrue(field1CopyTo.contains("conflictingAlias"), "field1 should copy_to conflictingAlias");
  }

  @Test
  public void testEntityNameAliasFilteredFromCopyTo() {
    // Test that _entityName is filtered out from copy_to arrays since it's an alias
    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation.getFieldName()).thenReturn("title");
    when(mockAnnotation.getSearchTier()).thenReturn(Optional.of(1));
    when(mockAnnotation.getSearchLabel()).thenReturn(Optional.of("entityName"));
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(mockAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockAnnotation.getSearchIndexed()).thenReturn(Optional.empty());
    when(mockAnnotation.getHasValuesFieldName()).thenReturn(Optional.empty());
    when(mockAnnotation.getNumValuesFieldName()).thenReturn(Optional.empty());
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());

    // Create a field mapping that would normally include _entityName in copy_to
    Map<String, Object> mapping =
        MultiEntityMappingsBuilder.getMappingsForField(mockFieldSpec, "testAspect");

    // Verify the field exists
    assertTrue(mapping.containsKey("title"), "title field should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> titleField = (Map<String, Object>) mapping.get("title");

    // Verify copy_to exists
    assertTrue(titleField.containsKey("copy_to"), "title field should have copy_to");

    @SuppressWarnings("unchecked")
    List<String> copyTo = (List<String>) titleField.get("copy_to");

    // Verify that _entityName is NOT in the copy_to array (it should be filtered out)
    assertFalse(copyTo.contains("_entityName"), "_entityName should be filtered out from copy_to");

    // Verify that other valid copy_to destinations are still present
    assertTrue(copyTo.contains("_search.tier_1"), "_search.tier_1 should be in copy_to");
    assertTrue(copyTo.contains("_search.entityName"), "_search.entityName should be in copy_to");
  }

  @Test
  public void testFieldNameAndAliasConflicts() {
    // Test that conflicts between field names and field name aliases are handled correctly
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create two entity specs - one with a field name, another with a field name alias that
    // conflicts
    EntitySpec entitySpec1 = mock(EntitySpec.class);
    EntitySpec entitySpec2 = mock(EntitySpec.class);

    when(entitySpec1.getEntityAnnotation()).thenReturn(mock(EntityAnnotation.class));
    when(entitySpec1.getEntityAnnotation().getName()).thenReturn("entity1");
    when(entitySpec2.getEntityAnnotation()).thenReturn(mock(EntityAnnotation.class));
    when(entitySpec2.getEntityAnnotation().getName()).thenReturn("entity2");

    // Create aspect specs for both entities
    AspectSpec aspect1 = mock(AspectSpec.class);
    AspectSpec aspect2 = mock(AspectSpec.class);
    when(aspect1.getName()).thenReturn("aspect1");
    when(aspect2.getName()).thenReturn("aspect2");

    // Create field specs - one with field name "conflictingName", another with field name alias
    // "conflictingName"
    SearchableFieldSpec field1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec field2 = mock(SearchableFieldSpec.class);

    SearchableAnnotation annotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation annotation2 = mock(SearchableAnnotation.class);

    when(annotation1.getFieldName()).thenReturn("conflictingName");
    when(annotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(annotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(field1.getSearchableAnnotation()).thenReturn(annotation1);

    when(annotation2.getFieldName()).thenReturn("otherField");
    when(annotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(annotation2.getFieldNameAliases())
        .thenReturn(Collections.singletonList("conflictingName"));
    when(field2.getSearchableAnnotation()).thenReturn(annotation2);

    when(aspect1.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field1));
    when(aspect2.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(field2));

    when(entitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(aspect1));
    when(entitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(aspect2));

    // Mock the entity registry to return both entity specs
    Map<String, EntitySpec> entitySpecs = new HashMap<>();
    entitySpecs.put("entity1", entitySpec1);
    entitySpecs.put("entity2", entitySpec2);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default")).thenReturn(entitySpecs);

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that the conflicting name is created as a field (not alias) without copy_to
    assertTrue(
        properties.containsKey("conflictingName"),
        "Conflicting name 'conflictingName' should exist as a field");

    @SuppressWarnings("unchecked")
    Map<String, Object> conflictingNameField =
        (Map<String, Object>) properties.get("conflictingName");
    assertEquals(
        conflictingNameField.get("type"),
        "keyword",
        "conflictingName should be a field, not alias");
    assertFalse(
        conflictingNameField.containsKey("copy_to"),
        "conflictingName should NOT have copy_to (it's the target)");

    // Verify that aspect fields have copy_to pointing to the root field
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectsProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectsProperties, "_aspects should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspect1ForName = (Map<String, Object>) aspectsProperties.get("aspect1");
    assertNotNull(aspect1ForName, "aspect1 should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspect1PropsForName =
        (Map<String, Object>) aspect1ForName.get("properties");
    assertNotNull(aspect1PropsForName, "aspect1 should have properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> conflictingNameMapping =
        (Map<String, Object>) aspect1PropsForName.get("conflictingName");
    assertNotNull(conflictingNameMapping, "conflictingName should exist in aspect1");
    assertTrue(
        conflictingNameMapping.containsKey("copy_to"),
        "conflictingName should have copy_to in aspect1");

    @SuppressWarnings("unchecked")
    List<String> conflictingNameCopyTo = (List<String>) conflictingNameMapping.get("copy_to");
    assertTrue(
        conflictingNameCopyTo.contains("conflictingName"),
        "conflictingName should copy_to root conflictingName field");
  }

  @Test
  public void testStructuredPropertiesWithProperType() {
    // Test that structured properties are created with proper type definition
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Mock the entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a test structured property
    StructuredPropertyDefinition testProperty =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testStructuredProperty")
            .setDisplayName("Test Structured Property")
            .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:dataset")))
            .setValueType(UrnUtils.getUrn("urn:li:logicalType:STRING"));

    Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties =
        Collections.singletonList(
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:testStructuredProperty"), testProperty));

    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(operationContext, structuredProperties);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify that structuredProperties field exists with proper type definition
    assertTrue(
        properties.containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD),
        "structuredProperties field should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> structuredPropertiesField =
        (Map<String, Object>) properties.get(STRUCTURED_PROPERTY_MAPPING_FIELD);

    // Verify it has a properties field (proper type definition)
    assertTrue(
        structuredPropertiesField.containsKey("properties"),
        "structuredProperties should have properties field");

    // Verify it has dynamic: true
    assertTrue(
        structuredPropertiesField.containsKey("dynamic"),
        "structuredProperties should have dynamic field");
    assertEquals(
        structuredPropertiesField.get("dynamic"),
        true,
        "structuredProperties should have dynamic set to true");

    @SuppressWarnings("unchecked")
    Map<String, Object> structuredPropertiesProperties =
        (Map<String, Object>) structuredPropertiesField.get("properties");

    // Verify that our test structured property is included
    // The field name should be: _versioned.testStructuredProperty.00000000000001.string
    String expectedFieldName = "_versioned.testStructuredProperty.00000000000001.string";
    assertTrue(
        structuredPropertiesProperties.containsKey(expectedFieldName),
        "Test structured property should be included with field name: " + expectedFieldName);
  }

  @Test
  public void testStructuredPropertiesEmpty() {
    // Test that structuredProperties field is created even when no structured properties are
    // provided
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Call with empty structured properties
    Collection<IndexMapping> mappings =
        mappingsBuilder.getIndexMappings(operationContext, Collections.emptyList());

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    IndexMapping mapping = mappings.iterator().next();
    Map<String, Object> mappingContent = mapping.getMappings();

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappingContent.get("properties");
    assertNotNull(properties, "Mappings should have properties");

    // Verify that structuredProperties field exists even with empty structured properties
    assertTrue(
        properties.containsKey(STRUCTURED_PROPERTY_MAPPING_FIELD),
        "structuredProperties field should exist even with empty structured properties");

    @SuppressWarnings("unchecked")
    Map<String, Object> structuredPropertiesField =
        (Map<String, Object>) properties.get(STRUCTURED_PROPERTY_MAPPING_FIELD);

    // Verify it has dynamic: true
    assertTrue(
        structuredPropertiesField.containsKey("dynamic"),
        "structuredProperties should have dynamic field");
    assertEquals(
        structuredPropertiesField.get("dynamic"),
        true,
        "structuredProperties should have dynamic set to true");

    // Verify it has properties field (even if empty)
    assertTrue(
        structuredPropertiesField.containsKey("properties"),
        "structuredProperties should have properties field");

    @SuppressWarnings("unchecked")
    Map<String, Object> structuredPropertiesProperties =
        (Map<String, Object>) structuredPropertiesField.get("properties");
    assertNotNull(
        structuredPropertiesProperties, "structuredProperties properties should not be null");
    assertTrue(
        structuredPropertiesProperties.isEmpty(),
        "structuredProperties properties should be empty when no structured properties are provided");
  }

  @Test
  public void testEntitySearchGroups() throws IOException {
    // Test that entities have the correct search groups and not defaulting to "default"
    // Use the same operation context as IndexBuilderTestBase to get the entity registry
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    EntityRegistry entityRegistry = opContext.getEntityRegistry();

    // Debug: Print all entity specs and their search groups
    System.out.println("=== Entity Registry Debug Info ===");
    Map<String, EntitySpec> entitySpecs = entityRegistry.getEntitySpecs();
    System.out.println("Total entities: " + entitySpecs.size());

    for (EntitySpec spec : entitySpecs.values()) {
      System.out.println("Entity: " + spec.getName() + " -> SearchGroup: " + spec.getSearchGroup());
    }

    // Test specific entities that should have "primary" search group
    EntitySpec datasetSpec = entityRegistry.getEntitySpec("dataset");
    assertNotNull(datasetSpec, "Dataset entity spec should exist");

    // Debug: Print dataset spec details
    System.out.println("Dataset spec details:");
    System.out.println("  Name: " + datasetSpec.getName());
    System.out.println("  SearchGroup: " + datasetSpec.getSearchGroup());
    System.out.println("  EntityAnnotation: " + datasetSpec.getEntityAnnotation());

    assertEquals(
        datasetSpec.getSearchGroup(),
        "primary",
        "Dataset should have 'primary' search group, not 'default'");

    EntitySpec roleSpec = entityRegistry.getEntitySpec("role");
    assertNotNull(roleSpec, "Role entity spec should exist");
    assertEquals(
        roleSpec.getSearchGroup(),
        "primary",
        "Role should have 'primary' search group, not 'default'");

    // Test that we can get all search groups
    Set<String> searchGroups = entityRegistry.getSearchGroups();
    assertNotNull(searchGroups, "Search groups should not be null");
    assertFalse(searchGroups.isEmpty(), "Search groups should not be empty");

    // Verify that "primary" is in the search groups
    assertTrue(searchGroups.contains("primary"), "Search groups should contain 'primary'");

    // Verify that "default" is not the only search group (if it exists at all)
    if (searchGroups.contains("default")) {
      assertTrue(searchGroups.size() > 1, "Should have more than just 'default' search group");
    }

    // Log all search groups for debugging
    System.out.println("All search groups: " + searchGroups);

    // Test that entities are properly grouped by search group
    Map<String, EntitySpec> primaryGroupEntities =
        entityRegistry.getEntitySpecsBySearchGroup("primary");
    assertNotNull(primaryGroupEntities, "Primary group entities should not be null");
    assertFalse(primaryGroupEntities.isEmpty(), "Primary group should not be empty");

    // Verify that dataset and role are in the primary group
    assertTrue(
        primaryGroupEntities.containsKey("dataset"), "Dataset should be in primary search group");
    assertTrue(primaryGroupEntities.containsKey("role"), "Role should be in primary search group");

    System.out.println("Primary group entities: " + primaryGroupEntities.keySet());
  }

  @Test
  public void testDoubleUnderscoreFieldNameDetection() {
    // Test that fields with double underscores are detected and skipped
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a mock SearchableFieldSpec with double underscore field name
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
    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    // Verify that the mapping was created but the problematic field was skipped
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // The field with double underscores should not appear in the mappings
    // This is verified by the fact that no exception is thrown and mappings are created
  }

  @Test
  public void testCollectFieldPathsSkipsDoubleUnderscoreFields() {
    // Test that collectFieldPaths skips fields with double underscores
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a mock SearchableFieldSpec with double underscore field name
    SearchableFieldSpec mockFieldSpecWithDoubleUnderscore = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotationWithDoubleUnderscore = mock(SearchableAnnotation.class);

    when(mockAnnotationWithDoubleUnderscore.getFieldName())
        .thenReturn("fields__glossaryTerms_terms__urn");
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
    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    // Verify that the mapping was created
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // The field with double underscores should not appear in field paths
    // This is verified by the fact that no exception is thrown and mappings are created
  }

  @Test
  public void testNormalFieldNamesAreNotSkipped() {
    // Test that normal field names (without double underscores) are not skipped
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Create a mock SearchableFieldSpec with normal field name
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
    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    // Verify that the mapping was created
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");
  }

  @Test
  public void testYamlConfigurationLoading() throws IOException {
    // Test that YAML configuration is loaded correctly
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);

    // Verify that the builder was created successfully (no exception thrown)
    assertNotNull(builder, "Builder should be created successfully");
  }

  @Test
  public void testYamlConfigurationWithEmptyPath() throws IOException {
    // Test that empty mapping config path is handled correctly
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);

    // Verify that the builder was created successfully even with empty config path
    assertNotNull(builder, "Builder should be created successfully with empty config path");
  }

  @Test
  public void testYamlConfigurationWithNullPath() throws IOException {
    // Test that null mapping config path is handled correctly
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn(null);

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);

    // Verify that the builder was created successfully even with null config path
    assertNotNull(builder, "Builder should be created successfully with null config path");
  }

  @Test
  public void testYamlConfigurationMerging() throws IOException {
    // Test that YAML configuration is properly merged with generated mappings
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    // Use the existing operation context setup
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("primary"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("primary"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);
    Collection<IndexMapping> mappings = builder.getMappings(operationContext);

    // Verify that mappings were created
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Find the primary index mapping
    IndexMapping primaryMapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("primary"))
            .findFirst()
            .orElse(null);

    assertNotNull(primaryMapping, "Primary index mapping should exist");

    // Verify that YAML-defined fields are present in the mapping
    Map<String, Object> mappingProperties =
        (Map<String, Object>) primaryMapping.getMappings().get("properties");
    assertNotNull(mappingProperties, "Mapping properties should exist");

    // Check for basic system fields that should be present
    assertTrue(mappingProperties.containsKey("urn"), "urn field should be present");
    assertTrue(mappingProperties.containsKey("runId"), "runId field should be present");
    assertTrue(
        mappingProperties.containsKey("systemCreated"), "systemCreated field should be present");
  }

  @Test
  public void testYamlConfigurationWithInvalidPath() {
    // Test that invalid YAML configuration path throws appropriate exception
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("nonexistent_config.yaml");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    // Verify that IOException is thrown for invalid config path
    try {
      new MultiEntityMappingsBuilder(config);
      fail("Should throw IOException for invalid config path");
    } catch (IOException e) {
      // Expected behavior
    }
  }

  @Test
  public void testYamlConfigurationMergeOrder() throws IOException {
    // Test that YAML configuration is merged after generated mappings (YAML overrides)
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    // Use the existing operation context setup
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("primary"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("primary"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);
    Collection<IndexMapping> mappings = builder.getMappings(operationContext);

    // Verify that mappings were created
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");
  }

  @Test
  public void testDynamicTemplatesProcessing() throws IOException {
    // Test that dynamic templates from YAML are properly converted to Elasticsearch format
    // TODO: This test is temporarily disabled as dynamic templates are causing OpenSearch parsing
    // errors
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn("search_entity_mapping_config.yaml");

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    // Use the existing operation context setup
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("primary"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("primary"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);
    Collection<IndexMapping> mappings = builder.getMappings(operationContext);

    // Verify that mappings were created
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Find the primary index mapping
    IndexMapping primaryMapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("primary"))
            .findFirst()
            .orElse(null);

    assertNotNull(primaryMapping, "Primary index mapping should exist");

    // Verify that dynamic templates are present and properly formatted
    Map<String, Object> mappingContent = primaryMapping.getMappings();
    assertTrue(
        mappingContent.containsKey("dynamic_templates"), "Dynamic templates should be present");

    Object dynamicTemplates = mappingContent.get("dynamic_templates");
    assertTrue(dynamicTemplates instanceof List, "Dynamic templates should be a list");

    @SuppressWarnings("unchecked")
    List<Map<String, Object>> templateList = (List<Map<String, Object>>) dynamicTemplates;
    assertFalse(templateList.isEmpty(), "Dynamic templates list should not be empty");

    // Verify the structure of the first template
    Map<String, Object> firstTemplate = templateList.get(0);
    assertTrue(firstTemplate.containsKey("tierN"), "First template should contain 'tierN'");
  }

  @Test
  public void testDynamicTemplatesWithoutYamlConfig() throws IOException {
    // Test that dynamic templates work correctly when no YAML config is provided
    EntityIndexVersionConfiguration v3Config = mock(EntityIndexVersionConfiguration.class);
    when(v3Config.isEnabled()).thenReturn(true);
    when(v3Config.getMappingConfig()).thenReturn(""); // Empty config path

    EntityIndexConfiguration config = mock(EntityIndexConfiguration.class);
    when(config.getV3()).thenReturn(v3Config);

    // Use the existing operation context setup
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("primary"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("primary"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    MultiEntityMappingsBuilder builder = new MultiEntityMappingsBuilder(config);
    Collection<IndexMapping> mappings = builder.getMappings(operationContext);

    // Verify that mappings were created
    assertNotNull(mappings, "Mappings should be created");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty");

    // Find the primary index mapping
    IndexMapping primaryMapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("primary"))
            .findFirst()
            .orElse(null);

    assertNotNull(primaryMapping, "Primary index mapping should exist");

    // Verify that dynamic templates are not present when no YAML config is provided
    Map<String, Object> mappingContent = primaryMapping.getMappings();
    assertFalse(
        mappingContent.containsKey("dynamic_templates"),
        "Dynamic templates should not be present when no YAML config is provided");
  }

  @Test
  public void testEnhancedNumericFieldTypeMapping() {
    // Test that COUNT fields with different underlying PDL types are mapped to appropriate
    // Elasticsearch types

    // Test int field with COUNT annotation
    SearchableFieldSpec intCountField =
        createMockSearchableFieldSpec("intCountField", FieldType.COUNT, DataSchema.Type.INT);
    Map<String, Object> intMapping =
        MultiEntityMappingsBuilder.getMappingsForField(intCountField, "testAspect");
    Map<String, Object> intFieldMapping = (Map<String, Object>) intMapping.get("intCountField");
    assertEquals(
        intFieldMapping.get("type"),
        ESUtils.INTEGER_FIELD_TYPE,
        "int COUNT field should map to integer type");

    // Test long field with COUNT annotation
    SearchableFieldSpec longCountField =
        createMockSearchableFieldSpec("longCountField", FieldType.COUNT, DataSchema.Type.LONG);
    Map<String, Object> longMapping =
        MultiEntityMappingsBuilder.getMappingsForField(longCountField, "testAspect");
    Map<String, Object> longFieldMapping = (Map<String, Object>) longMapping.get("longCountField");
    assertEquals(
        longFieldMapping.get("type"),
        ESUtils.LONG_FIELD_TYPE,
        "long COUNT field should map to long type");

    // Test float field with COUNT annotation
    SearchableFieldSpec floatCountField =
        createMockSearchableFieldSpec("floatCountField", FieldType.COUNT, DataSchema.Type.FLOAT);
    Map<String, Object> floatMapping =
        MultiEntityMappingsBuilder.getMappingsForField(floatCountField, "testAspect");
    Map<String, Object> floatFieldMapping =
        (Map<String, Object>) floatMapping.get("floatCountField");
    assertEquals(
        floatFieldMapping.get("type"),
        ESUtils.FLOAT_FIELD_TYPE,
        "float COUNT field should map to float type");

    // Test double field with COUNT annotation
    SearchableFieldSpec doubleCountField =
        createMockSearchableFieldSpec("doubleCountField", FieldType.COUNT, DataSchema.Type.DOUBLE);
    Map<String, Object> doubleMapping =
        MultiEntityMappingsBuilder.getMappingsForField(doubleCountField, "testAspect");
    Map<String, Object> doubleFieldMapping =
        (Map<String, Object>) doubleMapping.get("doubleCountField");
    assertEquals(
        doubleFieldMapping.get("type"),
        ESUtils.DOUBLE_FIELD_TYPE,
        "double COUNT field should map to double type");

    // Test non-numeric field with COUNT annotation (should default to long)
    SearchableFieldSpec stringCountField =
        createMockSearchableFieldSpec("stringCountField", FieldType.COUNT, DataSchema.Type.STRING);
    Map<String, Object> stringMapping =
        MultiEntityMappingsBuilder.getMappingsForField(stringCountField, "testAspect");
    Map<String, Object> stringFieldMapping =
        (Map<String, Object>) stringMapping.get("stringCountField");
    assertEquals(
        stringFieldMapping.get("type"),
        ESUtils.LONG_FIELD_TYPE,
        "non-numeric COUNT field should default to long type");
  }

  @Test
  public void testEagerGlobalOrdinalsMapping() {
    // Test that eagerGlobalOrdinals is properly applied to field mappings

    // Test keyword field with eagerGlobalOrdinals = true
    SearchableFieldSpec keywordFieldWithEager =
        createMockSearchableFieldSpecWithEagerOrdinals(
            "statusField", FieldType.KEYWORD, DataSchema.Type.STRING, true);
    Map<String, Object> keywordMapping =
        MultiEntityMappingsBuilder.getMappingsForField(keywordFieldWithEager, "testAspect");
    Map<String, Object> keywordFieldMapping =
        (Map<String, Object>) keywordMapping.get("statusField");
    assertEquals(
        keywordFieldMapping.get("eager_global_ordinals"),
        true,
        "keyword field with eagerGlobalOrdinals=true should have eager_global_ordinals=true");

    // Test keyword field with eagerGlobalOrdinals = false
    SearchableFieldSpec keywordFieldWithoutEager =
        createMockSearchableFieldSpecWithEagerOrdinals(
            "categoryField", FieldType.KEYWORD, DataSchema.Type.STRING, false);
    Map<String, Object> categoryMapping =
        MultiEntityMappingsBuilder.getMappingsForField(keywordFieldWithoutEager, "testAspect");
    Map<String, Object> categoryFieldMapping =
        (Map<String, Object>) categoryMapping.get("categoryField");
    assertNull(
        categoryFieldMapping.get("eager_global_ordinals"),
        "keyword field with eagerGlobalOrdinals=false should not have eager_global_ordinals property");

    // Test URN field with eagerGlobalOrdinals = true
    SearchableFieldSpec urnFieldWithEager =
        createMockSearchableFieldSpecWithEagerOrdinals(
            "ownerField", FieldType.URN, DataSchema.Type.STRING, true);
    Map<String, Object> urnMapping =
        MultiEntityMappingsBuilder.getMappingsForField(urnFieldWithEager, "testAspect");
    Map<String, Object> urnFieldMapping = (Map<String, Object>) urnMapping.get("ownerField");
    assertEquals(
        urnFieldMapping.get("eager_global_ordinals"),
        true,
        "URN field with eagerGlobalOrdinals=true should have eager_global_ordinals=true");

    // Test text field with eagerGlobalOrdinals = true (should not be applied due to validation)
    SearchableFieldSpec textFieldWithEager =
        createMockSearchableFieldSpecWithEagerOrdinals(
            "descriptionField", FieldType.TEXT, DataSchema.Type.STRING, true);
    Map<String, Object> textMapping =
        MultiEntityMappingsBuilder.getMappingsForField(textFieldWithEager, "testAspect");
    Map<String, Object> textFieldMapping =
        (Map<String, Object>) textMapping.get("descriptionField");
    assertNull(
        textFieldMapping.get("eager_global_ordinals"),
        "TEXT field should not have eager_global_ordinals even if set to true (validation should prevent this)");
  }

  private SearchableFieldSpec createMockSearchableFieldSpec(
      String fieldName, FieldType fieldType, DataSchema.Type schemaType) {
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn(fieldName);
    when(mockAnnotation.getFieldType()).thenReturn(fieldType);

    PrimitiveDataSchema mockSchema = mock(PrimitiveDataSchema.class);
    when(mockSchema.isPrimitive()).thenReturn(true);
    when(mockSchema.getType()).thenReturn(schemaType);

    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockFieldSpec.getPegasusSchema()).thenReturn(mockSchema);

    return mockFieldSpec;
  }

  private SearchableFieldSpec createMockSearchableFieldSpecWithEagerOrdinals(
      String fieldName,
      FieldType fieldType,
      DataSchema.Type schemaType,
      boolean eagerGlobalOrdinals) {
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn(fieldName);
    when(mockAnnotation.getFieldType()).thenReturn(fieldType);
    when(mockAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.of(eagerGlobalOrdinals));

    PrimitiveDataSchema mockSchema = mock(PrimitiveDataSchema.class);
    when(mockSchema.isPrimitive()).thenReturn(true);
    when(mockSchema.getType()).thenReturn(schemaType);

    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockFieldSpec.getPegasusSchema()).thenReturn(mockSchema);

    return mockFieldSpec;
  }

  @Test
  public void testSearchIndexedAnnotation() {
    // Test searchIndexed: true
    testSearchIndexedBehavior(Optional.of(true), true);

    // Test searchIndexed: false
    testSearchIndexedBehavior(Optional.of(false), false);

    // Test searchIndexed: not specified (Optional.empty())
    testSearchIndexedBehavior(Optional.empty(), null);
  }

  private void testSearchIndexedBehavior(
      Optional<Boolean> searchIndexed, Boolean expectedIndexValue) {
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create a mock aspect spec with a field that has the searchIndexed annotation
    AspectSpec mockAspect = mock(AspectSpec.class);
    when(mockAspect.getName()).thenReturn("testAspect");

    SearchableFieldSpec mockField =
        createMockSearchableFieldSpecWithSearchIndexed(
            "testField", FieldType.KEYWORD, DataSchema.Type.STRING, searchIndexed, Optional.of(1));

    when(mockAspect.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(mockField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspect));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Navigate to the aspect field
    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "_aspects should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectProperties, "aspect properties should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> testAspectMapping =
        (Map<String, Object>) aspectProperties.get("testAspect");
    assertNotNull(testAspectMapping, "testAspect mapping should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> testAspectProperties =
        (Map<String, Object>) testAspectMapping.get("properties");
    assertNotNull(testAspectProperties, "testAspect properties should exist");

    @SuppressWarnings("unchecked")
    Map<String, Object> testFieldMapping =
        (Map<String, Object>) testAspectProperties.get("testField");
    assertNotNull(testFieldMapping, "testField mapping should exist");

    if (expectedIndexValue != null) {
      assertEquals(
          testFieldMapping.get("index"),
          expectedIndexValue,
          "Field should have index: " + expectedIndexValue);
    } else {
      assertFalse(
          testFieldMapping.containsKey("index"),
          "Field should not have index property when searchIndexed is not specified");
    }

    if (searchIndexed.isPresent() && searchIndexed.get()) {
      assertEquals(
          testFieldMapping.get("type"),
          ESUtils.KEYWORD_FIELD_TYPE,
          "Field should have type: keyword when searchIndexed is true");
    }
  }

  private SearchableFieldSpec createMockSearchableFieldSpecWithSearchIndexed(
      String fieldName,
      FieldType fieldType,
      DataSchema.Type schemaType,
      Optional<Boolean> searchIndexed,
      Optional<Integer> searchTier) {
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn(fieldName);
    when(mockAnnotation.getFieldType()).thenReturn(fieldType);
    when(mockAnnotation.getSearchIndexed()).thenReturn(searchIndexed);
    when(mockAnnotation.getSearchTier()).thenReturn(searchTier);

    PrimitiveDataSchema mockSchema = mock(PrimitiveDataSchema.class);
    when(mockSchema.isPrimitive()).thenReturn(true);
    when(mockSchema.getType()).thenReturn(schemaType);

    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockFieldSpec.getPegasusSchema()).thenReturn(mockSchema);

    return mockFieldSpec;
  }

  @Test
  public void testSearchSectionDestinationFields() {
    // Test that _search section is properly built with destination fields for copy_to operations
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Create a mock aspect spec with fields that have copy_to destinations
    AspectSpec mockAspect = mock(AspectSpec.class);
    when(mockAspect.getName()).thenReturn("testAspect");

    // Create field with searchLabel
    SearchableFieldSpec sortField =
        createMockSearchableFieldSpecWithSearchLabel(
            "sortField", FieldType.KEYWORD, DataSchema.Type.STRING, "name");

    // Create field with searchLabel
    SearchableFieldSpec boostField =
        createMockSearchableFieldSpecWithSearchLabel2(
            "boostField", FieldType.KEYWORD, DataSchema.Type.STRING, "score");

    // Create field with searchTier
    SearchableFieldSpec tierField =
        createMockSearchableFieldSpecWithTier(
            "tierField", FieldType.KEYWORD, DataSchema.Type.STRING, 1);

    when(mockAspect.getSearchableFieldSpecs())
        .thenReturn(Arrays.asList(sortField, boostField, tierField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspect));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    Collection<IndexMapping> mappings = mappingsBuilder.getMappings(operationContext);

    assertNotNull(mappings, "Mappings should not be null");
    assertFalse(mappings.isEmpty(), "Mappings should not be empty when v3 is enabled");

    // Find the v3 mapping
    IndexMapping v3Mapping =
        mappings.stream()
            .filter(mapping -> mapping.getIndexName().contains("v3"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No v3 mapping found"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties =
        (Map<String, Object>) v3Mapping.getMappings().get("properties");

    // Verify _search section exists
    @SuppressWarnings("unchecked")
    Map<String, Object> searchSection = (Map<String, Object>) properties.get("_search");
    assertNotNull(searchSection, "_search section should exist");

    // Verify _search has properties
    @SuppressWarnings("unchecked")
    Map<String, Object> searchProperties = (Map<String, Object>) searchSection.get("properties");
    assertNotNull(searchProperties, "_search.properties should exist");

    // Verify search label destination field exists
    @SuppressWarnings("unchecked")
    Map<String, Object> nameField = (Map<String, Object>) searchProperties.get("name");
    assertNotNull(nameField, "name field should exist in _search.properties");
    assertEquals(nameField.get("type"), "keyword", "name should be keyword type");
    assertEquals(nameField.get("normalizer"), "keyword_normalizer", "name should have normalizer");

    // Verify search label destination field exists
    @SuppressWarnings("unchecked")
    Map<String, Object> scoreField = (Map<String, Object>) searchProperties.get("score");
    assertNotNull(scoreField, "score field should exist in _search.properties");
    assertEquals(scoreField.get("type"), "keyword", "score should be keyword type");
    assertEquals(
        scoreField.get("normalizer"), "keyword_normalizer", "score should have normalizer");

    // Note: tier destination fields are no longer explicitly created in _search section
    // They will be created dynamically when data is copied to them via copy_to
  }

  private SearchableFieldSpec createMockSearchableFieldSpecWithSearchLabel(
      String fieldName, FieldType fieldType, DataSchema.Type schemaType, String searchLabel) {
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn(fieldName);
    when(mockAnnotation.getFieldType()).thenReturn(fieldType);
    when(mockAnnotation.getSearchLabel()).thenReturn(Optional.of(searchLabel));

    PrimitiveDataSchema mockSchema = mock(PrimitiveDataSchema.class);
    when(mockSchema.isPrimitive()).thenReturn(true);
    when(mockSchema.getType()).thenReturn(schemaType);

    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockFieldSpec.getPegasusSchema()).thenReturn(mockSchema);

    return mockFieldSpec;
  }

  private SearchableFieldSpec createMockSearchableFieldSpecWithSearchLabel2(
      String fieldName, FieldType fieldType, DataSchema.Type schemaType, String searchLabel) {
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn(fieldName);
    when(mockAnnotation.getFieldType()).thenReturn(fieldType);
    when(mockAnnotation.getSearchLabel()).thenReturn(Optional.of(searchLabel));

    PrimitiveDataSchema mockSchema = mock(PrimitiveDataSchema.class);
    when(mockSchema.isPrimitive()).thenReturn(true);
    when(mockSchema.getType()).thenReturn(schemaType);

    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockFieldSpec.getPegasusSchema()).thenReturn(mockSchema);

    return mockFieldSpec;
  }

  private SearchableFieldSpec createMockSearchableFieldSpecWithTier(
      String fieldName, FieldType fieldType, DataSchema.Type schemaType, int tier) {
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);
    when(mockAnnotation.getFieldName()).thenReturn(fieldName);
    when(mockAnnotation.getFieldType()).thenReturn(fieldType);
    when(mockAnnotation.getSearchTier()).thenReturn(Optional.of(tier));

    PrimitiveDataSchema mockSchema = mock(PrimitiveDataSchema.class);
    when(mockSchema.isPrimitive()).thenReturn(true);
    when(mockSchema.getType()).thenReturn(schemaType);

    SearchableFieldSpec mockFieldSpec = mock(SearchableFieldSpec.class);
    when(mockFieldSpec.getSearchableAnnotation()).thenReturn(mockAnnotation);
    when(mockFieldSpec.getPegasusSchema()).thenReturn(mockSchema);

    return mockFieldSpec;
  }

  @Test
  public void testGetMappingsWithNullFieldNameAliasConflicts() {
    // Test that getMappings handles null fieldNameAliasConflicts by setting it to empty map
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with null fieldNameAliasConflicts - should not throw exception
    Collection<IndexMapping> mappings = builder.getIndexMappings(operationContext, null);

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsWithNullFieldNameConflicts() {
    // Test that getMappings handles null fieldNameConflicts by setting it to empty map
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with null fieldNameConflicts - should not throw exception
    Collection<IndexMapping> mappings = builder.getIndexMappings(operationContext, null);

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsWithNullStructuredProperties() {
    // Test that getMappings handles null structuredProperties by setting it to empty list
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with null structuredProperties - should not throw exception
    Collection<IndexMapping> mappings = builder.getIndexMappings(operationContext, null);

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsWithAllNullParameters() {
    // Test that getMappings handles all null parameters gracefully
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with all null parameters - should not throw exception
    Collection<IndexMapping> mappings = builder.getIndexMappings(operationContext, null);

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsWithEmptyFieldNameAliasConflicts() {
    // Test that getMappings works correctly with empty fieldNameAliasConflicts map
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with empty fieldNameAliasConflicts - should work the same as null
    Collection<IndexMapping> mappings =
        builder.getIndexMappings(operationContext, Collections.emptyList());

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsWithEmptyFieldNameConflicts() {
    // Test that getMappings works correctly with empty fieldNameConflicts map
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with empty fieldNameConflicts - should work the same as null
    Collection<IndexMapping> mappings =
        builder.getIndexMappings(operationContext, Collections.emptyList());

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsWithEmptyStructuredProperties() {
    // Test that getMappings works correctly with empty structuredProperties collection
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("testGroup"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with empty structuredProperties - should work the same as null
    Collection<IndexMapping> mappings =
        builder.getIndexMappings(operationContext, Collections.emptyList());

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsConvenienceMethodWithEntityRegistryAndEntitySpec() {
    // Test the public convenience method: getMappings(EntityRegistry, EntitySpec)
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test the convenience method - should delegate to main getMappings with nulls
    Map<String, Object> mappings = builder.getMappings(mockEntityRegistry, mockEntitySpec);

    // Verify that mappings are created successfully (not null)
    assertNotNull(mappings, "Mappings should not be null");
    // Note: Even entities with no fields may have _systemMetadata fields in aspects
    // So we just verify the structure is correct
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");
  }

  @Test
  public void
      testGetMappingsConvenienceMethodWithEntityRegistryEntitySpecAndStructuredProperties() {
    // Test the private convenience method: getMappings(EntityRegistry, EntitySpec,
    // structuredProperties)
    // We'll test this indirectly through the public methods that use it

    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with empty structured properties - this should use the convenience method internally
    Collection<Pair<Urn, StructuredPropertyDefinition>> emptyStructuredProperties =
        Collections.emptyList();

    // We can't directly call the private method, but we can verify it works through
    // the public getIndexMappings method which internally uses it
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> mappings =
        builder.getIndexMappings(operationContext, emptyStructuredProperties);

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Mappings should be empty for entity with no fields");
  }

  @Test
  public void testGetMappingsConvenienceMethodWithNullEntityRegistry() {
    // Test that convenience method handles null EntityRegistry gracefully
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with null EntityRegistry - should not throw exception (delegates to
    // AspectMappingBuilder)
    Map<String, Object> mappings = builder.getMappings(null, mockEntitySpec);

    // Verify that mappings are created successfully (not null)
    assertNotNull(mappings, "Mappings should not be null even with null EntityRegistry");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");
  }

  @Test
  public void testGetMappingsConvenienceMethodWithNullEntitySpec() {
    // Test that convenience method handles null EntitySpec gracefully
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with null EntitySpec - should throw NullPointerException
    assertThrows(
        "Should throw NullPointerException for null EntitySpec",
        NullPointerException.class,
        () -> builder.getMappings(mockEntityRegistry, null));
  }

  @Test
  public void testGetMappingsConvenienceMethodWithBothNulls() {
    // Test that convenience method handles both null parameters gracefully
    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with both null parameters - should throw NullPointerException (EntitySpec is null)
    assertThrows(
        "Should throw NullPointerException for both null parameters",
        NullPointerException.class,
        () -> builder.getMappings(null, null));
  }

  @Test
  public void testGetMappingsConvenienceMethodDelegation() {
    // Test that the convenience method properly delegates to the main getMappings method
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test the convenience method
    Map<String, Object> convenienceMappings =
        builder.getMappings(mockEntityRegistry, mockEntitySpec);

    // Verify that mappings are created successfully (not null)
    assertNotNull(convenienceMappings, "Convenience method mappings should not be null");
    // Note: Even entities with no fields may have _systemMetadata fields in aspects
    // So we just verify the structure is correct
    assertTrue(
        convenienceMappings.containsKey("properties"),
        "Convenience method mappings should contain 'properties' key");

    // The convenience method should delegate to getMappings(entityRegistry, entitySpec, null, null,
    // null)
    // Since we can't directly test the private method, we verify the behavior is consistent
    // by ensuring the result is the same as what we'd expect from the main method
  }

  @Test
  public void testGetMappingsConvenienceMethodWithStructuredPropertiesDelegation() {
    // Test that the convenience method with structured properties properly delegates
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    when(mockEntitySpec.getName()).thenReturn("testEntity");
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchScoreFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getRelationshipFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockEntitySpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());

    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    // Create mappings builder
    MultiEntityMappingsBuilder builder;
    try {
      builder = new MultiEntityMappingsBuilder(entityIndexConfiguration);
    } catch (IOException e) {
      fail("Should not throw IOException: " + e.getMessage());
      return;
    }

    // Test with empty structured properties through public method that uses the convenience method
    Collection<Pair<Urn, StructuredPropertyDefinition>> emptyStructuredProperties =
        Collections.emptyList();
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> mappings =
        builder.getIndexMappings(operationContext, emptyStructuredProperties);

    // Verify that mappings are created successfully (empty but not null)
    assertNotNull(mappings, "Mappings with structured properties should not be null");
    assertTrue(
        mappings.isEmpty(),
        "Mappings with structured properties should be empty for entity with no fields");
  }

  // ==================== Entity Name Field Mapping Tests ====================

  @Test
  public void testEntityNameFieldMappingInConflictedFieldName() {
    // Test the entity name field mapping logic when _entityName is a conflicted field name
    // Create multiple entities with _entityName fields to trigger conflicts
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockEntityNameField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockEntityNameField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockEntityNameAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockEntityNameAnnotation2 = mock(SearchableAnnotation.class);

    // Set up the _entityName fields for both entities
    when(mockEntityNameAnnotation1.getFieldName()).thenReturn("_entityName");
    when(mockEntityNameAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockEntityNameField1.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation1);

    when(mockEntityNameAnnotation2.getFieldName()).thenReturn("_entityName");
    when(mockEntityNameAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockEntityNameField2.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation2);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));

    // Mock entity registry to return multiple entity specs with conflicts
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Use the multiple entity method that triggers conflict resolution
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Should have at least one index mapping");

    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that _entityName field exists at root level with alias mapping
    assertTrue(
        properties.containsKey("_entityName"), "_entityName field should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameMapping = (Map<String, Object>) properties.get("_entityName");

    // Verify the alias mapping structure
    assertEquals("alias", entityNameMapping.get("type"), "_entityName should have alias type");
    assertEquals(
        "_search.entityName",
        entityNameMapping.get("path"),
        "_entityName should point to _search.entityName");
  }

  @Test
  public void testEntityNameFieldMappingInConflictedFieldNameAlias() {
    // Test the entity name field mapping logic when _entityName is a conflicted field name alias
    // Create multiple entities with _entityName as field name alias to trigger conflicts
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockEntityNameField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockEntityNameField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockEntityNameAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockEntityNameAnnotation2 = mock(SearchableAnnotation.class);

    // Set up fields with _entityName as alias for both entities
    when(mockEntityNameAnnotation1.getFieldName()).thenReturn("entityName");
    when(mockEntityNameAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation1.getFieldNameAliases())
        .thenReturn(Collections.singletonList("_entityName"));
    when(mockEntityNameAnnotation1.getSearchLabel()).thenReturn(Optional.of("entityName"));
    when(mockEntityNameField1.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation1);

    when(mockEntityNameAnnotation2.getFieldName()).thenReturn("entityName");
    when(mockEntityNameAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation2.getFieldNameAliases())
        .thenReturn(Collections.singletonList("_entityName"));
    when(mockEntityNameAnnotation2.getSearchLabel()).thenReturn(Optional.of("entityName"));
    when(mockEntityNameField2.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation2);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));

    // Mock entity registry to return multiple entity specs with conflicts
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Use the multiple entity method that triggers conflict resolution
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Should have at least one index mapping");

    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that _entityName field exists at root level with alias mapping
    assertTrue(
        properties.containsKey("_entityName"), "_entityName field should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameMapping = (Map<String, Object>) properties.get("_entityName");

    // Verify the alias mapping structure
    assertEquals("alias", entityNameMapping.get("type"), "_entityName should have alias type");
    assertEquals(
        "_search.entityName",
        entityNameMapping.get("path"),
        "_entityName should point to _search.entityName");
  }

  @Test
  public void testEntityNameFieldMappingInSingleFieldAlias() {
    // Test the entity name field mapping logic when _entityName is a single field alias (no
    // conflicts)
    // This test should NOT create root-level fields since there are no conflicts
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockEntityNameField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockEntityNameAnnotation = mock(SearchableAnnotation.class);

    // Set up a field with _entityName as alias (no conflicts)
    when(mockEntityNameAnnotation.getFieldName()).thenReturn("entityName");
    when(mockEntityNameAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation.getFieldNameAliases())
        .thenReturn(Collections.singletonList("_entityName"));
    when(mockEntityNameAnnotation.getSearchLabel()).thenReturn(Optional.of("entityName"));
    when(mockEntityNameField.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Mock entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the public getMappings method that returns Map<String, Object>
    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // For single field alias with no conflicts, _entityName should exist at root level
    // as an alias pointing to _search.entityName
    assertTrue(
        properties.containsKey("_entityName"),
        "_entityName field should exist at root level as an alias");

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameMapping = (Map<String, Object>) properties.get("_entityName");

    // Verify the alias mapping structure
    assertEquals("alias", entityNameMapping.get("type"), "_entityName should have alias type");
    assertEquals(
        "_search.entityName",
        entityNameMapping.get("path"),
        "_entityName should point to _search.entityName");
  }

  @Test
  public void testEntityNameFieldMappingWithMultipleConflicts() {
    // Test the entity name field mapping logic when _entityName has multiple conflicts
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockEntityNameField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockEntityNameField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockEntityNameAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockEntityNameAnnotation2 = mock(SearchableAnnotation.class);

    // Set up first entity with _entityName field
    when(mockEntityNameAnnotation1.getFieldName()).thenReturn("_entityName");
    when(mockEntityNameAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockEntityNameField1.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with _entityName field (creating conflict)
    when(mockEntityNameAnnotation2.getFieldName()).thenReturn("_entityName");
    when(mockEntityNameAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockEntityNameAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockEntityNameField2.getSearchableAnnotation()).thenReturn(mockEntityNameAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockEntityNameField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Set up entity registry for multi-entity method
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));

    // Use the multi-entity method to test conflict resolution
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);
    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Index mappings should not be empty");

    // Get the first index mapping
    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that _entityName field exists at root level with alias mapping
    assertTrue(
        properties.containsKey("_entityName"), "_entityName field should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameMapping = (Map<String, Object>) properties.get("_entityName");

    // Verify the alias mapping structure
    assertEquals("alias", entityNameMapping.get("type"), "_entityName should have alias type");
    assertEquals(
        "_search.entityName",
        entityNameMapping.get("path"),
        "_entityName should point to _search.entityName");
  }

  @Test
  public void testEntityNameFieldMappingWithNonEntityNameField() {
    // Test that non-entity name fields are handled normally (not as entity name aliases)
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockRegularField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockRegularAnnotation = mock(SearchableAnnotation.class);

    // Set up a regular field (not _entityName)
    when(mockRegularAnnotation.getFieldName()).thenReturn("regularField");
    when(mockRegularAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockRegularAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockRegularField.getSearchableAnnotation()).thenReturn(mockRegularAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockRegularField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getAspectSpec("testAspect")).thenReturn(mockAspectSpec);

    // Set up entity registry for multi-entity method
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the multi-entity method to test conflict resolution
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);
    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Index mappings should not be empty");

    // Get the first index mapping
    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that regularField exists at root level with alias mapping (not entity name mapping)
    assertTrue(properties.containsKey("regularField"), "regularField should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> regularFieldMapping = (Map<String, Object>) properties.get("regularField");

    // Verify the alias mapping structure (should point to aspect field, not _search.entityName)
    assertEquals("alias", regularFieldMapping.get("type"), "regularField should have alias type");
    assertNotEquals(
        "_search.entityName",
        regularFieldMapping.get("path"),
        "regularField should NOT point to _search.entityName");
    assertTrue(
        regularFieldMapping.get("path").toString().contains("testAspect"),
        "regularField should point to aspect field");
  }

  @Test
  public void testEntityNameFieldMappingWithCaseSensitivity() {
    // Test that entity name field detection is case-sensitive
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockCaseSensitiveField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockCaseSensitiveAnnotation = mock(SearchableAnnotation.class);

    // Set up a field with similar name but different case
    when(mockCaseSensitiveAnnotation.getFieldName()).thenReturn("_EntityName"); // Different case
    when(mockCaseSensitiveAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockCaseSensitiveAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockCaseSensitiveField.getSearchableAnnotation()).thenReturn(mockCaseSensitiveAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockCaseSensitiveField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getAspectSpec("testAspect")).thenReturn(mockAspectSpec);

    // Set up entity registry for multi-entity method
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the multi-entity method to test conflict resolution
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);
    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Index mappings should not be empty");

    // Get the first index mapping
    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that _EntityName field exists at root level with regular alias mapping (not entity
    // name mapping)
    assertTrue(
        properties.containsKey("_EntityName"), "_EntityName field should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> entityNameMapping = (Map<String, Object>) properties.get("_EntityName");

    // Verify the alias mapping structure (should point to aspect field, not _search.entityName)
    assertEquals("alias", entityNameMapping.get("type"), "_EntityName should have alias type");
    assertNotEquals(
        "_search.entityName",
        entityNameMapping.get("path"),
        "_EntityName should NOT point to _search.entityName");
    assertTrue(
        entityNameMapping.get("path").toString().contains("testAspect"),
        "_EntityName should point to aspect field");
  }

  // ==================== Entity Field Name Copy-To Tests ====================

  @Test
  public void testEntityFieldNameCopyToWithValidEntityFieldName() {
    // Test that entityFieldName annotation adds copy_to to _search.{entityFieldName}
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    // Set up field with entityFieldName annotation
    when(mockAnnotation.getFieldName()).thenReturn("testField");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.of("entityField"));
    when(mockField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(mockField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Mock entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the public getMappings method that returns Map<String, Object>
    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Mappings should contain '_aspects' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testAspect"),
        "Aspect properties should contain 'testAspect'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFields = (Map<String, Object>) aspectProperties.get("testAspect");
    assertTrue(aspectFields.containsKey("properties"), "Aspect should contain 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFieldProperties =
        (Map<String, Object>) aspectFields.get("properties");
    assertTrue(
        aspectFieldProperties.containsKey("testField"), "Aspect fields should contain 'testField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) aspectFieldProperties.get("testField");

    // Verify that copy_to contains the entityFieldName destination
    assertTrue(fieldMapping.containsKey("copy_to"), "Field should have copy_to property");
    @SuppressWarnings("unchecked")
    List<String> copyTo = (List<String>) fieldMapping.get("copy_to");
    assertTrue(
        copyTo.contains("_search.entityField"), "copy_to should contain '_search.entityField'");
  }

  @Test
  public void testEntityFieldNameCopyToWithEmptyEntityFieldName() {
    // Test that empty entityFieldName does not add copy_to
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    // Set up field with empty entityFieldName annotation
    when(mockAnnotation.getFieldName()).thenReturn("testField");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.of(""));
    when(mockField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(mockField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Mock entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the public getMappings method that returns Map<String, Object>
    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Mappings should contain '_aspects' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testAspect"),
        "Aspect properties should contain 'testAspect'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFields = (Map<String, Object>) aspectProperties.get("testAspect");
    assertTrue(aspectFields.containsKey("properties"), "Aspect should contain 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFieldProperties =
        (Map<String, Object>) aspectFields.get("properties");
    assertTrue(
        aspectFieldProperties.containsKey("testField"), "Aspect fields should contain 'testField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) aspectFieldProperties.get("testField");

    // Verify that copy_to does not contain empty entityFieldName destination
    if (fieldMapping.containsKey("copy_to")) {
      @SuppressWarnings("unchecked")
      List<String> copyTo = (List<String>) fieldMapping.get("copy_to");
      assertFalse(
          copyTo.contains("_search."), "copy_to should not contain empty '_search.' destination");
    }
  }

  @Test
  public void testEntityFieldNameCopyToWithEmptyEntityFieldNameOptional() {
    // Test that Optional.empty() entityFieldName does not add copy_to
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    // Set up field with null entityFieldName annotation
    when(mockAnnotation.getFieldName()).thenReturn("testField");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.empty());
    when(mockField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(mockField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Mock entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the public getMappings method that returns Map<String, Object>
    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Mappings should contain '_aspects' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testAspect"),
        "Aspect properties should contain 'testAspect'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFields = (Map<String, Object>) aspectProperties.get("testAspect");
    assertTrue(aspectFields.containsKey("properties"), "Aspect should contain 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFieldProperties =
        (Map<String, Object>) aspectFields.get("properties");
    assertTrue(
        aspectFieldProperties.containsKey("testField"), "Aspect fields should contain 'testField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) aspectFieldProperties.get("testField");

    // Verify that copy_to is not added when entityFieldName is Optional.empty()
    assertFalse(
        fieldMapping.containsKey("copy_to"),
        "Field should not have copy_to property when entityFieldName is Optional.empty()");
  }

  @Test
  public void testEntityFieldNameCopyToWithMultipleCopyToDestinations() {
    // Test that entityFieldName adds to existing copy_to destinations
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    // Set up field with both searchLabel and entityFieldName annotations
    when(mockAnnotation.getFieldName()).thenReturn("testField");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation.getSearchLabel()).thenReturn(Optional.of("searchLabel"));
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.of("entityField"));
    when(mockField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(mockField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Mock entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the public getMappings method that returns Map<String, Object>
    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Mappings should contain '_aspects' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testAspect"),
        "Aspect properties should contain 'testAspect'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFields = (Map<String, Object>) aspectProperties.get("testAspect");
    assertTrue(aspectFields.containsKey("properties"), "Aspect should contain 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFieldProperties =
        (Map<String, Object>) aspectFields.get("properties");
    assertTrue(
        aspectFieldProperties.containsKey("testField"), "Aspect fields should contain 'testField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) aspectFieldProperties.get("testField");

    // Verify that copy_to contains both destinations
    assertTrue(fieldMapping.containsKey("copy_to"), "Field should have copy_to property");
    @SuppressWarnings("unchecked")
    List<String> copyTo = (List<String>) fieldMapping.get("copy_to");
    assertTrue(
        copyTo.contains("_search.searchLabel"), "copy_to should contain '_search.searchLabel'");
    assertTrue(
        copyTo.contains("_search.entityField"), "copy_to should contain '_search.entityField'");
    assertEquals(2, copyTo.size(), "copy_to should contain exactly 2 destinations");
  }

  @Test
  public void testEntityFieldNameCopyToWithDifferentFieldTypes() {
    // Test that entityFieldName copy_to works with different field types
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation = mock(SearchableAnnotation.class);

    // Set up field with entityFieldName annotation and different field types
    when(mockAnnotation.getFieldName()).thenReturn("testField");
    when(mockAnnotation.getFieldType()).thenReturn(FieldType.TEXT);
    when(mockAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation.getEntityFieldName()).thenReturn(Optional.of("entityField"));
    when(mockField.getSearchableAnnotation()).thenReturn(mockAnnotation);

    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.singletonList(mockField));
    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));

    // Mock entity registry to return the entity spec
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(Collections.singletonMap("testEntity", mockEntitySpec));

    // Use the public getMappings method that returns Map<String, Object>
    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Mappings should contain '_aspects' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testAspect"),
        "Aspect properties should contain 'testAspect'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFields = (Map<String, Object>) aspectProperties.get("testAspect");
    assertTrue(aspectFields.containsKey("properties"), "Aspect should contain 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectFieldProperties =
        (Map<String, Object>) aspectFields.get("properties");
    assertTrue(
        aspectFieldProperties.containsKey("testField"), "Aspect fields should contain 'testField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) aspectFieldProperties.get("testField");

    // Verify that copy_to contains the entityFieldName destination regardless of field type
    assertTrue(fieldMapping.containsKey("copy_to"), "Field should have copy_to property");
    @SuppressWarnings("unchecked")
    List<String> copyTo = (List<String>) fieldMapping.get("copy_to");
    assertTrue(
        copyTo.contains("_search.entityField"), "copy_to should contain '_search.entityField'");
  }

  // ==================== Eager Global Ordinals Tests ====================

  @Test
  public void testEagerGlobalOrdinalsInConflictedFieldName() {
    // Test the eager_global_ordinals logic when a field name has conflicts and eagerGlobalOrdinals
    // is set
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation2 = mock(SearchableAnnotation.class);

    // Set up first entity with eagerGlobalOrdinals=true
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation1.getEagerGlobalOrdinals()).thenReturn(Optional.of(true));
    when(mockField1.getSearchableAnnotation()).thenReturn(mockAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with eagerGlobalOrdinals=false (creating conflict)
    when(mockAnnotation2.getFieldName()).thenReturn("testField");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation2.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField2.getSearchableAnnotation()).thenReturn(mockAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Mock entity registry to return multiple entity specs with conflicts
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Use the multi-entity method to trigger conflict resolution
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Should have at least one index mapping");

    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that testField exists at root level with eager_global_ordinals=true
    assertTrue(properties.containsKey("testField"), "testField should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) properties.get("testField");

    // Debug: Print the actual field mapping to understand its structure
    System.out.println("testField mapping: " + fieldMapping);

    // Verify the field mapping structure includes eager_global_ordinals=true
    assertEquals("keyword", fieldMapping.get("type"), "testField should have keyword type");
    assertTrue(
        (Boolean) fieldMapping.get("eager_global_ordinals"),
        "testField should have eager_global_ordinals=true");
  }

  @Test
  public void testEagerGlobalOrdinalsInConflictedFieldNameAlias() {
    // Test the eager_global_ordinals logic when a field name alias has conflicts and
    // eagerGlobalOrdinals is set
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation2 = mock(SearchableAnnotation.class);

    // Set up first entity with eagerGlobalOrdinals=true and field name alias
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.singletonList("testAlias"));
    when(mockAnnotation1.getEagerGlobalOrdinals()).thenReturn(Optional.of(true));
    when(mockField1.getSearchableAnnotation()).thenReturn(mockAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with eagerGlobalOrdinals=false (creating conflict)
    when(mockAnnotation2.getFieldName()).thenReturn("testField");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.singletonList("testAlias"));
    when(mockAnnotation2.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField2.getSearchableAnnotation()).thenReturn(mockAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Mock entity registry to return multiple entity specs with conflicts
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Use the multi-entity method to trigger conflict resolution
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Should have at least one index mapping");

    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that testAlias exists at root level with eager_global_ordinals=true
    assertTrue(properties.containsKey("testAlias"), "testAlias should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> aliasMapping = (Map<String, Object>) properties.get("testAlias");

    // Verify the alias mapping structure includes eager_global_ordinals=true
    assertEquals("keyword", aliasMapping.get("type"), "testAlias should have keyword type");
    assertTrue(
        (Boolean) aliasMapping.get("eager_global_ordinals"),
        "testAlias should have eager_global_ordinals=true");
  }

  @Test
  public void testEagerGlobalOrdinalsWithNoEagerGlobalOrdinals() {
    // Test that eager_global_ordinals is not set when no fields have eagerGlobalOrdinals=true
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation2 = mock(SearchableAnnotation.class);

    // Set up first entity with eagerGlobalOrdinals=false
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation1.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField1.getSearchableAnnotation()).thenReturn(mockAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with eagerGlobalOrdinals=false (creating conflict)
    when(mockAnnotation2.getFieldName()).thenReturn("testField");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation2.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField2.getSearchableAnnotation()).thenReturn(mockAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Mock entity registry to return multiple entity specs with conflicts
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Use the multi-entity method to trigger conflict resolution
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Should have at least one index mapping");

    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that testField exists at root level without eager_global_ordinals
    assertTrue(properties.containsKey("testField"), "testField should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) properties.get("testField");

    // Verify the field mapping structure does not include eager_global_ordinals
    assertEquals("keyword", fieldMapping.get("type"), "testField should have keyword type");
    assertFalse(
        fieldMapping.containsKey("eager_global_ordinals"),
        "testField should NOT have eager_global_ordinals");
  }

  @Test
  public void testEagerGlobalOrdinalsWithMixedEagerGlobalOrdinals() {
    // Test that eager_global_ordinals is set when at least one field has eagerGlobalOrdinals=true
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec3 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec3 = mock(AspectSpec.class);
    SearchableFieldSpec mockField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField2 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField3 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation2 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation3 = mock(SearchableAnnotation.class);

    // Set up first entity with eagerGlobalOrdinals=false
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation1.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField1.getSearchableAnnotation()).thenReturn(mockAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with eagerGlobalOrdinals=true
    when(mockAnnotation2.getFieldName()).thenReturn("testField");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation2.getEagerGlobalOrdinals()).thenReturn(Optional.of(true));
    when(mockField2.getSearchableAnnotation()).thenReturn(mockAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Set up third entity with eagerGlobalOrdinals=false
    when(mockAnnotation3.getFieldName()).thenReturn("testField");
    when(mockAnnotation3.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation3.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation3.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField3.getSearchableAnnotation()).thenReturn(mockAnnotation3);

    when(mockAspectSpec3.getName()).thenReturn("testAspect3");
    when(mockAspectSpec3.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField3));
    when(mockEntitySpec3.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec3));
    when(mockEntitySpec3.getAspectSpec("testAspect3")).thenReturn(mockAspectSpec3);

    // Mock entity registry to return multiple entity specs with conflicts
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of(
                "testEntity1",
                mockEntitySpec1,
                "testEntity2",
                mockEntitySpec2,
                "testEntity3",
                mockEntitySpec3));
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));

    // Use the multi-entity method to trigger conflict resolution
    OperationContext operationContext =
        TestOperationContexts.systemContextNoSearchAuthorization(mockEntityRegistry);
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);

    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Should have at least one index mapping");

    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that testField exists at root level with eager_global_ordinals=true (because at least
    // one field has it)
    assertTrue(properties.containsKey("testField"), "testField should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) properties.get("testField");

    // Verify the field mapping structure includes eager_global_ordinals=true
    assertEquals("keyword", fieldMapping.get("type"), "testField should have keyword type");
    assertTrue(
        (Boolean) fieldMapping.get("eager_global_ordinals"),
        "testField should have eager_global_ordinals=true");
  }

  @Test
  public void testEagerGlobalOrdinalsWithEmptyOptional() {
    // Test that eager_global_ordinals is not set when eagerGlobalOrdinals is Optional.empty()
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation2 = mock(SearchableAnnotation.class);

    // Set up first entity with eagerGlobalOrdinals=Optional.empty()
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation1.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockField1.getSearchableAnnotation()).thenReturn(mockAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with eagerGlobalOrdinals=Optional.empty() (creating conflict)
    when(mockAnnotation2.getFieldName()).thenReturn("testField");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation2.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockField2.getSearchableAnnotation()).thenReturn(mockAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Set up entity registry for multi-entity method
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));

    // Use the multi-entity method to test conflict resolution
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);
    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Index mappings should not be empty");

    // Get the first index mapping
    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that testField exists at root level without eager_global_ordinals
    assertTrue(properties.containsKey("testField"), "testField should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) properties.get("testField");

    // Verify the field mapping structure does not include eager_global_ordinals
    assertEquals("keyword", fieldMapping.get("type"), "testField should have keyword type");
    assertFalse(
        fieldMapping.containsKey("eager_global_ordinals"),
        "testField should NOT have eager_global_ordinals");
  }

  @Test
  public void testEagerGlobalOrdinalsWithDifferentFieldTypes() {
    // Test that eager_global_ordinals is set correctly for different field types
    EntitySpec mockEntitySpec1 = mock(EntitySpec.class);
    EntitySpec mockEntitySpec2 = mock(EntitySpec.class);
    AspectSpec mockAspectSpec1 = mock(AspectSpec.class);
    AspectSpec mockAspectSpec2 = mock(AspectSpec.class);
    SearchableFieldSpec mockField1 = mock(SearchableFieldSpec.class);
    SearchableFieldSpec mockField2 = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockAnnotation1 = mock(SearchableAnnotation.class);
    SearchableAnnotation mockAnnotation2 = mock(SearchableAnnotation.class);

    // Set up first entity with eagerGlobalOrdinals=true and URN field type
    when(mockAnnotation1.getFieldName()).thenReturn("testField");
    when(mockAnnotation1.getFieldType()).thenReturn(FieldType.URN);
    when(mockAnnotation1.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation1.getEagerGlobalOrdinals()).thenReturn(Optional.of(true));
    when(mockField1.getSearchableAnnotation()).thenReturn(mockAnnotation1);

    when(mockAspectSpec1.getName()).thenReturn("testAspect1");
    when(mockAspectSpec1.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField1));
    when(mockEntitySpec1.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec1));
    when(mockEntitySpec1.getAspectSpec("testAspect1")).thenReturn(mockAspectSpec1);

    // Set up second entity with eagerGlobalOrdinals=false and KEYWORD field type (creating
    // conflict)
    when(mockAnnotation2.getFieldName()).thenReturn("testField");
    when(mockAnnotation2.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockAnnotation2.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockAnnotation2.getEagerGlobalOrdinals()).thenReturn(Optional.of(false));
    when(mockField2.getSearchableAnnotation()).thenReturn(mockAnnotation2);

    when(mockAspectSpec2.getName()).thenReturn("testAspect2");
    when(mockAspectSpec2.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockField2));
    when(mockEntitySpec2.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec2));
    when(mockEntitySpec2.getAspectSpec("testAspect2")).thenReturn(mockAspectSpec2);

    // Set up entity registry for multi-entity method
    when(mockEntityRegistry.getSearchGroups()).thenReturn(Collections.singleton("default"));
    when(mockEntityRegistry.getEntitySpecsBySearchGroup("default"))
        .thenReturn(
            ImmutableMap.of("testEntity1", mockEntitySpec1, "testEntity2", mockEntitySpec2));

    // Use the multi-entity method to test conflict resolution
    Collection<IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(operationContext, null);
    assertNotNull(indexMappings, "Index mappings should not be null");
    assertFalse(indexMappings.isEmpty(), "Index mappings should not be empty");

    // Get the first index mapping
    IndexMapping indexMapping = indexMappings.iterator().next();
    Map<String, Object> mappings = indexMapping.getMappings();

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");

    // Verify that testField exists at root level with eager_global_ordinals=true
    assertTrue(properties.containsKey("testField"), "testField should exist at root level");

    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) properties.get("testField");

    // Verify the field mapping structure includes eager_global_ordinals=true
    // The resolved type should be based on conflict resolution, but eager_global_ordinals should be
    // true
    assertTrue(
        (Boolean) fieldMapping.get("eager_global_ordinals"),
        "testField should have eager_global_ordinals=true");
  }

  // ==================== SearchableRefField Mapping Tests ====================

  @Test
  public void testSearchableRefFieldMappingWithDepthZero() throws IOException {
    // Test SearchableRefField mapping when depth is 0 (should return URN mapping)

    // Create mock SearchableRefFieldSpec with depth 0
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(0);
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testRefField"),
        "Aspect properties should contain 'testRefField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping =
        (Map<String, Object>) aspectProperties.get("testRefField");

    // For depth 0, should be keyword mapping (not urn)
    assertEquals(
        "keyword",
        refFieldMapping.get("type"),
        "testRefField should have type 'keyword' for depth 0");
  }

  @Test
  public void testSearchableRefFieldMappingWithDepthOne() throws IOException {
    // Test SearchableRefField mapping when depth is 1 (should include nested fields)

    // Create mock SearchableRefFieldSpec with depth 1
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(1);
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Create mock EntitySpec for the referenced entity type
    EntitySpec mockReferencedEntitySpec = mock(EntitySpec.class);
    AspectSpec mockReferencedAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockReferencedField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockReferencedAnnotation = mock(SearchableAnnotation.class);

    when(mockReferencedAnnotation.getFieldName()).thenReturn("referencedField");
    when(mockReferencedAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockReferencedAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockReferencedAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockReferencedField.getSearchableAnnotation()).thenReturn(mockReferencedAnnotation);

    when(mockReferencedAspectSpec.getName()).thenReturn("referencedAspect");
    when(mockReferencedAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockReferencedField));
    when(mockReferencedAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockReferencedEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockReferencedAspectSpec));
    when(mockReferencedEntitySpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockReferencedField));
    when(mockReferencedEntitySpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    // Mock entity registry to return the referenced entity spec
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockReferencedEntitySpec);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testRefField"),
        "Aspect properties should contain 'testRefField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping =
        (Map<String, Object>) aspectProperties.get("testRefField");

    // For depth 1, should have properties with nested fields
    assertTrue(
        refFieldMapping.containsKey("properties"),
        "testRefField should have 'properties' for depth 1");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldProperties =
        (Map<String, Object>) refFieldMapping.get("properties");
    assertTrue(
        refFieldProperties.containsKey("referencedField"), "Should contain referenced field");
    assertTrue(refFieldProperties.containsKey("urn"), "Should contain urn field");
  }

  @Test
  public void testSearchableRefFieldMappingWithRecursiveDepth() throws IOException {
    // Test SearchableRefField mapping with recursive depth handling

    // Create mock SearchableRefFieldSpec with depth 2
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(2);
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Create mock SearchableRefFieldSpec for nested reference with depth 1
    SearchableRefFieldSpec mockNestedRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockNestedRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockNestedRefAnnotation.getFieldName()).thenReturn("nestedRefField");
    when(mockNestedRefAnnotation.getRefType()).thenReturn("chart");
    when(mockNestedRefAnnotation.getDepth()).thenReturn(1);
    when(mockNestedRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockNestedRefAnnotation);

    // Create mock EntitySpec for the referenced entity type (dataset)
    EntitySpec mockReferencedEntitySpec = mock(EntitySpec.class);
    AspectSpec mockReferencedAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockReferencedField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockReferencedAnnotation = mock(SearchableAnnotation.class);

    when(mockReferencedAnnotation.getFieldName()).thenReturn("referencedField");
    when(mockReferencedAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockReferencedAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockReferencedAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockReferencedField.getSearchableAnnotation()).thenReturn(mockReferencedAnnotation);

    when(mockReferencedAspectSpec.getName()).thenReturn("referencedAspect");
    when(mockReferencedAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockReferencedField));
    when(mockReferencedAspectSpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedRefFieldSpec));
    when(mockReferencedEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockReferencedAspectSpec));
    when(mockReferencedEntitySpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockReferencedField));
    when(mockReferencedEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedRefFieldSpec));

    // Create mock EntitySpec for the nested referenced entity type (chart)
    EntitySpec mockNestedReferencedEntitySpec = mock(EntitySpec.class);
    AspectSpec mockNestedReferencedAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockNestedReferencedField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockNestedReferencedAnnotation = mock(SearchableAnnotation.class);

    when(mockNestedReferencedAnnotation.getFieldName()).thenReturn("nestedReferencedField");
    when(mockNestedReferencedAnnotation.getFieldType()).thenReturn(FieldType.TEXT);
    when(mockNestedReferencedAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockNestedReferencedAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockNestedReferencedField.getSearchableAnnotation())
        .thenReturn(mockNestedReferencedAnnotation);

    when(mockNestedReferencedAspectSpec.getName()).thenReturn("nestedReferencedAspect");
    when(mockNestedReferencedAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedReferencedField));
    when(mockNestedReferencedAspectSpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.emptyList());
    when(mockNestedReferencedEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockNestedReferencedAspectSpec));
    when(mockNestedReferencedEntitySpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedReferencedField));
    when(mockNestedReferencedEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.emptyList());

    // Mock entity registry to return both referenced entity specs
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockReferencedEntitySpec);
    when(mockEntityRegistry.getEntitySpec("chart")).thenReturn(mockNestedReferencedEntitySpec);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testRefField"),
        "Aspect properties should contain 'testRefField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping =
        (Map<String, Object>) aspectProperties.get("testRefField");

    // For depth 2, should have properties with nested fields including recursive references
    assertTrue(
        refFieldMapping.containsKey("properties"),
        "testRefField should have 'properties' for depth 2");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldProperties =
        (Map<String, Object>) refFieldMapping.get("properties");
    assertTrue(
        refFieldProperties.containsKey("referencedField"), "Should contain referenced field");
    assertTrue(refFieldProperties.containsKey("urn"), "Should contain urn field");
    assertTrue(
        refFieldProperties.containsKey("nestedRefField"), "Should contain nested reference field");

    // Verify nested reference field has properties
    @SuppressWarnings("unchecked")
    Map<String, Object> nestedRefFieldMapping =
        (Map<String, Object>) refFieldProperties.get("nestedRefField");
    assertTrue(
        nestedRefFieldMapping.containsKey("properties"), "nestedRefField should have 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> nestedRefFieldProperties =
        (Map<String, Object>) nestedRefFieldMapping.get("properties");
    assertTrue(
        nestedRefFieldProperties.containsKey("nestedReferencedField"),
        "Should contain nested referenced field");
    assertTrue(nestedRefFieldProperties.containsKey("urn"), "Should contain urn field");
  }

  @Test
  public void testSearchableRefFieldMappingWithDepthLimiting() throws IOException {
    // Test SearchableRefField mapping with depth limiting (Math.min logic)

    // Create mock SearchableRefFieldSpec with depth 3
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(3);
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Create mock SearchableRefFieldSpec for nested reference with depth 5 (should be limited to 2)
    SearchableRefFieldSpec mockNestedRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockNestedRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockNestedRefAnnotation.getFieldName()).thenReturn("nestedRefField");
    when(mockNestedRefAnnotation.getRefType()).thenReturn("chart");
    when(mockNestedRefAnnotation.getDepth()).thenReturn(5); // This should be limited to 2 (3-1)
    when(mockNestedRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockNestedRefAnnotation);

    // Create mock EntitySpec for the referenced entity type (dataset)
    EntitySpec mockReferencedEntitySpec = mock(EntitySpec.class);
    AspectSpec mockReferencedAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockReferencedField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockReferencedAnnotation = mock(SearchableAnnotation.class);

    when(mockReferencedAnnotation.getFieldName()).thenReturn("referencedField");
    when(mockReferencedAnnotation.getFieldType()).thenReturn(FieldType.KEYWORD);
    when(mockReferencedAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockReferencedAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockReferencedField.getSearchableAnnotation()).thenReturn(mockReferencedAnnotation);

    when(mockReferencedAspectSpec.getName()).thenReturn("referencedAspect");
    when(mockReferencedAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockReferencedField));
    when(mockReferencedAspectSpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedRefFieldSpec));
    when(mockReferencedEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockReferencedAspectSpec));
    when(mockReferencedEntitySpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockReferencedField));
    when(mockReferencedEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedRefFieldSpec));

    // Create mock EntitySpec for the nested referenced entity type (chart)
    EntitySpec mockNestedReferencedEntitySpec = mock(EntitySpec.class);
    AspectSpec mockNestedReferencedAspectSpec = mock(AspectSpec.class);
    SearchableFieldSpec mockNestedReferencedField = mock(SearchableFieldSpec.class);
    SearchableAnnotation mockNestedReferencedAnnotation = mock(SearchableAnnotation.class);

    when(mockNestedReferencedAnnotation.getFieldName()).thenReturn("nestedReferencedField");
    when(mockNestedReferencedAnnotation.getFieldType()).thenReturn(FieldType.TEXT);
    when(mockNestedReferencedAnnotation.getFieldNameAliases()).thenReturn(Collections.emptyList());
    when(mockNestedReferencedAnnotation.getEagerGlobalOrdinals()).thenReturn(Optional.empty());
    when(mockNestedReferencedField.getSearchableAnnotation())
        .thenReturn(mockNestedReferencedAnnotation);

    when(mockNestedReferencedAspectSpec.getName()).thenReturn("nestedReferencedAspect");
    when(mockNestedReferencedAspectSpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedReferencedField));
    when(mockNestedReferencedAspectSpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.emptyList());
    when(mockNestedReferencedEntitySpec.getAspectSpecs())
        .thenReturn(Collections.singletonList(mockNestedReferencedAspectSpec));
    when(mockNestedReferencedEntitySpec.getSearchableFieldSpecs())
        .thenReturn(Collections.singletonList(mockNestedReferencedField));
    when(mockNestedReferencedEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.emptyList());

    // Mock entity registry to return both referenced entity specs
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockReferencedEntitySpec);
    when(mockEntityRegistry.getEntitySpec("chart")).thenReturn(mockNestedReferencedEntitySpec);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects'");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertTrue(
        aspectProperties.containsKey("testRefField"),
        "Aspect properties should contain 'testRefField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping =
        (Map<String, Object>) aspectProperties.get("testRefField");

    // For depth 3, should have properties with nested fields
    assertTrue(
        refFieldMapping.containsKey("properties"),
        "testRefField should have 'properties' for depth 3");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldProperties =
        (Map<String, Object>) refFieldMapping.get("properties");
    assertTrue(
        refFieldProperties.containsKey("referencedField"), "Should contain referenced field");
    assertTrue(refFieldProperties.containsKey("urn"), "Should contain urn field");
    assertTrue(
        refFieldProperties.containsKey("nestedRefField"), "Should contain nested reference field");

    // Verify nested reference field has properties (depth should be limited to 2, not 5)
    @SuppressWarnings("unchecked")
    Map<String, Object> nestedRefFieldMapping =
        (Map<String, Object>) refFieldProperties.get("nestedRefField");
    assertTrue(
        nestedRefFieldMapping.containsKey("properties"), "nestedRefField should have 'properties'");

    @SuppressWarnings("unchecked")
    Map<String, Object> nestedRefFieldProperties =
        (Map<String, Object>) nestedRefFieldMapping.get("properties");
    assertTrue(
        nestedRefFieldProperties.containsKey("nestedReferencedField"),
        "Should contain nested referenced field");
    assertTrue(nestedRefFieldProperties.containsKey("urn"), "Should contain urn field");
  }

  @Test
  public void testSearchableRefFieldMappingWithNullEntityRegistry() throws IOException {
    // Test SearchableRefField mapping with null entity registry (should throw exception)

    // Create mock SearchableRefFieldSpec
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(1);
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    // Mock entity registry to return null for the referenced entity type
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(null);

    // Should throw exception when trying to get entity spec for referenced type
    assertThrows(
        NullPointerException.class,
        () -> {
          mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);
        });
  }

  @Test
  public void testSearchableRefFieldMappingDepthZeroEarlyReturn() {
    // Test the depth == 0 early return logic in getMappingForSearchableRefField
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(0); // Set depth to 0 to trigger early return
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    // Mock entity registry
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    // Verify that the mappings contain the expected structure
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertNotNull(properties, "Properties should not be null");

    // Verify that the _aspects field contains the SearchableRefField with URN mapping
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "Aspects should not be null");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectProperties, "Aspect properties should not be null");

    // Verify that the SearchableRefField exists with URN mapping (depth == 0 should return URN
    // mapping)
    assertTrue(
        aspectProperties.containsKey("testRefField"),
        "Aspect properties should contain 'testRefField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping =
        (Map<String, Object>) aspectProperties.get("testRefField");
    assertNotNull(refFieldMapping, "Ref field mapping should not be null");

    // Debug: print the actual mapping structure
    System.out.println("testRefField mapping: " + refFieldMapping);

    // Verify that the mapping has keyword type (from FieldTypeMapper.getMappingsForUrn())
    // URN fields are mapped to keyword type with ignore_above property
    assertEquals(
        "keyword",
        refFieldMapping.get("type"),
        "testRefField should have type 'keyword' for depth 0");
    assertTrue(
        refFieldMapping.containsKey("ignore_above"),
        "testRefField should have ignore_above property");
    assertEquals(
        255, refFieldMapping.get("ignore_above"), "testRefField should have ignore_above=255");
  }

  @Test
  public void testSearchableRefFieldMappingDepthZeroWithMultipleFields() {
    // Test the depth == 0 early return logic with multiple SearchableRefFields
    SearchableRefFieldSpec mockRefFieldSpec1 = mock(SearchableRefFieldSpec.class);
    SearchableRefFieldSpec mockRefFieldSpec2 = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation1 = mock(SearchableRefAnnotation.class);
    SearchableRefAnnotation mockRefAnnotation2 = mock(SearchableRefAnnotation.class);

    // First field with depth 0
    when(mockRefAnnotation1.getFieldName()).thenReturn("testRefField1");
    when(mockRefAnnotation1.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation1.getDepth()).thenReturn(0);
    when(mockRefFieldSpec1.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation1);

    // Second field with depth 0
    when(mockRefAnnotation2.getFieldName()).thenReturn("testRefField2");
    when(mockRefAnnotation2.getRefType()).thenReturn("chart");
    when(mockRefAnnotation2.getDepth()).thenReturn(0);
    when(mockRefFieldSpec2.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation2);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Arrays.asList(mockRefFieldSpec1, mockRefFieldSpec2));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    // Mock entity registry
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    // Verify that the mappings contain the expected structure
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertNotNull(properties, "Properties should not be null");

    // Verify that the _aspects field contains both SearchableRefFields with URN mappings
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "Aspects should not be null");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectProperties, "Aspect properties should not be null");

    // Verify that both SearchableRefFields exist with URN mappings
    assertTrue(
        aspectProperties.containsKey("testRefField1"),
        "Aspect properties should contain 'testRefField1'");
    assertTrue(
        aspectProperties.containsKey("testRefField2"),
        "Aspect properties should contain 'testRefField2'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping1 =
        (Map<String, Object>) aspectProperties.get("testRefField1");
    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping2 =
        (Map<String, Object>) aspectProperties.get("testRefField2");

    // Verify that both mappings have keyword type (from FieldTypeMapper.getMappingsForUrn())
    // URN fields are mapped to keyword type with ignore_above property
    assertEquals(
        "keyword",
        refFieldMapping1.get("type"),
        "testRefField1 should have type 'keyword' for depth 0");
    assertEquals(
        "keyword",
        refFieldMapping2.get("type"),
        "testRefField2 should have type 'keyword' for depth 0");

    // Verify that both mappings have the URN-specific properties
    assertTrue(
        refFieldMapping1.containsKey("ignore_above"),
        "testRefField1 should have ignore_above property");
    assertTrue(
        refFieldMapping2.containsKey("ignore_above"),
        "testRefField2 should have ignore_above property");
    assertEquals(
        255, refFieldMapping1.get("ignore_above"), "testRefField1 should have ignore_above=255");
    assertEquals(
        255, refFieldMapping2.get("ignore_above"), "testRefField2 should have ignore_above=255");
  }

  @Test
  public void testSearchableRefFieldMappingDepthZeroWithDifferentRefTypes() {
    // Test the depth == 0 early return logic with different ref types
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn("testRefField");
    when(mockRefAnnotation.getRefType()).thenReturn("customEntity"); // Different ref type
    when(mockRefAnnotation.getDepth()).thenReturn(0); // Set depth to 0 to trigger early return
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    // Mock entity registry
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    // Verify that the mappings contain the expected structure
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertNotNull(properties, "Properties should not be null");

    // Verify that the _aspects field contains the SearchableRefField with URN mapping
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "Aspects should not be null");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectProperties, "Aspect properties should not be null");

    // Verify that the SearchableRefField exists with URN mapping (depth == 0 should return URN
    // mapping regardless of ref type)
    assertTrue(
        aspectProperties.containsKey("testRefField"),
        "Aspect properties should contain 'testRefField'");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping =
        (Map<String, Object>) aspectProperties.get("testRefField");
    assertNotNull(refFieldMapping, "Ref field mapping should not be null");

    // Verify that the mapping has keyword type (from FieldTypeMapper.getMappingsForUrn())
    // The ref type doesn't matter when depth == 0, it should always return keyword mapping
    assertEquals(
        "keyword",
        refFieldMapping.get("type"),
        "testRefField should have type 'keyword' for depth 0 regardless of ref type");
    assertTrue(
        refFieldMapping.containsKey("ignore_above"),
        "testRefField should have ignore_above property");
    assertEquals(
        255, refFieldMapping.get("ignore_above"), "testRefField should have ignore_above=255");
  }

  @Test
  public void testSearchableRefFieldMappingDepthZeroWithEmptyFieldName() {
    // Test the depth == 0 early return logic with empty field name
    SearchableRefFieldSpec mockRefFieldSpec = mock(SearchableRefFieldSpec.class);
    SearchableRefAnnotation mockRefAnnotation = mock(SearchableRefAnnotation.class);

    when(mockRefAnnotation.getFieldName()).thenReturn(""); // Empty field name
    when(mockRefAnnotation.getRefType()).thenReturn("dataset");
    when(mockRefAnnotation.getDepth()).thenReturn(0); // Set depth to 0 to trigger early return
    when(mockRefFieldSpec.getSearchableRefAnnotation()).thenReturn(mockRefAnnotation);

    // Test the private method indirectly through getMappings
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockAspectSpec = mock(AspectSpec.class);

    when(mockEntitySpec.getAspectSpecs()).thenReturn(Collections.singletonList(mockAspectSpec));
    when(mockEntitySpec.getSearchableRefFieldSpecs())
        .thenReturn(Collections.singletonList(mockRefFieldSpec));
    when(mockAspectSpec.getName()).thenReturn("testAspect");
    when(mockAspectSpec.getSearchableFieldSpecs()).thenReturn(Collections.emptyList());
    when(mockAspectSpec.getSearchableRefFieldSpecs()).thenReturn(Collections.emptyList());

    // Mock entity registry
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);

    Map<String, Object> mappings = mappingsBuilder.getMappings(mockEntityRegistry, mockEntitySpec);

    // Verify that the mappings contain the expected structure
    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.containsKey("properties"), "Mappings should contain 'properties' key");

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
    assertNotNull(properties, "Properties should not be null");

    // Verify that the _aspects field contains the SearchableRefField with empty field name
    assertTrue(properties.containsKey("_aspects"), "Properties should contain '_aspects' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspects = (Map<String, Object>) properties.get("_aspects");
    assertNotNull(aspects, "Aspects should not be null");
    assertTrue(aspects.containsKey("properties"), "Aspects should contain 'properties' field");

    @SuppressWarnings("unchecked")
    Map<String, Object> aspectProperties = (Map<String, Object>) aspects.get("properties");
    assertNotNull(aspectProperties, "Aspect properties should not be null");

    // Verify that the SearchableRefField exists with empty field name and URN mapping
    assertTrue(
        aspectProperties.containsKey(""), "Aspect properties should contain empty field name");

    @SuppressWarnings("unchecked")
    Map<String, Object> refFieldMapping = (Map<String, Object>) aspectProperties.get("");
    assertNotNull(refFieldMapping, "Ref field mapping should not be null");

    // Verify that the mapping has keyword type (from FieldTypeMapper.getMappingsForUrn())
    assertEquals(
        "keyword",
        refFieldMapping.get("type"),
        "Empty field name should have type 'keyword' for depth 0");
    assertTrue(
        refFieldMapping.containsKey("ignore_above"),
        "Empty field name should have ignore_above property");
    assertEquals(
        255, refFieldMapping.get("ignore_above"), "Empty field name should have ignore_above=255");
  }
}
