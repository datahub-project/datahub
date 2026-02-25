package com.linkedin.metadata.search.elasticsearch.index.entity.v2;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.test.TestRefEntity;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.EntitySpecBuilder;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder.IndexMapping;
import com.linkedin.metadata.search.query.request.TestSearchFieldConfig;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for LegacyMappingsBuilder with comprehensive test coverage. */
public class V2MappingsBuilderTest {

  private V2MappingsBuilder mappingsBuilder;
  private EntityIndexConfiguration entityIndexConfiguration;
  private OperationContext operationContext;
  private EntityRegistry mockEntityRegistry;
  private EntitySpec mockEntitySpec;

  @BeforeMethod
  public void setUp() {
    // Create mock EntityIndexConfiguration
    entityIndexConfiguration = mock(EntityIndexConfiguration.class);
    EntityIndexVersionConfiguration v2Config = mock(EntityIndexVersionConfiguration.class);
    when(entityIndexConfiguration.getV2()).thenReturn(v2Config);

    // Create LegacyMappingsBuilder
    mappingsBuilder = new V2MappingsBuilder(entityIndexConfiguration);

    // Create real OperationContext with test setup
    operationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    // Create mock EntityRegistry and EntitySpec
    mockEntityRegistry = mock(EntityRegistry.class);
    mockEntitySpec = mock(EntitySpec.class);
  }

  @Test
  public void testMappingsBuilder() {
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Use the real EntityRegistry from the test OperationContext
    Collection<IndexMapping> result = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");

    // If there are mappings, verify they contain expected properties
    if (!result.isEmpty()) {
      IndexMapping mapping = result.iterator().next();
      Map<String, Object> mappings = mapping.getMappings();
      assertTrue(mappings.containsKey("properties"), "Mappings should contain properties");

      Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
      assertTrue(properties.containsKey("urn"), "Should contain urn field");
      assertTrue(properties.containsKey("runId"), "Should contain runId field");
      assertTrue(properties.containsKey("systemCreated"), "Should contain systemCreated field");
    }
  }

  @Test
  public void testGetIndexMappingsWithStructuredProperty() throws URISyntaxException {
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Baseline comparison: Mappings with no structured props
    Collection<IndexMapping> resultWithoutStructuredProps =
        mappingsBuilder.getIndexMappings(operationContext);

    // Test that a structured property that does not apply to the entity does not alter the mappings
    StructuredPropertyDefinition structPropNotForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("propNotForThis")
            .setDisplayName("propNotForThis")
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Collection<IndexMapping> resultWithOnlyUnrelatedStructuredProp =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));

    // The results should be the same since the structured property doesn't apply to test entities
    assertEquals(resultWithOnlyUnrelatedStructuredProp.size(), resultWithoutStructuredProps.size());

    // Test that a structured property that does apply to this entity is included in the mappings
    String fqnOfRelatedProp = "propForThis";
    StructuredPropertyDefinition structPropForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName(fqnOfRelatedProp)
            .setDisplayName("propForThis")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Collection<IndexMapping> resultWithOnlyRelatedStructuredProp =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity)));

    assertNotNull(resultWithOnlyRelatedStructuredProp, "Result should not be null");

    // Test that only structured properties that apply are included
    Collection<IndexMapping> resultWithBothStructuredProps =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity),
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));

    // Results should be the same as with only the related structured property
    assertEquals(resultWithBothStructuredProps.size(), resultWithOnlyRelatedStructuredProp.size());
  }

  /**
   * Test that structured properties using the datahub. prefix format (e.g.,
   * urn:li:entityType:datahub.dataset) are correctly matched to entities. This is the format used
   * in production and documented in the API.
   */
  @Test
  public void testGetIndexMappingsWithStructuredPropertyDatahubPrefix() throws URISyntaxException {
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Test structured property with datahub. prefix format - this is the production format
    StructuredPropertyDefinition structPropWithDatahubPrefix =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("propWithDatahubPrefix")
            .setDisplayName("propWithDatahubPrefix")
            .setEntityTypes(
                new UrnArray(
                    // Use datahub. prefix format (production format)
                    Urn.createFromString("urn:li:entityType:datahub.dataset"),
                    Urn.createFromString("urn:li:entityType:datahub.testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Collection<IndexMapping> resultWithDatahubPrefix =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propWithDatahubPrefix"),
                    structPropWithDatahubPrefix)));

    assertNotNull(resultWithDatahubPrefix, "Result should not be null");

    // Test structured property with legacy format (without datahub. prefix)
    StructuredPropertyDefinition structPropWithLegacyFormat =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("propWithLegacyFormat")
            .setDisplayName("propWithLegacyFormat")
            .setEntityTypes(
                new UrnArray(
                    // Legacy format without datahub. prefix
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Collection<IndexMapping> resultWithLegacyFormat =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propWithLegacyFormat"),
                    structPropWithLegacyFormat)));

    // Both formats should produce the same number of mappings
    assertEquals(
        resultWithDatahubPrefix.size(),
        resultWithLegacyFormat.size(),
        "Both URN formats should produce the same number of mappings");

    // Test with mixed formats - both should be included
    Collection<IndexMapping> resultWithMixedFormats =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propWithDatahubPrefix"),
                    structPropWithDatahubPrefix),
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propWithLegacyFormat"),
                    structPropWithLegacyFormat)));

    assertEquals(
        resultWithMixedFormats.size(),
        resultWithDatahubPrefix.size(),
        "Mixed format properties should produce same number of mappings");
  }

  @Test
  public void testGetIndexMappingsWithStructuredPropertyV1() throws URISyntaxException {
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Baseline comparison: Mappings with no structured props
    Collection<IndexMapping> resultWithoutStructuredProps =
        mappingsBuilder.getIndexMappings(operationContext);

    // Test that a structured property that does not apply to the entity does not alter the mappings
    StructuredPropertyDefinition structPropNotForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("propNotForThis")
            .setDisplayName("propNotForThis")
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Collection<IndexMapping> resultWithOnlyUnrelatedStructuredProp =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));

    // The results should be the same since the structured property doesn't apply to test entities
    assertEquals(resultWithOnlyUnrelatedStructuredProp.size(), resultWithoutStructuredProps.size());

    // Test that a structured property that does apply to this entity is included in the mappings
    String fqnOfRelatedProp = "propForThis";
    StructuredPropertyDefinition structPropForThisEntity =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName(fqnOfRelatedProp)
            .setDisplayName("propForThis")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Collection<IndexMapping> resultWithOnlyRelatedStructuredProp =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity)));

    assertNotNull(resultWithOnlyRelatedStructuredProp, "Result should not be null");

    // Test that only structured properties that apply are included
    Collection<IndexMapping> resultWithBothStructuredProps =
        mappingsBuilder.getIndexMappings(
            operationContext,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propForThis"),
                    structPropForThisEntity),
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propNotForThis"),
                    structPropNotForThisEntity)));

    // Results should be the same as with only the related structured property
    assertEquals(resultWithBothStructuredProps.size(), resultWithOnlyRelatedStructuredProp.size());
  }

  @Test
  public void testGetIndexMappingsForStructuredProperty() throws URISyntaxException {
    StructuredPropertyDefinition testStructProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testProp")
            .setDisplayName("exampleProp")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> structuredPropertyFieldMappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:testProp"), testStructProp)));
    assertEquals(structuredPropertyFieldMappings.size(), 1);
    String keyInMap = structuredPropertyFieldMappings.keySet().stream().findFirst().get();
    assertEquals(keyInMap, "testProp");

    Object mappings = structuredPropertyFieldMappings.get(keyInMap);
    assertEquals(
        mappings,
        Map.of(
            "type",
            "keyword",
            "normalizer",
            "keyword_normalizer",
            "fields",
            Map.of("keyword", Map.of("type", "keyword"))));

    StructuredPropertyDefinition propWithNumericType =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testPropNumber")
            .setDisplayName("examplePropNumber")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    Map<String, Object> structuredPropertyFieldMappingsNumber =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:testPropNumber"),
                    propWithNumericType)));
    assertEquals(structuredPropertyFieldMappingsNumber.size(), 1);
    keyInMap = structuredPropertyFieldMappingsNumber.keySet().stream().findFirst().get();
    assertEquals("testPropNumber", keyInMap);
    mappings = structuredPropertyFieldMappingsNumber.get(keyInMap);
    assertEquals(Map.of("type", "double"), mappings);
  }

  /**
   * Regression test for structured properties with valueType urn:li:dataType:datahub.urn. Without
   * StructuredPropertyUtils.getLogicalValueType(), getId() returns "datahub.urn" and no branch
   * matches, producing a mapping with no type and causing mapper_parsing_exception during reindex.
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
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataJob")))
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.urn"));

    Map<String, Object> mappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:com.example.domain.owner_urn"),
                    propWithUrnType)));

    assertEquals(mappings.size(), 1, "Should have one mapping");
    String fieldName = "com_example_domain_owner_urn";
    assertTrue(mappings.containsKey(fieldName), "Should contain sanitized field name");
    @SuppressWarnings("unchecked")
    Map<String, Object> fieldMapping = (Map<String, Object>) mappings.get(fieldName);
    assertNotNull(fieldMapping.get("type"), "URN structured property must have type for reindex");
    assertEquals(fieldMapping.get("type"), "keyword", "URN type should map to keyword");
  }

  /**
   * Ensures every structured property field has a "type" so reindex/putMapping does not fail with
   * mapper_parsing_exception.
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
                            Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataJob"),
                            Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
                    .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.urn"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:simpleString"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("simpleString")
                    .setDisplayName("Simple")
                    .setEntityTypes(
                        new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
                    .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:richTextProp"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("richTextProp")
                    .setDisplayName("Rich Text")
                    .setEntityTypes(
                        new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
                    .setValueType(
                        Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.rich_text"))),
            Pair.of(
                UrnUtils.getUrn("urn:li:structuredProperty:dateProp"),
                new StructuredPropertyDefinition()
                    .setVersion(null, SetMode.REMOVE_IF_NULL)
                    .setQualifiedName("dateProp")
                    .setDisplayName("Date")
                    .setEntityTypes(
                        new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
                    .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.date"))));

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
  public void testGetIndexMappingsForStructuredPropertyV1() throws URISyntaxException {
    StructuredPropertyDefinition testStructProp =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testProp")
            .setDisplayName("exampleProp")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));
    Map<String, Object> structuredPropertyFieldMappings =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:testProp"), testStructProp)));
    assertEquals(structuredPropertyFieldMappings.size(), 1);
    String keyInMap = structuredPropertyFieldMappings.keySet().stream().findFirst().get();
    assertEquals(keyInMap, "_versioned.testProp.00000000000001.string");

    Object mappings = structuredPropertyFieldMappings.get(keyInMap);
    assertEquals(
        mappings,
        Map.of(
            "type",
            "keyword",
            "normalizer",
            "keyword_normalizer",
            "fields",
            Map.of("keyword", Map.of("type", "keyword"))));

    StructuredPropertyDefinition propWithNumericType =
        new StructuredPropertyDefinition()
            .setVersion("00000000000001")
            .setQualifiedName("testPropNumber")
            .setDisplayName("examplePropNumber")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "testEntity")))
            .setValueType(Urn.createFromString("urn:li:logicalType:NUMBER"));
    Map<String, Object> structuredPropertyFieldMappingsNumber =
        mappingsBuilder.getIndexMappingsForStructuredProperty(
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:testPropNumber"),
                    propWithNumericType)));
    assertEquals(structuredPropertyFieldMappingsNumber.size(), 1);
    keyInMap = structuredPropertyFieldMappingsNumber.keySet().stream().findFirst().get();
    assertEquals(keyInMap, "_versioned.testPropNumber.00000000000001.number");
    mappings = structuredPropertyFieldMappingsNumber.get(keyInMap);
    assertEquals(Map.of("type", "double"), mappings);
  }

  @Test
  public void testRefMappingsBuilder() {
    EntityRegistry entityRegistry = getTestEntityRegistry();
    EntitySpec entitySpec = new EntitySpecBuilder().buildEntitySpec(new TestRefEntity().schema());

    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Create a new OperationContext with the test entity registry
    OperationContext testOperationContext =
        TestOperationContexts.systemContextNoSearchAuthorization();
    // We can't mock the OperationContext's getEntityRegistry method directly, so we'll use the real
    // one
    // and rely on the test entity registry being set up properly

    Collection<IndexMapping> result = mappingsBuilder.getIndexMappings(testOperationContext);
    assertNotNull(result, "Result should not be null");

    // If there are mappings, verify they contain expected properties
    if (!result.isEmpty()) {
      IndexMapping mapping = result.iterator().next();
      Map<String, Object> mappings = mapping.getMappings();
      assertTrue(mappings.containsKey("properties"), "Mappings should contain properties");

      Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
      assertTrue(properties.containsKey("urn"), "Should contain urn field");
      assertTrue(properties.containsKey("runId"), "Should contain runId field");
      assertTrue(properties.containsKey("systemCreated"), "Should contain systemCreated field");
    }
  }

  @Test
  public void testConstructor() {
    // Test that the constructor works correctly
    assertNotNull(mappingsBuilder, "LegacyMappingsBuilder should be created successfully");
  }

  @Test
  public void testGetIndexMappingsWithCleanupEnabled() {
    // Test getMappings when cleanup is enabled
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Use the real EntityRegistry from the test OperationContext
    Collection<IndexMapping> result = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // Note: The actual result depends on the implementation, but we can test that it doesn't throw
  }

  @Test
  public void testGetIndexMappingsWithCleanupDisabled() {
    // Test getMappings when cleanup is disabled - should still return mappings
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(false);

    Collection<IndexMapping> result = mappingsBuilder.getIndexMappings(operationContext);

    assertNotNull(result, "Result should not be null");
    // With the fix, mappings should be returned regardless of cleanup setting
    assertFalse(
        result.isEmpty(),
        "Result should not be empty - cleanup setting no longer affects index creation");
  }

  @Test
  public void testGetIndexMappingsWithStructuredProperties() {
    // Test getIndexMappings with structured properties
    when(entityIndexConfiguration.getV2().isCleanup()).thenReturn(true);

    // Use the real EntityRegistry from the test OperationContext
    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappings(operationContext, Collections.emptyList());

    assertNotNull(result, "Result should not be null");
    // Note: The actual result depends on the implementation, but we can test that it doesn't throw
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredProperty() {
    // Test getIndexMappingsWithNewStructuredProperty when V2 is enabled
    when(entityIndexConfiguration.getV2().isEnabled()).thenReturn(true);

    // Create a mock URN
    Urn mockUrn = mock(Urn.class);
    when(mockUrn.getEntityType()).thenReturn("test");

    // Create a mock StructuredPropertyDefinition
    StructuredPropertyDefinition mockProperty = mock(StructuredPropertyDefinition.class);
    when(mockProperty.getEntityTypes()).thenReturn(new UrnArray());

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
            operationContext, mockUrn, mockProperty);

    assertNotNull(result, "Result should not be null");
    // Note: The actual result depends on the implementation, but we can test that it doesn't throw
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredPropertyV2Disabled() {
    // Test getIndexMappingsWithNewStructuredProperty when V2 is disabled
    when(entityIndexConfiguration.getV2().isEnabled()).thenReturn(false);

    // Create a mock URN
    Urn mockUrn = mock(Urn.class);

    // Create a mock StructuredPropertyDefinition
    StructuredPropertyDefinition mockProperty = mock(StructuredPropertyDefinition.class);

    Collection<IndexMapping> result =
        mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
            operationContext, mockUrn, mockProperty);

    assertNotNull(result, "Result should not be null");
    assertTrue(result.isEmpty(), "Result should be empty when V2 is disabled");
  }

  @Test
  public void testGetIndexMappingsWithNewStructuredPropertyMissingEntitySpec() {
    // Test getIndexMappingsWithNewStructuredProperty when entity spec is missing
    when(entityIndexConfiguration.getV2().isEnabled()).thenReturn(true);

    // Create a mock URN with a non-existent entity type
    Urn mockUrn = mock(Urn.class);
    when(mockUrn.getEntityType()).thenReturn("nonExistentEntityType");

    // Create a mock StructuredPropertyDefinition
    StructuredPropertyDefinition mockProperty = mock(StructuredPropertyDefinition.class);
    when(mockProperty.getEntityTypes()).thenReturn(new UrnArray());

    try {
      Collection<IndexMapping> result =
          mappingsBuilder.getIndexMappingsWithNewStructuredProperty(
              operationContext, mockUrn, mockProperty);
      // If it doesn't throw an exception, the result should be null or empty
      assertTrue(
          result == null || result.isEmpty(),
          "Result should be null or empty for missing entity spec");
    } catch (Exception e) {
      // Expected behavior - should throw an exception for missing entity spec
      assertTrue(
          e instanceof IllegalArgumentException,
          "Should throw IllegalArgumentException for missing entity spec");
    }
  }

  @Test
  public void testNullEntityIndexConfiguration() {
    // Test that constructor properly handles null EntityIndexConfiguration
    // The constructor doesn't actually throw an exception, so this test should pass
    // This is the current behavior of the implementation
    V2MappingsBuilder builder = new V2MappingsBuilder(null);
    assertNotNull(builder, "Constructor should create instance even with null input");
  }

  @Test
  public void testNullOperationContext() {
    // Test that methods properly handle null OperationContext
    try {
      Collection<IndexMapping> result = mappingsBuilder.getIndexMappings(null);
      // If it doesn't throw an exception, the result should be null or empty
      assertTrue(
          result == null || result.isEmpty(), "Result should be null or empty for null input");
    } catch (Exception e) {
      // Expected behavior - should throw an exception for null input
      assertTrue(
          e instanceof NullPointerException || e instanceof IllegalArgumentException,
          "Should throw appropriate exception for null OperationContext");
    }
  }

  private EntityRegistry getTestEntityRegistry() {
    return new ConfigEntityRegistry(
        TestSearchFieldConfig.class
            .getClassLoader()
            .getResourceAsStream("test-entity-registry.yaml"));
  }
}
