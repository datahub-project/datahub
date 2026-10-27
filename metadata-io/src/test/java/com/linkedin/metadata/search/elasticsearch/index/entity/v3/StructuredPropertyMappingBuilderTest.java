package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.ENTITY_TYPE_URN_PREFIX;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Tests for StructuredPropertyMappingBuilder with focus on entity type URN format handling. */
public class StructuredPropertyMappingBuilderTest {

  private EntitySpec mockEntitySpec;
  private EntityAnnotation mockEntityAnnotation;

  @BeforeMethod
  public void setUp() {
    mockEntitySpec = mock(EntitySpec.class);
    mockEntityAnnotation = mock(EntityAnnotation.class);
    when(mockEntitySpec.getEntityAnnotation()).thenReturn(mockEntityAnnotation);
    when(mockEntityAnnotation.getName()).thenReturn("dataset");
  }

  @Test
  public void testCreateStructuredPropertyMappingsWithLegacyFormat() throws URISyntaxException {
    // Test with legacy format (without datahub. prefix)
    StructuredPropertyDefinition structProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testProp")
            .setDisplayName("testProp")
            .setEntityTypes(
                new UrnArray(
                    // Legacy format: urn:li:entityType:dataset
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:testProp"), structProp)));

    assertNotNull(mappings, "Mappings should not be null");
    assertEquals(mappings.size(), 1, "Should have one mapping for the structured property");
    assertTrue(mappings.containsKey("testProp"), "Mapping should contain testProp key");
  }

  @Test
  public void testCreateStructuredPropertyMappingsWithDatahubPrefix() throws URISyntaxException {
    // Test with datahub. prefix format (production format)
    StructuredPropertyDefinition structProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testPropDatahub")
            .setDisplayName("testPropDatahub")
            .setEntityTypes(
                new UrnArray(
                    // Production format: urn:li:entityType:datahub.dataset
                    Urn.createFromString("urn:li:entityType:datahub.dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:testPropDatahub"), structProp)));

    assertNotNull(mappings, "Mappings should not be null");
    assertEquals(
        mappings.size(),
        1,
        "Should have one mapping for the structured property with datahub prefix");
    assertTrue(
        mappings.containsKey("testPropDatahub"), "Mapping should contain testPropDatahub key");
  }

  @Test
  public void testCreateStructuredPropertyMappingsWithMixedFormats() throws URISyntaxException {
    // Test with mixed formats in the same property
    StructuredPropertyDefinition structProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testPropMixed")
            .setDisplayName("testPropMixed")
            .setEntityTypes(
                new UrnArray(
                    // Both formats in the same property
                    Urn.createFromString("urn:li:entityType:datahub.dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataFlow")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:testPropMixed"), structProp)));

    assertNotNull(mappings, "Mappings should not be null");
    assertEquals(
        mappings.size(), 1, "Should have one mapping since dataset matches (via datahub.dataset)");
    assertTrue(mappings.containsKey("testPropMixed"), "Mapping should contain testPropMixed key");
  }

  @Test
  public void testCreateStructuredPropertyMappingsNoMatch() throws URISyntaxException {
    // Test that non-matching entity types are filtered out
    StructuredPropertyDefinition structProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testPropNoMatch")
            .setDisplayName("testPropNoMatch")
            .setEntityTypes(
                new UrnArray(
                    // Entity types that don't match "dataset"
                    Urn.createFromString("urn:li:entityType:datahub.dataFlow"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "chart")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:testPropNoMatch"), structProp)));

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Should have no mappings since entity types don't match");
  }

  @Test
  public void testCreateStructuredPropertyMappingsWithEmptyEntityTypes() throws URISyntaxException {
    // Test that empty entityTypes is handled gracefully
    StructuredPropertyDefinition structProp =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("testPropEmptyTypes")
            .setDisplayName("testPropEmptyTypes")
            .setEntityTypes(new UrnArray()) // Empty array
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:testPropEmptyTypes"), structProp)));

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Should have no mappings when entityTypes is empty");
  }

  @Test
  public void testCreateStructuredPropertyMappingsWithEmptyCollection() {
    // Test with empty structured properties collection
    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec, List.of());

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Should have no mappings for empty collection");
  }

  @Test
  public void testCreateStructuredPropertyMappingsWithNullCollection() {
    // Test with null structured properties collection
    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(mockEntitySpec, null);

    assertNotNull(mappings, "Mappings should not be null");
    assertTrue(mappings.isEmpty(), "Should have no mappings for null collection");
  }

  @Test
  public void testBothUrnFormatsProduceSameResult() throws URISyntaxException {
    // Verify that both URN formats produce the same result for entity matching
    StructuredPropertyDefinition structPropLegacy =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("propLegacy")
            .setDisplayName("propLegacy")
            .setEntityTypes(new UrnArray(Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    StructuredPropertyDefinition structPropDatahub =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("propDatahub")
            .setDisplayName("propDatahub")
            .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")))
            .setValueType(Urn.createFromString("urn:li:logicalType:STRING"));

    Map<String, Object> mappingsLegacy =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propLegacy"), structPropLegacy)));

    Map<String, Object> mappingsDatahub =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(
                Pair.of(
                    UrnUtils.getUrn("urn:li:structuredProperty:propDatahub"), structPropDatahub)));

    // Both should produce exactly one mapping
    assertEquals(
        mappingsLegacy.size(),
        mappingsDatahub.size(),
        "Both URN formats should produce the same number of mappings");
    assertEquals(mappingsLegacy.size(), 1, "Legacy format should produce one mapping");
    assertEquals(mappingsDatahub.size(), 1, "Datahub prefix format should produce one mapping");
  }

  /**
   * Regression test: valueType urn:li:dataType:datahub.urn must be resolved via
   * StructuredPropertyUtils.getLogicalValueType() so getMappingsForStructuredProperty does not
   * throw (LogicalValueType.valueOf("datahub.urn") would throw) and returns a mapping with a type
   * for reindex.
   */
  @Test
  public void testGetMappingsForStructuredPropertyWithDatahubUrnValueType()
      throws URISyntaxException {
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("com.example.domain.owner_urn")
            .setDisplayName("Owner URN")
            .setEntityTypes(
                new UrnArray(
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataset"),
                    Urn.createFromString(ENTITY_TYPE_URN_PREFIX + "dataJob")))
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.urn"));

    Map<String, Object> mapping =
        StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(definition);

    assertNotNull(mapping, "Mapping should not be null");
    assertNotNull(mapping.get("type"), "URN structured property must have type for reindex");
    assertEquals(mapping.get("type"), "keyword", "URN type should map to keyword");
  }

  @Test
  public void testSearchConfigurationOptsIntoAnalyzedSubfields() {
    for (String valueType : List.of("string", "rich_text")) {
      Map<String, Object> unconfigured =
          StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(
              propertyOfType(valueType));
      assertFalse(unconfigured.containsKey("copy_to"), valueType);
      assertFalse(analyzedSubfields(unconfigured).contains("text"), valueType);

      Map<String, Object> configured =
          StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(
              propertyOfType(valueType)
                  .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(false)));
      assertFalse(configured.containsKey("copy_to"), valueType);
      assertEquals(configured.get("type"), "keyword", valueType);
      assertAnalyzedSubfields(configured, valueType);
      if (!"urn".equals(valueType)) {
        assertTrue(analyzedSubfields(configured).contains("keyword"), configured.toString());
      }

      // V3 ignores the V2 exclusion once a configuration is present
      Map<String, Object> excluded =
          StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(
              propertyOfType(valueType)
                  .setSearchConfiguration(
                      new DataHubSearchConfig()
                          .setQueryByDefault(true)
                          .setExcludeFromFullTextSearch(true)));
      assertAnalyzedSubfields(excluded, valueType);
    }
    StructuredPropertyDefinition urn =
        propertyOfType("urn")
            .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true));
    Map<String, Object> urnValue =
        StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(urn);
    assertEquals(urnValue.get("type"), "keyword");
    assertFalse(urnValue.containsKey("normalizer"), urnValue.toString());
    assertFalse(urnValue.containsKey("copy_to"), urnValue.toString());
    assertFalse(analyzedSubfields(urnValue).contains("text"), urnValue.toString());
    assertEquals(
        ((Map<String, Object>) subfield(urnValue, "lowercase")).get("normalizer"),
        "keyword_normalizer");
    Map<String, Object> urnIdentity = StructuredPropertyUrnIdentity.mapping(urn, List.of());
    assertFalse(urnIdentity.containsKey("normalizer"), urnIdentity.toString());
    assertFalse(urnIdentity.containsKey("copy_to"), urnIdentity.toString());
    assertAnalyzedSubfields(urnIdentity, "urn");
    assertEquals(
        ((Map<String, Object>) subfield(urnIdentity, "lowercase")).get("normalizer"),
        "keyword_normalizer");
    Map<String, Object> exactOnly =
        StructuredPropertyUrnIdentity.mapping(propertyOfType("urn"), List.of());
    assertFalse(exactOnly.containsKey("normalizer"), exactOnly.toString());
    assertFalse(exactOnly.containsKey("copy_to"), exactOnly.toString());
    assertFalse(analyzedSubfields(exactOnly).contains("text"), exactOnly.toString());
    assertEquals(
        ((Map<String, Object>) subfield(exactOnly, "lowercase")).get("normalizer"),
        "keyword_normalizer");
    Map<String, Object> placed =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:prop"), urn)));
    assertFalse(analyzedSubfields((Map<String, Object>) placed.get("prop")).contains("text"));
    assertAnalyzedSubfields(
        (Map<String, Object>) placed.get("prop" + StructuredPropertyUrnIdentity.FIELD_SUFFIX),
        "urn");
    for (String valueType : List.of("number", "date")) {
      Map<String, Object> configured =
          StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(
              propertyOfType(valueType)
                  .setSearchConfiguration(new DataHubSearchConfig().setQueryByDefault(true)));
      assertFalse(configured.containsKey("copy_to"), valueType);
      assertFalse(analyzedSubfields(configured).contains("text"), configured.toString());
    }
  }

  private static Set<String> analyzedSubfields(Map<String, Object> mapping) {
    Object fields = mapping.get("fields");
    if (!(fields instanceof Map<?, ?> fieldsMap)) {
      return Set.of();
    }
    Set<String> names = new HashSet<>();
    fieldsMap.keySet().forEach(key -> names.add(String.valueOf(key)));
    return names;
  }

  private static void assertAnalyzedSubfields(Map<String, Object> mapping, String valueType) {
    Map<String, Object> expected = V3SearchFields.analyzedSubfields();
    assertEquals(subfield(mapping, "text"), expected.get("text"), valueType);
    assertEquals(subfield(mapping, "stemmed"), expected.get("stemmed"), valueType);
  }

  private static Object subfield(Map<String, Object> mapping, String name) {
    Object fields = mapping.get("fields");
    if (!(fields instanceof Map<?, ?> fieldsMap)) {
      return null;
    }
    return fieldsMap.get(name);
  }

  @Test
  public void testCopyToTargetsTheSharedFieldARootFieldFeeds() throws URISyntaxException {
    EntitySpec dataset = TestOperationContexts.defaultEntityRegistry().getEntitySpec("dataset");
    StructuredPropertyDefinition businessName =
        propertyOfType("string")
            .setQualifiedName("businessName")
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(false)
                    .setCopyTo(new StringArray(List.of("name", "entityName", "notAField"))));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            dataset,
            List.of(
                Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:businessName"), businessName)));

    @SuppressWarnings("unchecked")
    Map<String, Object> field = (Map<String, Object>) mappings.get("businessName");
    assertEquals(field.get("copy_to"), List.of("name"));
    assertEquals(field.get("type"), "keyword");
    assertAnalyzedSubfields(field, "string");

    StructuredPropertyDefinition notes =
        propertyOfType("string")
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(true)
                    .setCopyTo(new StringArray(List.of("description", "_search.other"))));
    Map<String, Object> noteMapping =
        StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(notes, List.of(dataset));
    assertEquals(noteMapping.get("copy_to"), List.of("description", "other"));

    Map<String, Object> number =
        StructuredPropertyMappingBuilder.getMappingsForStructuredProperty(
            propertyOfType("number")
                .setSearchConfiguration(
                    new DataHubSearchConfig()
                        .setQueryByDefault(true)
                        .setCopyTo(new StringArray(List.of("name")))),
            List.of(dataset));
    assertFalse(number.containsKey("copy_to"), number.toString());
    assertFalse(analyzedSubfields(number).contains("text"), number.toString());

    StructuredPropertyDefinition steward =
        propertyOfType("urn")
            .setQualifiedName("steward")
            .setSearchConfiguration(
                new DataHubSearchConfig()
                    .setQueryByDefault(true)
                    .setCopyTo(new StringArray(List.of("name")))
                    .setCopyUrnTo(new StringArray(List.of("other", "name"))));
    Map<String, Object> placed =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            dataset,
            List.of(Pair.of(UrnUtils.getUrn("urn:li:structuredProperty:steward"), steward)));
    @SuppressWarnings("unchecked")
    Map<String, Object> urnValue = (Map<String, Object>) placed.get("steward");
    assertEquals(urnValue.get("copy_to"), List.of("other", "name"), urnValue.toString());
    assertFalse(analyzedSubfields(urnValue).contains("text"), urnValue.toString());
    @SuppressWarnings("unchecked")
    Map<String, Object> parsed =
        (Map<String, Object>) placed.get("steward" + StructuredPropertyUrnIdentity.FIELD_SUFFIX);
    assertEquals(parsed.get("copy_to"), List.of("name"), parsed.toString());
    assertAnalyzedSubfields(parsed, "urn");
    assertFalse(parsed.containsKey("normalizer"), parsed.toString());
  }

  @Test
  public void testCreateStructuredPropertyMappingsSameTypeCollisionKeepsLowestUrn()
      throws URISyntaxException {
    Urn urnDot = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnUnderscore = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defDot =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification.status")
            .setDisplayName("Certification Status")
            .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")))
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));
    StructuredPropertyDefinition defUnderscore =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification_status")
            .setDisplayName("certification status")
            .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")))
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec,
            List.of(Pair.of(urnUnderscore, defUnderscore), Pair.of(urnDot, defDot)));

    assertEquals(mappings.size(), 1);
    assertTrue(mappings.containsKey("certification_status"));
  }

  @Test
  public void testCreateStructuredPropertyMappingsDifferentTypeCollisionOmitsField()
      throws URISyntaxException {
    Urn urnDot = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnUnderscore = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defString =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification.status")
            .setDisplayName("Certification Status")
            .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")))
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.string"));
    StructuredPropertyDefinition defNumber =
        new StructuredPropertyDefinition()
            .setVersion(null, SetMode.REMOVE_IF_NULL)
            .setQualifiedName("certification_status")
            .setDisplayName("certification status")
            .setEntityTypes(new UrnArray(Urn.createFromString("urn:li:entityType:datahub.dataset")))
            .setValueType(Urn.createFromString(DATA_TYPE_URN_PREFIX + "datahub.number"));

    Map<String, Object> mappings =
        StructuredPropertyMappingBuilder.createStructuredPropertyMappings(
            mockEntitySpec, List.of(Pair.of(urnDot, defString), Pair.of(urnUnderscore, defNumber)));

    assertFalse(mappings.containsKey("certification_status"));
  }

  private static StructuredPropertyDefinition propertyOfType(String valueType) {
    return new StructuredPropertyDefinition()
        .setQualifiedName("prop")
        .setEntityTypes(new UrnArray(UrnUtils.getUrn("urn:li:entityType:datahub.dataset")))
        .setValueType(UrnUtils.getUrn(DATA_TYPE_URN_PREFIX + "datahub." + valueType));
  }
}
