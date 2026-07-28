package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.DATA_TYPE_URN_PREFIX;
import static com.linkedin.metadata.Constants.ENTITY_TYPE_URN_PREFIX;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.annotation.EntityAnnotation;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
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
}
