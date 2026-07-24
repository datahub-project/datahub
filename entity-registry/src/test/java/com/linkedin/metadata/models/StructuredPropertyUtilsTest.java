package com.linkedin.metadata.models;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.test.metadata.aspect.MockAspectRetriever;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

public class StructuredPropertyUtilsTest {

  @Test
  public void testGetEntityTypeIdWithDatahubPrefix() {
    // URN with datahub. prefix (current format)
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    String entityTypeId = StructuredPropertyUtils.getEntityTypeId(entityTypeUrn);
    assertEquals(entityTypeId, "dataset");
  }

  @Test
  public void testGetEntityTypeIdWithoutDatahubPrefix() {
    // URN without datahub. prefix (legacy format)
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:dataset");
    String entityTypeId = StructuredPropertyUtils.getEntityTypeId(entityTypeUrn);
    assertEquals(entityTypeId, "dataset");
  }

  @Test
  public void testGetMissingPropertyDefinitionUrnsWhenEntityExistsWithoutDefinition()
      throws URISyntaxException {
    Urn propertyUrnA =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
    Urn propertyUrnMissing =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.deleted");
    Urn propertyUrnOrphanKeyOnly =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.keyOnly");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"));

    MockAspectRetriever retriever =
        new MockAspectRetriever(
            Map.of(
                propertyUrnA, List.of(definition),
                propertyUrnOrphanKeyOnly, List.of()));

    Set<Urn> missing =
        StructuredPropertyUtils.getMissingPropertyDefinitionUrns(
            OperationFingerprint.EMPTY,
            Set.of(propertyUrnA, propertyUrnMissing, propertyUrnOrphanKeyOnly),
            retriever);

    assertEquals(missing, Set.of(propertyUrnMissing, propertyUrnOrphanKeyOnly));
  }

  @Test
  public void testFilterMissingPropertyDefinitions() throws URISyntaxException {
    Urn propertyUrnA =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
    Urn propertyUrnMissing =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.deleted");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"));

    StructuredProperties properties =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnA)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(1.0))),
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnMissing)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(2.0)))));

    MockAspectRetriever retriever =
        new MockAspectRetriever(Map.of(propertyUrnA, List.of(definition)));

    Pair<StructuredProperties, Set<Urn>> result =
        StructuredPropertyUtils.filterMissingPropertyDefinitions(
            OperationFingerprint.EMPTY, properties, retriever);

    assertEquals(result.getFirst().getProperties().size(), 1);
    assertEquals(result.getFirst().getProperties().get(0).getPropertyUrn(), propertyUrnA);
    assertEquals(result.getSecond(), Set.of(propertyUrnMissing));
  }

  @Test
  public void testFilterMissingPropertyDefinitionsFromPrefetchedMap() throws URISyntaxException {
    Urn propertyUrnA =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.retentionTime");
    Urn propertyUrnMissing =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.deleted");
    Urn propertyUrnOrphanKeyOnly =
        Urn.createFromString("urn:li:structuredProperty:io.acryl.privacy.keyOnly");
    StructuredPropertyDefinition definition =
        new StructuredPropertyDefinition()
            .setValueType(Urn.createFromString("urn:li:type:datahub.string"));

    StructuredProperties properties =
        new StructuredProperties()
            .setProperties(
                new StructuredPropertyValueAssignmentArray(
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnA)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(1.0))),
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnMissing)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(2.0))),
                    new StructuredPropertyValueAssignment()
                        .setPropertyUrn(propertyUrnOrphanKeyOnly)
                        .setValues(
                            new PrimitivePropertyValueArray(PrimitivePropertyValue.create(3.0)))));

    Map<Urn, Map<String, Aspect>> prefetched =
        Map.of(
            propertyUrnA,
            Map.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, new Aspect(definition.data())),
            propertyUrnOrphanKeyOnly,
            Map.of());

    Pair<StructuredProperties, Set<Urn>> result =
        StructuredPropertyUtils.filterMissingPropertyDefinitions(properties, prefetched);

    assertEquals(result.getFirst().getProperties().size(), 1);
    assertEquals(result.getFirst().getProperties().get(0).getPropertyUrn(), propertyUrnA);
    assertEquals(result.getSecond(), Set.of(propertyUrnMissing, propertyUrnOrphanKeyOnly));
  }

  @Test
  public void testGetEntityTypeIdWithNull() {
    String entityTypeId = StructuredPropertyUtils.getEntityTypeId(null);
    assertNull(entityTypeId);
  }

  @Test
  public void testGetEntityTypeIdWithDataFlow() {
    // Test another entity type with datahub. prefix
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dataFlow");
    String entityTypeId = StructuredPropertyUtils.getEntityTypeId(entityTypeUrn);
    assertEquals(entityTypeId, "dataFlow");
  }

  @Test
  public void testGetEntityTypeIdWithSchemaField() {
    // Test schema field entity type
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:datahub.schemaField");
    String entityTypeId = StructuredPropertyUtils.getEntityTypeId(entityTypeUrn);
    assertEquals(entityTypeId, "schemaField");
  }

  @Test
  public void testEntityTypeMatchesWithDatahubPrefix() {
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    assertTrue(StructuredPropertyUtils.entityTypeMatches(entityTypeUrn, "dataset"));
    assertFalse(StructuredPropertyUtils.entityTypeMatches(entityTypeUrn, "dataFlow"));
  }

  @Test
  public void testEntityTypeMatchesWithoutDatahubPrefix() {
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:dataset");
    assertTrue(StructuredPropertyUtils.entityTypeMatches(entityTypeUrn, "dataset"));
    assertFalse(StructuredPropertyUtils.entityTypeMatches(entityTypeUrn, "dataFlow"));
  }

  @Test
  public void testEntityTypeMatchesWithNull() {
    assertFalse(StructuredPropertyUtils.entityTypeMatches(null, "dataset"));
  }

  @Test
  public void testEntityTypeMatchesCaseSensitive() {
    // Ensure matching is case-sensitive
    Urn entityTypeUrn = UrnUtils.getUrn("urn:li:entityType:datahub.dataset");
    assertFalse(StructuredPropertyUtils.entityTypeMatches(entityTypeUrn, "Dataset"));
    assertFalse(StructuredPropertyUtils.entityTypeMatches(entityTypeUrn, "DATASET"));
  }

  @Test
  public void testToElasticsearchFieldNameCollidingQualifiedNames() {
    Urn urnDot = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnUnderscore = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    StructuredPropertyDefinition defDot =
        new StructuredPropertyDefinition().setQualifiedName("certification.status");
    StructuredPropertyDefinition defUnderscore =
        new StructuredPropertyDefinition().setQualifiedName("certification_status");

    assertEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(urnDot, defDot),
        StructuredPropertyUtils.toElasticsearchFieldName(urnUnderscore, defUnderscore));
    assertEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(urnDot, defDot), "certification_status");
  }

  @Test
  public void testToElasticsearchFieldNameNestedDotVariantsCollide() {
    Urn urn1 = UrnUtils.getUrn("urn:li:structuredProperty:a.b.c");
    Urn urn2 = UrnUtils.getUrn("urn:li:structuredProperty:a_b.c");
    Urn urn3 = UrnUtils.getUrn("urn:li:structuredProperty:a.b_c");
    assertEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(
            urn1, new StructuredPropertyDefinition().setQualifiedName("a.b.c")),
        "a_b_c");
    assertEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(
            urn2, new StructuredPropertyDefinition().setQualifiedName("a_b.c")),
        "a_b_c");
    assertEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(
            urn3, new StructuredPropertyDefinition().setQualifiedName("a.b_c")),
        "a_b_c");
  }

  @Test
  public void testToElasticsearchFieldNameVersionedDoesNotCollideWithUnversioned() {
    Urn urn = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    StructuredPropertyDefinition unversioned =
        new StructuredPropertyDefinition().setQualifiedName("certification.status");
    StructuredPropertyDefinition versioned =
        new StructuredPropertyDefinition()
            .setQualifiedName("certification.status")
            .setVersion("00000000000001")
            .setValueType(UrnUtils.getUrn("urn:li:dataType:datahub.string"));

    assertEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(urn, unversioned), "certification_status");
    assertTrue(
        StructuredPropertyUtils.toElasticsearchFieldName(urn, versioned)
            .startsWith("_versioned.certification_status."));
    assertNotEquals(
        StructuredPropertyUtils.toElasticsearchFieldName(urn, unversioned),
        StructuredPropertyUtils.toElasticsearchFieldName(urn, versioned));
  }

  @Test
  public void testResolveStructuredPropertyMappingCollisionsSameTypeKeepsLowestUrn() {
    Urn urnA = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnB = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    // urnA < urnB lexicographically (...status with dot vs underscore: '.' < '_')
    Map<String, Object> mapping = Map.of("type", "keyword");
    Map<String, Object> resolved =
        StructuredPropertyUtils.resolveStructuredPropertyMappingCollisions(
            List.of(
                new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                    "certification_status", urnB, mapping),
                new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                    "certification_status", urnA, mapping)));
    assertEquals(resolved.size(), 1);
    assertEquals(resolved.get("certification_status"), mapping);
  }

  @Test
  public void testResolveStructuredPropertyMappingCollisionsDifferentTypeOmitsField() {
    Urn urnA = UrnUtils.getUrn("urn:li:structuredProperty:certification.status");
    Urn urnB = UrnUtils.getUrn("urn:li:structuredProperty:certification_status");
    Map<String, Object> resolved =
        StructuredPropertyUtils.resolveStructuredPropertyMappingCollisions(
            List.of(
                new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                    "certification_status", urnA, Map.of("type", "keyword")),
                new StructuredPropertyUtils.StructuredPropertyFieldMapping(
                    "certification_status", urnB, Map.of("type", "double"))));
    assertTrue(resolved.isEmpty());
  }
}
