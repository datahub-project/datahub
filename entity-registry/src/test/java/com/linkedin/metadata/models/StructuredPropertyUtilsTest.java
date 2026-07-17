package com.linkedin.metadata.models;

import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
}
