package com.linkedin.metadata.models;

import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
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
