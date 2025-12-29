package com.linkedin.metadata.aspect.utils;

import static org.testng.Assert.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.DatasetAssertionScope;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FieldAssertionType;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionType;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.SqlAssertionType;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.assertion.VolumeAssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.function.Function;
import org.testng.annotations.Test;

public class AssertionUtilsTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,testDataset,PROD)");

  // ==================== isValidSubProperty Tests ====================

  @Test
  public void testIsValidSubPropertyWithValidProperty() {
    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessInfo.setEntity(TEST_DATASET_URN);

    assertTrue(
        AssertionUtils.isValidSubProperty(true, () -> freshnessInfo),
        "Expected valid when property is present and non-empty");
  }

  @Test
  public void testIsValidSubPropertyWithNullProperty() {
    assertFalse(
        AssertionUtils.isValidSubProperty(true, () -> null),
        "Expected false when property getter returns null");
  }

  @Test
  public void testIsValidSubPropertyWithEmptyProperty() {
    // Empty record template - no fields set
    FreshnessAssertionInfo emptyInfo = new FreshnessAssertionInfo();

    assertFalse(
        AssertionUtils.isValidSubProperty(true, () -> emptyInfo),
        "Expected false when property is empty (no fields set)");
  }

  @Test
  public void testIsValidSubPropertyWhenNotPresent() {
    assertFalse(
        AssertionUtils.isValidSubProperty(false, () -> new FreshnessAssertionInfo()),
        "Expected false when hasProperty is false");
  }

  // ==================== getExpectedPropertyName Tests ====================

  @Test
  public void testGetExpectedPropertyNameForAllTypes() {
    assertEquals(AssertionUtils.getExpectedPropertyName(AssertionType.DATASET), "datasetAssertion");
    assertEquals(
        AssertionUtils.getExpectedPropertyName(AssertionType.FRESHNESS), "freshnessAssertion");
    assertEquals(AssertionUtils.getExpectedPropertyName(AssertionType.VOLUME), "volumeAssertion");
    assertEquals(AssertionUtils.getExpectedPropertyName(AssertionType.SQL), "sqlAssertion");
    assertEquals(AssertionUtils.getExpectedPropertyName(AssertionType.FIELD), "fieldAssertion");
    assertEquals(
        AssertionUtils.getExpectedPropertyName(AssertionType.DATA_SCHEMA), "schemaAssertion");
    assertEquals(AssertionUtils.getExpectedPropertyName(AssertionType.CUSTOM), "customAssertion");
  }

  // ==================== getEntityFromAssertionInfo Tests ====================

  @Test
  public void testGetEntityFromAssertionInfoWithNull() {
    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(null),
        "Expected null when assertionInfo is null");
  }

  @Test
  public void testGetEntityFromAssertionInfoWithNoType() {
    AssertionInfo assertionInfo = new AssertionInfo();
    // No type set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when assertionInfo has no type");
  }

  @Test
  public void testGetEntityFromDatasetAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);

    DatasetAssertionInfo datasetInfo = new DatasetAssertionInfo();
    datasetInfo.setDataset(TEST_DATASET_URN);
    datasetInfo.setScope(DatasetAssertionScope.DATASET_ROWS);
    assertionInfo.setDatasetAssertion(datasetInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from datasetAssertion.dataset");
  }

  @Test
  public void testGetEntityFromDatasetAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATASET);
    // No datasetAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when datasetAssertion is null");
  }

  @Test
  public void testGetEntityFromFreshnessAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);

    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setFreshnessAssertion(freshnessInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from freshnessAssertion.entity");
  }

  @Test
  public void testGetEntityFromFreshnessAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    // No freshnessAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when freshnessAssertion is null");
  }

  @Test
  public void testGetEntityFromVolumeAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);

    VolumeAssertionInfo volumeInfo = new VolumeAssertionInfo();
    volumeInfo.setType(VolumeAssertionType.ROW_COUNT_TOTAL);
    volumeInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setVolumeAssertion(volumeInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from volumeAssertion.entity");
  }

  @Test
  public void testGetEntityFromVolumeAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);
    // No volumeAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when volumeAssertion is null");
  }

  @Test
  public void testGetEntityFromSqlAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.SQL);

    SqlAssertionInfo sqlInfo = new SqlAssertionInfo();
    sqlInfo.setType(SqlAssertionType.METRIC);
    sqlInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setSqlAssertion(sqlInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from sqlAssertion.entity");
  }

  @Test
  public void testGetEntityFromSqlAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.SQL);
    // No sqlAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when sqlAssertion is null");
  }

  @Test
  public void testGetEntityFromFieldAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FIELD);

    FieldAssertionInfo fieldInfo = new FieldAssertionInfo();
    fieldInfo.setType(FieldAssertionType.FIELD_VALUES);
    fieldInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setFieldAssertion(fieldInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from fieldAssertion.entity");
  }

  @Test
  public void testGetEntityFromFieldAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FIELD);
    // No fieldAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when fieldAssertion is null");
  }

  @Test
  public void testGetEntityFromSchemaAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATA_SCHEMA);

    SchemaAssertionInfo schemaInfo = new SchemaAssertionInfo();
    schemaInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setSchemaAssertion(schemaInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from schemaAssertion.entity");
  }

  @Test
  public void testGetEntityFromSchemaAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.DATA_SCHEMA);
    // No schemaAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when schemaAssertion is null");
  }

  @Test
  public void testGetEntityFromCustomAssertion() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);

    CustomAssertionInfo customInfo = new CustomAssertionInfo();
    customInfo.setType("custom-type");
    customInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setCustomAssertion(customInfo);

    assertEquals(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        TEST_DATASET_URN,
        "Expected entity from customAssertion.entity");
  }

  @Test
  public void testGetEntityFromCustomAssertionWithNullSubProperty() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);
    // No customAssertion set

    assertNull(
        AssertionUtils.getEntityFromAssertionInfo(assertionInfo),
        "Expected null when customAssertion is null");
  }

  // ==================== ASSERTION_TYPE_SUB_PROPERTY_CHECKS Tests ====================

  @Test
  public void testSubPropertyChecksMapContainsAllTypes() {
    // Verify all expected types are in the map
    assertTrue(
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.DATASET));
    assertTrue(
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.FRESHNESS));
    assertTrue(AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.VOLUME));
    assertTrue(AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.SQL));
    assertTrue(AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.FIELD));
    assertTrue(
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.DATA_SCHEMA));
    assertTrue(AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.containsKey(AssertionType.CUSTOM));
  }

  @Test
  public void testSubPropertyCheckFreshnessValid() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);

    FreshnessAssertionInfo freshnessInfo = new FreshnessAssertionInfo();
    freshnessInfo.setType(FreshnessAssertionType.DATASET_CHANGE);
    freshnessInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setFreshnessAssertion(freshnessInfo);

    Function<AssertionInfo, Boolean> checker =
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.get(AssertionType.FRESHNESS);
    assertTrue(checker.apply(assertionInfo), "Expected check to pass for valid freshnessAssertion");
  }

  @Test
  public void testSubPropertyCheckFreshnessInvalid() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.FRESHNESS);
    // No freshnessAssertion set

    Function<AssertionInfo, Boolean> checker =
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.get(AssertionType.FRESHNESS);
    assertFalse(
        checker.apply(assertionInfo), "Expected check to fail for missing freshnessAssertion");
  }

  @Test
  public void testSubPropertyCheckVolumeValid() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);

    VolumeAssertionInfo volumeInfo = new VolumeAssertionInfo();
    volumeInfo.setType(VolumeAssertionType.ROW_COUNT_TOTAL);
    volumeInfo.setEntity(TEST_DATASET_URN);
    assertionInfo.setVolumeAssertion(volumeInfo);

    Function<AssertionInfo, Boolean> checker =
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.get(AssertionType.VOLUME);
    assertTrue(checker.apply(assertionInfo), "Expected check to pass for valid volumeAssertion");
  }

  @Test
  public void testSubPropertyCheckVolumeInvalid() {
    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.VOLUME);
    // No volumeAssertion set

    Function<AssertionInfo, Boolean> checker =
        AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS.get(AssertionType.VOLUME);
    assertFalse(checker.apply(assertionInfo), "Expected check to fail for missing volumeAssertion");
  }
}
