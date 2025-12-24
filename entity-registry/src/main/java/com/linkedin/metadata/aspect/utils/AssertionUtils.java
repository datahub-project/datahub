package com.linkedin.metadata.aspect.utils;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.assertion.DatasetAssertionInfo;
import com.linkedin.assertion.FieldAssertionInfo;
import com.linkedin.assertion.FreshnessAssertionInfo;
import com.linkedin.assertion.SchemaAssertionInfo;
import com.linkedin.assertion.SqlAssertionInfo;
import com.linkedin.assertion.VolumeAssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility methods for working with AssertionInfo aspects.
 *
 * <p>This class provides common functionality for validating and extracting data from
 * AssertionInfo, used by both validators and consistency checks.
 */
@Slf4j
public class AssertionUtils {

  private AssertionUtils() {
    // Utility class - prevent instantiation
  }

  /**
   * Map of assertion types to their corresponding sub-property validation functions. Used by both
   * validators and consistency checks to verify that the correct sub-property is populated.
   */
  public static final Map<AssertionType, Function<AssertionInfo, Boolean>>
      ASSERTION_TYPE_SUB_PROPERTY_CHECKS =
          Collections.unmodifiableMap(
              Map.of(
                  AssertionType.DATASET,
                      info ->
                          isValidSubProperty(info.hasDatasetAssertion(), info::getDatasetAssertion),
                  AssertionType.FRESHNESS,
                      info ->
                          isValidSubProperty(
                              info.hasFreshnessAssertion(), info::getFreshnessAssertion),
                  AssertionType.VOLUME,
                      info ->
                          isValidSubProperty(info.hasVolumeAssertion(), info::getVolumeAssertion),
                  AssertionType.SQL,
                      info -> isValidSubProperty(info.hasSqlAssertion(), info::getSqlAssertion),
                  AssertionType.FIELD,
                      info -> isValidSubProperty(info.hasFieldAssertion(), info::getFieldAssertion),
                  AssertionType.DATA_SCHEMA,
                      info ->
                          isValidSubProperty(info.hasSchemaAssertion(), info::getSchemaAssertion),
                  AssertionType.CUSTOM,
                      info ->
                          isValidSubProperty(info.hasCustomAssertion(), info::getCustomAssertion)));

  /**
   * Check if a sub-property is valid (present and non-empty).
   *
   * @param hasProperty whether the property is set
   * @param getter supplier to get the property value
   * @return true if the property is valid (present and non-empty)
   */
  public static boolean isValidSubProperty(
      boolean hasProperty, Supplier<? extends RecordTemplate> getter) {
    if (!hasProperty) {
      return false;
    }
    RecordTemplate value = getter.get();
    if (value == null) {
      return false;
    }
    return !value.data().isEmpty();
  }

  /**
   * Get the expected property name for a given assertion type.
   *
   * @param type the assertion type
   * @return the expected property name (e.g., "freshnessAssertion" for FRESHNESS type)
   */
  public static String getExpectedPropertyName(AssertionType type) {
    switch (type) {
      case DATASET:
        return "datasetAssertion";
      case FRESHNESS:
        return "freshnessAssertion";
      case VOLUME:
        return "volumeAssertion";
      case SQL:
        return "sqlAssertion";
      case FIELD:
        return "fieldAssertion";
      case DATA_SCHEMA:
        return "schemaAssertion";
      case CUSTOM:
        return "customAssertion";
      default:
        return type.toString().toLowerCase() + "Assertion";
    }
  }

  /**
   * Extract the entity URN from an AssertionInfo's type-specific sub-property.
   *
   * <p>Each assertion type stores the entity reference in a different sub-property:
   *
   * <ul>
   *   <li>DATASET -&gt; datasetAssertion.dataset
   *   <li>FRESHNESS -&gt; freshnessAssertion.entity
   *   <li>VOLUME -&gt; volumeAssertion.entity
   *   <li>SQL -&gt; sqlAssertion.entity
   *   <li>FIELD -&gt; fieldAssertion.entity
   *   <li>DATA_SCHEMA -&gt; schemaAssertion.entity
   *   <li>CUSTOM -&gt; customAssertion.entity
   * </ul>
   *
   * @param assertionInfo the assertion info to extract entity from
   * @return the entity URN, or null if it cannot be derived
   */
  @Nullable
  public static Urn getEntityFromAssertionInfo(AssertionInfo assertionInfo) {
    if (assertionInfo == null) {
      return null;
    }
    if (!assertionInfo.hasType()) {
      log.warn(
          "AssertionInfo missing type; cannot derive entityUrn. assertionInfo={}", assertionInfo);
      return null;
    }

    switch (assertionInfo.getType()) {
      case DATASET:
        final DatasetAssertionInfo datasetAssertionInfo = assertionInfo.getDatasetAssertion();
        if (datasetAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=DATASET but datasetAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return datasetAssertionInfo.getDataset();
      case FRESHNESS:
        final FreshnessAssertionInfo freshnessAssertionInfo = assertionInfo.getFreshnessAssertion();
        if (freshnessAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=FRESHNESS but freshnessAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return freshnessAssertionInfo.getEntity();
      case VOLUME:
        final VolumeAssertionInfo volumeAssertionInfo = assertionInfo.getVolumeAssertion();
        if (volumeAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=VOLUME but volumeAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return volumeAssertionInfo.getEntity();
      case SQL:
        final SqlAssertionInfo sqlAssertionInfo = assertionInfo.getSqlAssertion();
        if (sqlAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=SQL but sqlAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return sqlAssertionInfo.getEntity();
      case FIELD:
        final FieldAssertionInfo fieldAssertionInfo = assertionInfo.getFieldAssertion();
        if (fieldAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=FIELD but fieldAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return fieldAssertionInfo.getEntity();
      case DATA_SCHEMA:
        final SchemaAssertionInfo schemaAssertionInfo = assertionInfo.getSchemaAssertion();
        if (schemaAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=DATA_SCHEMA but schemaAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return schemaAssertionInfo.getEntity();
      case CUSTOM:
        final CustomAssertionInfo customAssertionInfo = assertionInfo.getCustomAssertion();
        if (customAssertionInfo == null) {
          log.warn(
              "AssertionInfo type=CUSTOM but customAssertion is null; cannot derive entityUrn. assertionInfo={}",
              assertionInfo);
          return null;
        }
        return customAssertionInfo.getEntity();
      default:
        log.warn(
            "Unsupported AssertionInfo type {}; cannot derive entityUrn. assertionInfo={}",
            assertionInfo.getType(),
            assertionInfo);
        return null;
    }
  }
}
