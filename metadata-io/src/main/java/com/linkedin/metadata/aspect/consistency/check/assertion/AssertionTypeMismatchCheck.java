package com.linkedin.metadata.aspect.consistency.check.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Check that assertions have the correct sub-property populated based on their type.
 *
 * <p>Each assertion type requires a corresponding sub-property to be populated:
 *
 * <ul>
 *   <li>DATASET -&gt; datasetAssertion
 *   <li>FRESHNESS -&gt; freshnessAssertion
 *   <li>VOLUME -&gt; volumeAssertion
 *   <li>SQL -&gt; sqlAssertion
 *   <li>FIELD -&gt; fieldAssertion
 *   <li>DATA_SCHEMA -&gt; schemaAssertion
 *   <li>CUSTOM -&gt; customAssertion
 * </ul>
 *
 * <p>If the type is set but the corresponding sub-property is null or empty, this indicates data
 * corruption and the assertion should be soft-deleted.
 */
@Slf4j
@Component("consistencyAssertionTypeMismatchCheck")
public class AssertionTypeMismatchCheck extends AbstractAssertionCheck {

  public AssertionTypeMismatchCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  /** Type to sub-property validation mapping */
  private static final Map<AssertionType, Function<AssertionInfo, Boolean>> TYPE_CHECKS =
      Map.of(
          AssertionType.DATASET,
              info -> isValidSubProperty(info.hasDatasetAssertion(), info::getDatasetAssertion),
          AssertionType.FRESHNESS,
              info -> isValidSubProperty(info.hasFreshnessAssertion(), info::getFreshnessAssertion),
          AssertionType.VOLUME,
              info -> isValidSubProperty(info.hasVolumeAssertion(), info::getVolumeAssertion),
          AssertionType.SQL,
              info -> isValidSubProperty(info.hasSqlAssertion(), info::getSqlAssertion),
          AssertionType.FIELD,
              info -> isValidSubProperty(info.hasFieldAssertion(), info::getFieldAssertion),
          AssertionType.DATA_SCHEMA,
              info -> isValidSubProperty(info.hasSchemaAssertion(), info::getSchemaAssertion),
          AssertionType.CUSTOM,
              info -> isValidSubProperty(info.hasCustomAssertion(), info::getCustomAssertion));

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Type Mismatch";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that assertions have the correct sub-property populated based on their type";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkAssertion(
      @Nonnull CheckContext ctx,
      @Nonnull Urn assertionUrn,
      @Nonnull EntityResponse response,
      @Nonnull AssertionInfo assertionInfo) {

    // Skip if no type is set
    if (!assertionInfo.hasType()) {
      return Collections.emptyList();
    }

    AssertionType type = assertionInfo.getType();
    Function<AssertionInfo, Boolean> checker = TYPE_CHECKS.get(type);

    // Skip if we don't have a checker for this type (shouldn't happen with known types)
    if (checker == null) {
      log.warn(
          "No type checker configured for assertion type {} on assertion {}", type, assertionUrn);
      return Collections.emptyList();
    }

    // Check if the corresponding sub-property is valid (present and non-empty)
    if (!checker.apply(assertionInfo)) {
      String expectedProperty = getExpectedPropertyName(type);
      return List.of(
          createIssueBuilder(assertionUrn, ConsistencyFixType.SOFT_DELETE)
              .description(
                  String.format(
                      "Assertion has type '%s' but %s is null or empty", type, expectedProperty))
              .batchItems(List.of(createSoftDeleteItem(ctx, assertionUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }

  /**
   * Check if a sub-property is valid (present and non-empty).
   *
   * @param hasProperty whether the property is set
   * @param getter supplier to get the property value
   * @return true if the property is valid
   */
  private static boolean isValidSubProperty(
      boolean hasProperty, Supplier<? extends RecordTemplate> getter) {
    if (!hasProperty) {
      return false;
    }
    RecordTemplate value = getter.get();
    if (value == null) {
      return false;
    }
    // Check if the record has any fields populated (not empty)
    return !value.data().isEmpty();
  }

  /**
   * Get the expected property name for a given assertion type.
   *
   * @param type the assertion type
   * @return the expected property name
   */
  private String getExpectedPropertyName(AssertionType type) {
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
}
