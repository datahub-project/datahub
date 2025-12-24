package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.MONITOR_INFO_ASPECT_NAME;
import static com.linkedin.metadata.aspect.utils.AssertionUtils.ASSERTION_TYPE_SUB_PROPERTY_CHECKS;
import static com.linkedin.metadata.aspect.utils.AssertionUtils.getExpectedPropertyName;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionType;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import com.linkedin.metadata.aspect.utils.AssertionUtils;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorType;
import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Validator for Monitor and Assertion entities that enforces business logic constraints not covered
 * by PDL annotations.
 *
 * <p>For AssertionInfo:
 *
 * <ul>
 *   <li>Entity reference must be resolvable - either entityUrn must be present, OR the
 *       type-specific sub-property must contain an entity that the AssertionInfoMutator can copy
 *   <li>type must match sub-property - if type is set, the corresponding sub-property must be
 *       populated
 * </ul>
 *
 * <p>For MonitorInfo:
 *
 * <ul>
 *   <li>assertionMonitor must be present when type is ASSERTION
 *   <li>assertions array must have at most one element (1:1 relationship between monitor and
 *       assertion, empty allowed for intermediate states)
 * </ul>
 */
@Slf4j
@Setter
@Getter
@Accessors(chain = true)
public class MonitorAssertionValidator extends AspectPayloadValidator {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {

    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();

    mcpItems.forEach(
        item -> {
          if (ASSERTION_INFO_ASPECT_NAME.equals(item.getAspectName())) {
            validateAssertionInfo(item, exceptions);
          } else if (MONITOR_INFO_ASPECT_NAME.equals(item.getAspectName())) {
            validateMonitorInfo(item, exceptions);
          }
        });

    return exceptions.streamAllExceptions();
  }

  /**
   * Validate AssertionInfo aspect.
   *
   * @param item the batch item containing the aspect
   * @param exceptions collection to add validation exceptions to
   */
  private void validateAssertionInfo(BatchItem item, ValidationExceptionCollection exceptions) {
    AssertionInfo assertionInfo = item.getAspect(AssertionInfo.class);
    if (assertionInfo == null) {
      return;
    }

    // Validate entity reference is resolvable (either entityUrn or derivable from sub-property)
    if (!canResolveEntityUrn(assertionInfo)) {
      exceptions.addException(
          AspectValidationException.forItem(
              item,
              "AssertionInfo must have entityUrn defined or an entity in the type-specific "
                  + "sub-property. Assertions without a resolvable entity reference are non-functional."));
    }

    // Validate type matches sub-property
    if (assertionInfo.hasType()) {
      AssertionType type = assertionInfo.getType();
      Function<AssertionInfo, Boolean> checker = ASSERTION_TYPE_SUB_PROPERTY_CHECKS.get(type);

      if (checker != null && !checker.apply(assertionInfo)) {
        String expectedProperty = getExpectedPropertyName(type);
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "AssertionInfo has type '%s' but %s is null or empty. "
                        + "The corresponding sub-property must be populated for the assertion type.",
                    type, expectedProperty)));
      }
    }
  }

  /**
   * Check if the entity URN can be resolved for an assertion.
   *
   * <p>The entity URN can be resolved if:
   *
   * <ul>
   *   <li>entityUrn is set directly, OR
   *   <li>The type-specific sub-property contains an entity that the AssertionInfoMutator can
   *       extract
   * </ul>
   *
   * @param assertionInfo the assertion info to check
   * @return true if entity URN can be resolved
   */
  private boolean canResolveEntityUrn(AssertionInfo assertionInfo) {
    // entityUrn already set
    if (assertionInfo.hasEntityUrn() && assertionInfo.getEntityUrn() != null) {
      return true;
    }
    // Or can be derived from type-specific sub-property
    return AssertionUtils.getEntityFromAssertionInfo(assertionInfo) != null;
  }

  /**
   * Validate MonitorInfo aspect.
   *
   * @param item the batch item containing the aspect
   * @param exceptions collection to add validation exceptions to
   */
  private void validateMonitorInfo(BatchItem item, ValidationExceptionCollection exceptions) {
    MonitorInfo monitorInfo = item.getAspect(MonitorInfo.class);
    if (monitorInfo == null) {
      return;
    }

    // Validate assertionMonitor is present when type is ASSERTION
    if (monitorInfo.hasType() && MonitorType.ASSERTION.equals(monitorInfo.getType())) {
      if (!monitorInfo.hasAssertionMonitor() || monitorInfo.getAssertionMonitor() == null) {
        exceptions.addException(
            AspectValidationException.forItem(
                item, "MonitorInfo with type ASSERTION must have assertionMonitor defined."));
        return;
      }

      // Validate assertions array has at most one element (0 or 1 allowed for intermediate states)
      if (monitorInfo.getAssertionMonitor().hasAssertions()
          && monitorInfo.getAssertionMonitor().getAssertions() != null
          && monitorInfo.getAssertionMonitor().getAssertions().size() > 1) {
        exceptions.addException(
            AspectValidationException.forItem(
                item,
                String.format(
                    "MonitorInfo assertionMonitor must have at most one assertion (1:1 relationship). "
                        + "Found %d assertions.",
                    monitorInfo.getAssertionMonitor().getAssertions().size())));
      }
    }
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull Collection<ChangeMCP> changeMCPs, @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }
}
