package com.linkedin.metadata.aspect.consistency.check.monitor;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Check that the primary assertion referenced by a monitor exists.
 *
 * <p>If the assertion has been hard-deleted, the monitor is orphaned and should also be
 * hard-deleted.
 */
@Component("consistencyMonitorAssertionNotFoundCheck")
public class MonitorAssertionNotFoundCheck extends AbstractMonitorCheck {

  public MonitorAssertionNotFoundCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Assertion Not Found";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the assertion referenced by a monitor exists (not hard-deleted)";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkMonitor(
      @Nonnull CheckContext ctx, @Nonnull Urn monitorUrn, @Nonnull EntityResponse response) {

    MonitorInfo monitorInfo = getMonitorInfo(response);
    if (monitorInfo == null) {
      return Collections.emptyList();
    }

    AssertionMonitor assertionMonitor = getAssertionMonitor(monitorInfo);
    if (assertionMonitor == null) {
      return Collections.emptyList();
    }

    // Skip if no assertions (handled by MonitorAssertionsEmptyCheck)
    if (!assertionMonitor.hasAssertions() || assertionMonitor.getAssertions().isEmpty()) {
      return Collections.emptyList();
    }

    // Check primary assertion
    AssertionEvaluationSpec primarySpec = assertionMonitor.getAssertions().get(0);
    if (!primarySpec.hasAssertion() || primarySpec.getAssertion() == null) {
      return Collections.emptyList();
    }

    Urn assertionUrn = primarySpec.getAssertion();

    // Skip if invalid entity type (handled by MonitorAssertionRefTypeInvalidCheck)
    if (!ASSERTION_ENTITY_NAME.equals(assertionUrn.getEntityType())) {
      return Collections.emptyList();
    }

    // Check if assertion exists (including soft-deleted)
    if (!referencedEntityExists(ctx, assertionUrn)) {
      // Assertion has been hard-deleted - recommend hard-delete of the monitor
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.HARD_DELETE)
              .description(
                  String.format("Monitor references non-existent assertion: %s", assertionUrn))
              .relatedUrns(List.of(assertionUrn))
              .hardDeleteUrns(List.of(monitorUrn))
              .build());
    }

    // If assertion exists (including soft-deleted), skip - soft-delete case is handled by
    // MonitorAssertionSoftDeletedCheck
    return Collections.emptyList();
  }
}
