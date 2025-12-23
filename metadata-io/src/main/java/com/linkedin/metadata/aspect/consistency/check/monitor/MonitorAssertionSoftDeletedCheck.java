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
 * Check that monitors referencing soft-deleted assertions are also soft-deleted.
 *
 * <p>If an assertion is soft-deleted, the monitor that references it should also be soft-deleted to
 * maintain consistency.
 */
@Component("consistencyMonitorAssertionSoftDeletedCheck")
public class MonitorAssertionSoftDeletedCheck extends AbstractMonitorCheck {

  public MonitorAssertionSoftDeletedCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Assertion Soft-Deleted";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that monitors referencing soft-deleted assertions are also soft-deleted";
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

    // Check if assertion is soft-deleted by querying directly
    if (isReferencedEntitySoftDeleted(ctx, assertionUrn)) {
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.SOFT_DELETE)
              .description(
                  String.format("Monitor references soft-deleted assertion: %s", assertionUrn))
              .relatedUrns(List.of(assertionUrn))
              .batchItems(List.of(createSoftDeleteItem(ctx, monitorUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }
}
