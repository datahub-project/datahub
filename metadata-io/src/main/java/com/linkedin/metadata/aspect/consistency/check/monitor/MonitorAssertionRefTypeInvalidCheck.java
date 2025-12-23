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
 * Check that the primary assertion reference in a monitor has a valid entity type.
 *
 * <p>The assertion reference in AssertionMonitor.assertions[0].assertion must be of entity type
 * "assertion". If it references any other entity type, this is data corruption and the monitor
 * should be soft-deleted.
 */
@Component("consistencyMonitorAssertionRefTypeInvalidCheck")
public class MonitorAssertionRefTypeInvalidCheck extends AbstractMonitorCheck {

  public MonitorAssertionRefTypeInvalidCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Assertion Reference Type Invalid";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the primary assertion reference in a monitor is of entity type 'assertion'";
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

    // Check if the assertion URN has invalid entity type
    if (!ASSERTION_ENTITY_NAME.equals(assertionUrn.getEntityType())) {
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.SOFT_DELETE)
              .description(
                  String.format(
                      "Monitor references non-assertion entity type '%s' (expected assertion)",
                      assertionUrn.getEntityType()))
              .relatedUrns(List.of(assertionUrn))
              .batchItems(List.of(createSoftDeleteItem(ctx, monitorUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }
}
