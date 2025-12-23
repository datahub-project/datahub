package com.linkedin.metadata.aspect.consistency.check.monitor;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Check that monitors have exactly one assertion (1:1 relationship).
 *
 * <p>Monitors should have a 1:1 relationship with assertions. If a monitor has multiple assertions,
 * the extra assertions should be removed.
 */
@Component("consistencyMonitorAssertionsMultipleCheck")
public class MonitorAssertionsMultipleCheck extends AbstractMonitorCheck {

  public MonitorAssertionsMultipleCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Multiple Assertions";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that monitors have exactly one assertion (1:1 relationship)";
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

    // Skip if no assertions or only one (valid)
    if (!assertionMonitor.hasAssertions() || assertionMonitor.getAssertions().size() <= 1) {
      return Collections.emptyList();
    }

    List<AssertionEvaluationSpec> assertions = assertionMonitor.getAssertions();
    List<String> extraAssertionUrns =
        assertions.subList(1, assertions.size()).stream()
            .filter(spec -> spec.hasAssertion() && spec.getAssertion() != null)
            .map(spec -> spec.getAssertion().toString())
            .collect(Collectors.toList());

    // Create a fixed MonitorInfo with only the first assertion
    MonitorInfo fixedMonitorInfo = createFixedMonitorInfo(monitorInfo, assertions.get(0));

    return List.of(
        createIssueBuilder(monitorUrn, ConsistencyFixType.UPSERT)
            .description(
                String.format(
                    "Monitor has %d assertions (expected 1). Extra assertions: %s",
                    assertions.size(), extraAssertionUrns))
            .details(String.join(",", extraAssertionUrns))
            .batchItems(
                List.of(
                    createUpsertItem(
                        ctx,
                        monitorUrn,
                        MONITOR_INFO_ASPECT_NAME,
                        fixedMonitorInfo,
                        getAspect(response, MONITOR_INFO_ASPECT_NAME))))
            .build());
  }

  /**
   * Create a fixed MonitorInfo with only the primary assertion.
   *
   * @param originalInfo the original MonitorInfo
   * @param primaryAssertion the primary assertion to keep
   * @return a new MonitorInfo with only the primary assertion
   */
  private MonitorInfo createFixedMonitorInfo(
      MonitorInfo originalInfo, AssertionEvaluationSpec primaryAssertion) {
    // Create a deep copy using utility method
    MonitorInfo fixedInfo = GenericRecordUtils.copy(originalInfo, MonitorInfo.class);

    // Update the AssertionMonitor to have only the primary assertion
    AssertionMonitor fixedAssertionMonitor =
        GenericRecordUtils.copy(fixedInfo.getAssertionMonitor(), AssertionMonitor.class);
    fixedAssertionMonitor.setAssertions(
        new AssertionEvaluationSpecArray(Collections.singletonList(primaryAssertion)));
    fixedInfo.setAssertionMonitor(fixedAssertionMonitor);

    return fixedInfo;
  }
}
