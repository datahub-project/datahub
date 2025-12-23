package com.linkedin.metadata.aspect.consistency.check.monitor;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Check that monitors have at least one assertion defined.
 *
 * <p>A monitor without any assertions is non-functional and should be soft-deleted.
 */
@Component("consistencyMonitorAssertionsEmptyCheck")
public class MonitorAssertionsEmptyCheck extends AbstractMonitorCheck {

  public MonitorAssertionsEmptyCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Assertions Empty";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that monitors have at least one assertion defined";
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

    if (!assertionMonitor.hasAssertions() || assertionMonitor.getAssertions().isEmpty()) {
      return List.of(
          createSoftDeleteIssue(
              monitorUrn,
              "Monitor has no assertions defined - monitor is non-functional",
              List.of(createSoftDeleteItem(ctx, monitorUrn, response))));
    }

    return Collections.emptyList();
  }
}
