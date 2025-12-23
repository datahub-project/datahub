package com.linkedin.metadata.aspect.consistency.check.monitor;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.MonitorUrnUtils;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Check that the entity referenced by a monitor exists.
 *
 * <p>If the entity has been hard-deleted, the monitor is orphaned and should also be hard-deleted.
 */
@Component("consistencyMonitorEntityNotFoundCheck")
public class MonitorEntityNotFoundCheck extends AbstractMonitorCheck {

  public MonitorEntityNotFoundCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Entity Not Found";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the entity referenced by a monitor exists";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkMonitor(
      @Nonnull CheckContext ctx, @Nonnull Urn monitorUrn, @Nonnull EntityResponse response) {

    Urn entityUrn = MonitorUrnUtils.getEntityUrn(monitorUrn);
    if (entityUrn == null) {
      return Collections.emptyList();
    }

    // Skip if invalid entity type (handled by MonitorEntityTypeInvalidCheck)
    if (!isValidMonitorEntityType(entityUrn.getEntityType())) {
      return Collections.emptyList();
    }

    // Check if entity exists (including soft-deleted)
    if (!referencedEntityExists(ctx, entityUrn)) {
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.HARD_DELETE)
              .description(String.format("Monitor references non-existent entity: %s", entityUrn))
              .relatedUrns(List.of(entityUrn))
              .hardDeleteUrns(List.of(monitorUrn))
              .build());
    }

    return Collections.emptyList();
  }
}
