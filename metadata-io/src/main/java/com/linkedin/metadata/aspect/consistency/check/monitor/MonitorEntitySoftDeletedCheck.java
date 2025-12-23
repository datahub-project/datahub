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
 * Check that the entity referenced by a monitor is not soft-deleted.
 *
 * <p>If the entity is soft-deleted, the monitor should also be soft-deleted to maintain
 * consistency.
 */
@Component("consistencyMonitorEntitySoftDeletedCheck")
public class MonitorEntitySoftDeletedCheck extends AbstractMonitorCheck {

  public MonitorEntitySoftDeletedCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Entity Soft-Deleted";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the entity referenced by a monitor is not soft-deleted";
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

    // Skip if entity doesn't exist at all (handled by MonitorEntityNotFoundCheck)
    if (!referencedEntityExists(ctx, entityUrn)) {
      return Collections.emptyList();
    }

    // Check if entity is soft-deleted
    if (isReferencedEntitySoftDeleted(ctx, entityUrn)) {
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.SOFT_DELETE)
              .description(String.format("Monitor references soft-deleted entity: %s", entityUrn))
              .relatedUrns(List.of(entityUrn))
              .batchItems(List.of(createSoftDeleteItem(ctx, monitorUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }
}
