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
 * Check that monitors reference a valid entity type.
 *
 * <p>Valid entity types are derived from @UrnValidation annotations in the monitorKey PDL. If the
 * entity type is invalid, the monitor should be soft-deleted.
 */
@Component("consistencyMonitorEntityTypeInvalidCheck")
public class MonitorEntityTypeInvalidCheck extends AbstractMonitorCheck {

  public MonitorEntityTypeInvalidCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Monitor Entity Type Invalid";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that monitors reference a valid entity type (per PDL annotations)";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkMonitor(
      @Nonnull CheckContext ctx, @Nonnull Urn monitorUrn, @Nonnull EntityResponse response) {

    Urn entityUrn = MonitorUrnUtils.getEntityUrn(monitorUrn);
    if (entityUrn == null) {
      return Collections.emptyList();
    }

    if (!isValidMonitorEntityType(entityUrn.getEntityType())) {
      return List.of(
          createIssueBuilder(monitorUrn, ConsistencyFixType.SOFT_DELETE)
              .description(
                  String.format(
                      "Monitor has invalid entity type '%s' (expected one of: %s)",
                      entityUrn.getEntityType(), String.join(", ", getValidMonitorEntityTypes())))
              .relatedUrns(List.of(entityUrn))
              .batchItems(List.of(createSoftDeleteItem(ctx, monitorUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }
}
