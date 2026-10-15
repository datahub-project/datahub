package com.linkedin.datahub.upgrade.system.policyprivileges;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

/**
 * Grants the {@code VIEW_ALL_QUERIES} privilege to existing policies that grant {@code
 * VIEW_ENTITY_PAGE}, preserving pre-upgrade behavior: before query-view authorization existed,
 * being able to view an entity's page implied being able to view ALL of its queries, including ones
 * with no recorded subject dataset. {@code VIEW_ALL_QUERIES} is a distinct, platform-level
 * privilege from the dataset-scoped {@code VIEW_ENTITY_QUERIES} — see {@link
 * BackfillViewAllQueriesPrivilegeStep} for the full rationale, including why administrators may
 * want to narrow this grant after upgrading.
 */
public class BackfillViewAllQueriesPrivilege implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> _steps;

  public BackfillViewAllQueriesPrivilege(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillViewAllQueriesPrivilegeStep(
                  opContext, entityService, searchService, reprocessEnabled, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillViewAllQueriesPrivilege";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
