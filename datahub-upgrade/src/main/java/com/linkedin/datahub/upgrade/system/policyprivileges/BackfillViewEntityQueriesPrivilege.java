package com.linkedin.datahub.upgrade.system.policyprivileges;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

/**
 * Grants the {@code VIEW_ENTITY_QUERIES} privilege to existing policies that grant {@code
 * VIEW_ENTITY_PAGE}, preserving pre-upgrade behavior: before the privilege existed, being able to
 * view an entity's page implied being able to view its queries. Without this backfill, upgrading an
 * existing install would silently revoke query visibility from every user whose policies predate
 * the privilege.
 */
public class BackfillViewEntityQueriesPrivilege implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> _steps;

  public BackfillViewEntityQueriesPrivilege(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillViewEntityQueriesPrivilegeStep(
                  opContext, entityService, searchService, reprocessEnabled, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillViewEntityQueriesPrivilege";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
