package com.linkedin.datahub.upgrade.system.aliases;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * A {@link NonBlockingSystemUpgrade} that backfills the system-owned {@code aliases} aspect for
 * datasets created before {@code AliasesSideEffect} shipped. See {@link BackfillDatasetAliasesStep}
 * for the coverage and completion-marker semantics.
 */
public class BackfillDatasetAliases implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public BackfillDatasetAliases(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      boolean reprocessEnabled) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillDatasetAliasesStep(
                  opContext,
                  entityService,
                  searchService,
                  batchSize,
                  batchDelayMs,
                  reprocessEnabled));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillDatasetAliases";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
