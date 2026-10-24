package com.linkedin.datahub.upgrade.system.ingestion;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

public class BackfillIngestionSourceRunSummary implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public BackfillIngestionSourceRunSummary(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      EntitySearchService entitySearchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    steps =
        enabled
            ? ImmutableList.of(
                new BackfillIngestionSourceRunSummaryStep(
                    opContext,
                    entityService,
                    searchService,
                    entitySearchService,
                    reprocessEnabled,
                    batchSize))
            : ImmutableList.of();
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
