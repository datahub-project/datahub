package com.linkedin.datahub.upgrade.system.lineage;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;

/**
 * Non-blocking system upgrade that backfills dataset lineage index fields.
 *
 * <p>This upgrade ensures that all datasets have the proper lineage-related fields in their search
 * index for efficient querying and filtering: - hasUpstreams: indicates if the dataset has any
 * upstream lineage - hasFineGrainedUpstreams: indicates if the dataset has fine-grained
 * (column-level) lineage - fineGrainedUpstreams: list of schema field URNs that provide lineage to
 * this dataset
 */
public class BackfillDatasetLineageIndexFields implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> _steps;

  public BackfillDatasetLineageIndexFields(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      boolean enabled,
      boolean reprocessEnabled,
      Integer batchSize) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new BackfillDatasetLineageIndexFieldsStep(
                  opContext, entityService, searchService, reprocessEnabled, batchSize));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return "BackfillDatasetLineageIndexFields";
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
