package com.linkedin.datahub.upgrade.system.schemafield;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Migrate from URN document ids to hash based ids */
@Slf4j
public class MigrateSchemaFieldDocIds implements NonBlockingSystemUpgrade {
  private final List<UpgradeStep> _steps;

  public MigrateSchemaFieldDocIds(
      @Nonnull OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents elasticSearchComponents,
      EntityService<?> entityService,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new MigrateSchemaFieldDocIdsStep(
                  opContext,
                  elasticSearchComponents,
                  entityService,
                  batchSize,
                  batchDelayMs,
                  limit));
    } else {
      _steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return this.getClass().getName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
