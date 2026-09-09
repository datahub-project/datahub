package com.linkedin.datahub.upgrade.system.schemafield;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking system upgrade that re-ingests dataset schemaMetadata/status to materialize
 * schemaField entities via {@code SchemaFieldSideEffect}, including optional domain/ownership
 * backfill or cleanup based on current MCP mirror flags.
 */
@Slf4j
public class GenerateSchemaFieldsFromSchemaMetadata implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public GenerateSchemaFieldsFromSchemaMetadata(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit,
      boolean reprocessEnabled,
      boolean domainEnabled,
      boolean ownershipEnabled) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new GenerateSchemaFieldsFromSchemaMetadataStep(
                  opContext,
                  entityService,
                  aspectDao,
                  batchSize,
                  batchDelayMs,
                  limit,
                  reprocessEnabled,
                  domainEnabled,
                  ownershipEnabled));
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
