package com.linkedin.datahub.upgrade.system.restoreindices.columnlineage;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class ReindexDatasetUpstreamLineageStep extends AbstractMCLStep {

  public ReindexDatasetUpstreamLineageStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public String id() {
    return "dataset-upstream-lineage-v1";
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return UPSTREAM_LINEAGE_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + DATASET_ENTITY_NAME + ":%";
  }

  @Override
  protected String getLegacyId() {
    return "restore-column-lineage-indices";
  }

  @Override
  protected String getLegacyVersion() {
    return "1";
  }
}
