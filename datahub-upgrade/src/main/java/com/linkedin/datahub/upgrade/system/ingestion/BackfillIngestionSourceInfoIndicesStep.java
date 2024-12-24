package com.linkedin.datahub.upgrade.system.ingestion;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackfillIngestionSourceInfoIndicesStep extends AbstractMCLStep {

  private static final String UPGRADE_ID = BackfillIngestionSourceInfoIndices.class.getSimpleName();
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);

  public BackfillIngestionSourceInfoIndicesStep(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return INGESTION_INFO_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + INGESTION_SOURCE_ENTITY_NAME + ":%";
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return true;
  }
}
