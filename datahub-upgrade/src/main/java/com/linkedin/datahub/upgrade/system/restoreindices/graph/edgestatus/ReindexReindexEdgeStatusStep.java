package com.linkedin.datahub.upgrade.system.restoreindices.graph.edgestatus;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class ReindexReindexEdgeStatusStep extends AbstractMCLStep {

  public ReindexReindexEdgeStatusStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    super(opContext, entityService, aspectDao, batchSize, batchDelayMs, limit);
  }

  @Override
  public String id() {
    return "edge-status-reindex-v1";
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return STATUS_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return null;
  }

  @Override
  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    boolean envFlagRecommendsSkip = Boolean.parseBoolean(System.getenv("SKIP_REINDEX_EDGE_STATUS"));
    if (envFlagRecommendsSkip) {
      log.info("Environment variable SKIP_REINDEX_EDGE_STATUS is set to true. Skipping.");
    }
    return (super.skip(context) || envFlagRecommendsSkip);
  }
}
