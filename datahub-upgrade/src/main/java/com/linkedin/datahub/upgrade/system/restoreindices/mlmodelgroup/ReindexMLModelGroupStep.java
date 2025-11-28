package com.linkedin.datahub.upgrade.system.restoreindices.mlmodelgroup;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class ReindexMLModelGroupStep extends AbstractMCLStep {

  public ReindexMLModelGroupStep(
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
    return "mlmodelgroup-key-v1";
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return ML_MODEL_GROUP_KEY_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + ML_MODEL_GROUP_ENTITY_NAME + ":%";
  }
}
