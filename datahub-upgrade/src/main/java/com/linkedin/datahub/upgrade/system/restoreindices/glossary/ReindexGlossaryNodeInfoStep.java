package com.linkedin.datahub.upgrade.system.restoreindices.glossary;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class ReindexGlossaryNodeInfoStep extends AbstractMCLStep {

  public ReindexGlossaryNodeInfoStep(
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
    return "glossary-node-info-v1";
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return GLOSSARY_NODE_INFO_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + GLOSSARY_NODE_ENTITY_NAME + ":%";
  }
}
