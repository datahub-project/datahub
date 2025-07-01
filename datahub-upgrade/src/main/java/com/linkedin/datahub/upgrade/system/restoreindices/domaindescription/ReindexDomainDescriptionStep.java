package com.linkedin.datahub.upgrade.system.restoreindices.domaindescription;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.datahub.upgrade.system.AbstractMCLStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class ReindexDomainDescriptionStep extends AbstractMCLStep {

  public ReindexDomainDescriptionStep(
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
    return "domain-description-v1";
  }

  @Nonnull
  @Override
  protected String getAspectName() {
    return DOMAIN_PROPERTIES_ASPECT_NAME;
  }

  @Nullable
  @Override
  protected String getUrnLike() {
    return "urn:li:" + DOMAIN_ENTITY_NAME + ":%";
  }
}
