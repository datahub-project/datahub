package com.linkedin.datahub.upgrade.system;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.PersistentUpgradeStep;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Abstract base class for PersistentUpgradeStep implementations that provides common functionality
 * for managing operation context, entity service, and upgrade URN generation.
 *
 * <p>This class eliminates boilerplate code that would otherwise be duplicated across multiple
 * upgrade step implementations.
 */
public abstract class AbstractPersistentUpgradeStep implements PersistentUpgradeStep {

  private final OperationContext opContext;
  private final EntityService<?> entityService;

  protected AbstractPersistentUpgradeStep(
      OperationContext opContext, EntityService<?> entityService) {
    this.opContext = opContext;
    this.entityService = entityService;
  }

  @Nonnull
  @Override
  public Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Nonnull
  @Override
  public EntityService<?> getEntityService() {
    return entityService;
  }

  @Nonnull
  @Override
  public OperationContext getSystemOpContext() {
    return opContext;
  }

  @Override
  public boolean isReprocessEnabled() {
    return false;
  }
}
