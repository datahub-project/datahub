package com.linkedin.datahub.upgrade.system.assertions;

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
 * A {@link NonBlockingSystemUpgrade} upgrade job that populates the fieldPath field on all
 * FieldAssertionInfo aspects.
 */
@Slf4j
public class GenerateAssertionFieldPath implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public GenerateAssertionFieldPath(
      @Nonnull OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      boolean enabled,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    if (enabled) {
      _steps =
          ImmutableList.of(
              new GenerateAssertionFieldPathStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit));
    } else {
      log.info(
          "{} is disabled (systemUpdate.assertionFieldPath.enabled=false); no steps registered.",
          id());
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
