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
 * A {@link NonBlockingSystemUpgrade} that migrates assertion notes from the deprecated {@code note}
 * field embedded in {@code assertionInfo} into the dedicated {@code assertionNote} aspect.
 *
 * <p>Prior to this migration, notes were stored inside {@code assertionInfo}, meaning any UPSERT
 * from an ingestion source (e.g. dbt, Great Expectations) could silently overwrite the
 * user-authored note. The dedicated {@code assertionNote} aspect is never written by ingestion
 * sources.
 *
 * <p>The step is idempotent: it only writes {@code assertionNote} for assertions that have a note
 * in {@code assertionInfo} but no existing {@code assertionNote} aspect.
 */
@Slf4j
public class MigrateAssertionNoteToAspect implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public MigrateAssertionNoteToAspect(
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
              new MigrateAssertionNoteToAspectStep(
                  opContext, entityService, aspectDao, batchSize, batchDelayMs, limit));
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
