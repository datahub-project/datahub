package com.linkedin.datahub.upgrade.system.ingestion;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking system upgrade that ingests EntityTypeInfo aspects for all entity types in the
 * entity registry. Replaces the former GMS bootstrap step so ingestion runs only during
 * system-update.
 */
@Slf4j
public class IngestEntityTypes implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public IngestEntityTypes(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      final boolean enabled) {
    _steps = ImmutableList.of(new IngestEntityTypesStep(entityService, enabled));
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return _steps;
  }
}
