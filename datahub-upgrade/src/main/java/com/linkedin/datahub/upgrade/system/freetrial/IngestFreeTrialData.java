package com.linkedin.datahub.upgrade.system.freetrial;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * Non-blocking system upgrade that ingests sample data for free trial instances.
 *
 * <p>This upgrade runs after GMS starts, allowing the instance to come online quickly while sample
 * data is ingested in the background.
 */
@Slf4j
public class IngestFreeTrialData implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> steps;

  public IngestFreeTrialData(
      OperationContext opContext,
      EntityService<?> entityService,
      boolean enabled,
      boolean reprocessEnabled,
      int batchSize) {
    if (enabled) {
      steps =
          ImmutableList.of(
              new IngestFreeTrialDataStep(
                  opContext, entityService, enabled, reprocessEnabled, batchSize));
    } else {
      steps = ImmutableList.of();
    }
  }

  @Override
  public String id() {
    return getClass().getSimpleName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
