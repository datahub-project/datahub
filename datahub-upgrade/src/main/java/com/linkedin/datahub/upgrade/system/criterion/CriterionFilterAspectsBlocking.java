package com.linkedin.datahub.upgrade.system.criterion;

import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.BlockingSystemUpgrade;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Legacy blocking system update that rewrites stored {@code dataHubViewInfo} and {@code
 * dynamicFormAssignment} payloads from schema v1 to v2 by normalizing legacy {@code
 * Criterion.value} into {@code values} (see {@link
 * com.linkedin.metadata.aspect.hooks.migrations.criterion.CriterionFilterMutatorBase}).
 *
 * <p>This is <b>not</b> the ZDU aspect migration ({@link
 * com.linkedin.datahub.upgrade.system.migrations.MigrateAspects}); use that path when {@code
 * featureFlags.aspectMigrationMutatorEnabled} is on. Scanning and resume follow the same {@link
 * com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs} / {@code streamAspectBatches}
 * pattern as {@link
 * com.linkedin.datahub.upgrade.system.schemafield.GenerateSchemaFieldsFromSchemaMetadataStep}, not
 * {@code streamAspectBatchesForMigration}. The step uses {@link
 * com.linkedin.metadata.entity.EntityService#ingestAspects} so corrected aspects are written to
 * storage and MCLs are emitted (same as that example), not only {@code alwaysProduceMCLAsync}.
 * Remove this job when it is no longer needed.
 */
@Slf4j
public class CriterionFilterAspectsBlocking implements BlockingSystemUpgrade {

  private static final List<String> ASPECT_NAMES =
      List.of(DATAHUB_VIEW_INFO_ASPECT_NAME, DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME);

  private final List<UpgradeStep> steps;

  public CriterionFilterAspectsBlocking(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull AspectDao aspectDao,
      @Nonnull String upgradeVersion,
      boolean enabled,
      int batchSize,
      int batchDelayMs,
      int limit) {

    if (!enabled) {
      log.info(
          "CriterionFilterAspectsBlocking: disabled (systemUpdate.criterionFilterAspectsBlocking.enabled=false).");
      steps = ImmutableList.of();
      return;
    }

    steps =
        ImmutableList.of(
            new CriterionFilterAspectsBlockingStep(
                opContext,
                entityService,
                aspectDao,
                upgradeVersion,
                batchSize,
                batchDelayMs,
                limit));

    log.info(
        "CriterionFilterAspectsBlocking initialised: upgradeVersion={}, aspectNames={}.",
        upgradeVersion,
        ASPECT_NAMES);
  }

  @Override
  public String id() {
    return getClass().getName();
  }

  @Override
  public List<UpgradeStep> steps() {
    return steps;
  }
}
