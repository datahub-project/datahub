package com.linkedin.datahub.upgrade.system.migrations;

import static com.linkedin.metadata.Constants.DEFAULT_SCHEMA_VERSION;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.NonBlockingSystemUpgrade;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutatorChain;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * A {@link NonBlockingSystemUpgrade} that migrates all aspects registered with the {@link
 * AspectMigrationMutatorChain}.
 *
 * <p>Aspects are processed ordered by {@code createdon ASC} across all aspect types in a single
 * sweep. On {@code version=0} rows {@code createdon} is the last-write timestamp (updated on every
 * upsert), so least-recently-written aspects are migrated first.
 *
 * <p>The job is gated on the {@code systemUpdate.migrateAspects.enabled} flag. When the flag is
 * off, or the chain has no registered mutators, {@link #steps()} returns an empty list.
 */
@Slf4j
public class MigrateAspects implements NonBlockingSystemUpgrade {

  private final List<UpgradeStep> _steps;

  public MigrateAspects(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull AspectDao aspectDao,
      @Nonnull AspectMigrationMutatorChain chain,
      @Nonnull String upgradeVersion,
      boolean enabled,
      int batchSize,
      int batchDelayMs,
      int limit) {

    if (!enabled) {
      log.info("MigrateAspects: migrateAspects is disabled — skipping all migration steps.");
      _steps = ImmutableList.of();
      return;
    }

    Map<String, List<AspectMigrationMutator>> chainByAspect = chain.getChainByAspect();
    if (chainByAspect.isEmpty()) {
      log.info("MigrateAspects: no mutators registered — nothing to migrate.");
      _steps = ImmutableList.of();
      return;
    }

    // Build aspectName → maxTargetVersion from the mutator chain.
    Map<String, Long> aspectTargetVersions =
        chainByAspect.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e ->
                        e.getValue().stream()
                            .mapToLong(AspectMigrationMutator::getTargetVersion)
                            .max()
                            .orElse(DEFAULT_SCHEMA_VERSION)));

    _steps =
        ImmutableList.of(
            new MigrateAspectsStep(
                opContext,
                entityService,
                aspectDao,
                aspectTargetVersions,
                upgradeVersion,
                batchSize,
                batchDelayMs,
                limit));

    log.info(
        "MigrateAspects initialised: {} aspect(s), upgradeVersion={}, aspects={}.",
        aspectTargetVersions.size(),
        upgradeVersion,
        aspectTargetVersions);
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
