package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexSetup;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.search.RefreshIntervals;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.update.MirroredWriteLog;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocking preparation of one family's move to another cluster: creates the family's indices on the
 * target from the family's own mappings (and, for usage events, the full rollover or data-stream
 * layout plus the live staging index the mirror writes to), and records the migration row the live
 * writers mark and the non-blocking copy resumes from. Never touches the serving cluster.
 */
@Slf4j
public class ClusterMigrationPrepareStep implements UpgradeStep {
  static final String POINTER_ACTIVE_UPGRADE_ID = "activeUpgradeId";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchClusterRegistry registry;
  private final IndexConvention indexConvention;
  private final MigrationFamily family;
  private final String upgradeVersion;

  public ClusterMigrationPrepareStep(
      @Nonnull final OperationContext opContext,
      @Nonnull final EntityService<?> entityService,
      @Nonnull final SearchClusterRegistry registry,
      @Nonnull final IndexConvention indexConvention,
      @Nonnull final MigrationFamily family,
      @Nonnull final String upgradeVersion) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.registry = registry;
    this.indexConvention = indexConvention;
    this.family = family;
    this.upgradeVersion = upgradeVersion;
  }

  @Override
  public String id() {
    return "ClusterMigrationPrepare_" + family.getFamily().getConfigKey();
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    return registry.migrationFor(family.getFamily()).isEmpty();
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        return prepare();
      } catch (Exception e) {
        log.error("{} failed", id(), e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Nonnull
  private UpgradeStepResult prepare() throws Exception {
    final SearchClusterRegistry.FamilyMigration migration =
        registry.migrationFor(family.getFamily()).orElseThrow();
    family.checkSupported();
    final String upgradeId =
        ClusterMigrationState.upgradeId(
            family.getFamily(), migration.getSourceCluster(), migration.getTargetCluster());
    if (!guardPointer(upgradeId)) {
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
    final ClusterMigrationRow row = ClusterMigrationRow.load(opContext, entityService, upgradeId);
    final SearchClusterRegistry.ClusterConnection target =
        registry.targetConnectionFor(family.getFamily());
    final ClusterHttp targetHttp = new ClusterHttp(opContext, target.getClient());
    final List<String> logicalNames = family.logicalNames(opContext, indexConvention);

    final boolean copyEnabled = copyEnabled(migration, upgradeId);
    if (row.status().map(ClusterMigrationState.Status::isTerminal).orElse(false)) {
      if (targetIndicesExist(targetHttp, migration, logicalNames)) {
        stampRollout(row);
        row.put(ClusterMigrationState.COPY_ENABLED, String.valueOf(copyEnabled));
        armUsageSweep(row, migration, target, targetHttp);
        row.persist(DataHubUpgradeState.SUCCEEDED);
        log.info(
            "{}: migration {} is complete and its target indices exist; nothing to do",
            id(),
            upgradeId);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      }
      log.warn(
          "{}: migration {} was complete but its target indices are gone; planning it again",
          id(),
          upgradeId);
      resetKeepingTheRollout(row);
    }
    stampRollout(row);
    row.put(ClusterMigrationState.COPY_ENABLED, String.valueOf(copyEnabled));
    if (ServingRebuildGuard.inFlight(opContext, entityService, upgradeVersion, logicalNames)) {
      log.error(
          "{}: a serving-cluster reindex of a {} index did not complete in this deploy; the copy"
              + " would read across an alias swap. Fix Phase 1 first.",
          id(),
          family.getFamily().getConfigKey());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }

    createTargetIndices(target, targetHttp, migration);
    if (copyEnabled) {
      ensureLogIndex(targetHttp, migration, target);
    } else {
      armUsageSweep(row, migration, target, targetHttp);
    }

    final long now = System.currentTimeMillis();
    row.putIfAbsent(ClusterMigrationState.FAMILY, family.getFamily().getConfigKey());
    row.putIfAbsent(ClusterMigrationState.SOURCE_CLUSTER, migration.getSourceCluster());
    row.putIfAbsent(ClusterMigrationState.TARGET_CLUSTER, migration.getTargetCluster());
    row.putIfAbsent(ClusterMigrationState.MIRROR_START_TIME, String.valueOf(now));
    row.putIfAbsent(ClusterMigrationState.REVISION_STARTED, upgradeVersion);
    if (row.status().isEmpty()) {
      row.setStatus(ClusterMigrationState.Status.PREPARED);
    }
    row.persist(DataHubUpgradeState.IN_PROGRESS);
    log.info(
        "{}: prepared migration {} ({} -> {}, copy {}, mode {}), status {}",
        id(),
        upgradeId,
        migration.getSourceCluster(),
        migration.getTargetCluster(),
        copyEnabled ? "enabled" : "disabled",
        migration.getSettings().getCopy().copyMode().getConfigValue(),
        row.status().orElse(ClusterMigrationState.Status.PREPARED));
    return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
  }

  /**
   * A deploy is a rollout. The mirror markers on the row are the start times of the pods of the
   * system update that last prepared it, and say nothing about the pods this one is about to roll
   * out, which must report before the usage sweep reads the cluster that served until now. So a
   * prepare under a new system update (version and revision) stamps the row with now: markers older
   * than the stamp no longer count, and the new pods, which start after this job ends, record
   * theirs over them within a minute. A prepare under the same system update (the dev stack's copy
   * command, a restarted job) leaves the stamp alone, since the pods have not changed. Nothing is
   * deleted, so a marker a pod records while this step works is never lost.
   */
  private void stampRollout(@Nonnull final ClusterMigrationRow row) {
    if (upgradeVersion.equals(row.get(ClusterMigrationState.PREPARED_BY))) {
      return;
    }
    row.put(ClusterMigrationState.PREPARED_BY, upgradeVersion);
    row.put(ClusterMigrationState.ROLLOUT_STARTED_AT, System.currentTimeMillis());
  }

  /**
   * Clears a row so its migration is planned again, except for the rollout bookkeeping: the mirror
   * never stopped, so the pods that reported it live are still the ones to trust, and the stamp
   * that dates their markers must stay with them. {@link #stampRollout} then decides on the usual
   * grounds whether this is a new rollout.
   */
  private static void resetKeepingTheRollout(@Nonnull final ClusterMigrationRow row) {
    final String preparedBy = row.get(ClusterMigrationState.PREPARED_BY);
    final String rolloutStartedAt = row.get(ClusterMigrationState.ROLLOUT_STARTED_AT);
    row.reset();
    if (preparedBy != null) {
      row.put(ClusterMigrationState.PREPARED_BY, preparedBy);
    }
    if (rolloutStartedAt != null) {
      row.put(ClusterMigrationState.ROLLOUT_STARTED_AT, rolloutStartedAt);
    }
  }

  /**
   * Whether this migration copies the family's history: the configuration's copy setting, unless
   * the reverse migration has verified. After the flip the cluster that serves now holds everything
   * the other had and mirrors back to it, so there is nothing to copy, and a copy left on would
   * only scroll the whole family and refuse a rollback while it runs.
   */
  private boolean copyEnabled(
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final String upgradeId) {
    if (!migration.getSettings().isCopyEnabled()) {
      return false;
    }
    final String reverseId =
        ClusterMigrationState.upgradeId(
            family.getFamily(), migration.getTargetCluster(), migration.getSourceCluster());
    final ClusterMigrationRow reverse =
        ClusterMigrationRow.load(opContext, entityService, reverseId);
    if (reverse.status().map(ClusterMigrationState.Status::isTerminal).orElse(false)) {
      log.info(
          "{}: migration {} is verified, so {} has nothing to copy; mirroring only (copy.enabled"
              + " set aside)",
          id(),
          reverseId,
          upgradeId);
      return false;
    }
    return true;
  }

  /**
   * The usage mirror writes into the target's live staging index. When no copy will record how to
   * finish that index later (a mirror-only migration, or a completed one mirroring again after a
   * rollback), note it here: unless a sweep is already pending, record the serving cluster's
   * current write index as where the next sweep starts.
   */
  private void armUsageSweep(
      @Nonnull final ClusterMigrationRow row,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final SearchClusterRegistry.ClusterConnection target,
      @Nonnull final ClusterHttp targetHttp)
      throws Exception {
    if (family.getFamily() != SearchComponent.USAGE) {
      return;
    }
    final String logical =
        indexConvention.getIndexName(
            opContext, SearchComponent.USAGE, Constants.DATAHUB_USAGE_EVENT_INDEX);
    final String liveStaging = migration.getNames().target(opContext, logical);
    if (!targetHttp.indexExists(liveStaging)) {
      createUsageLayout(target, migration);
    }
    if ("pending".equals(row.get(ClusterMigrationState.USAGE_POST_FLIP_SWEEP))) {
      return;
    }
    final ClusterHttp servingHttp =
        new ClusterHttp(opContext, registry.connection(migration.getSourceCluster()).getClient());
    final SourceIndexSet serving =
        IndexLayoutResolver.resolve(servingHttp, SearchComponent.USAGE, logical);
    if (serving == null) {
      log.warn(
          "{}: {} does not exist on the serving cluster; nothing to sweep later", id(), logical);
      return;
    }
    row.put(ClusterMigrationState.USAGE_LIVE_STAGING_INDEX, liveStaging);
    row.put(ClusterMigrationState.USAGE_LAST_SOURCE_WRITE_INDEX, serving.writeIndex());
    row.put(ClusterMigrationState.USAGE_POST_FLIP_SWEEP, "pending");
  }

  /**
   * One family has one active migration at a time, and the pointer row names it. Pointing the
   * family at another cluster while that migration's copy is part way through is refused instead of
   * silently starting a second one. A migration that only prepared, the reverse mirror after a flip
   * among them, has nothing in flight: the pointer moves and the row it now names resumes, which is
   * how a rollback returns the family to the old cluster.
   */
  private boolean guardPointer(@Nonnull final String upgradeId) {
    final ClusterMigrationRow pointer =
        ClusterMigrationRow.load(opContext, entityService, pointerId(family.getFamily()));
    final String active = pointer.get(POINTER_ACTIVE_UPGRADE_ID);
    if (active != null && !active.equals(upgradeId)) {
      final ClusterMigrationRow activeRow =
          ClusterMigrationRow.load(opContext, entityService, active);
      final boolean inFlight =
          activeRow.status().map(ClusterMigrationState.Status::hasCopyInFlight).orElse(false);
      if (inFlight) {
        log.error(
            "{}: migration {} is still {} with its copy in flight; finish it or reset its row"
                + " before pointing {} at another cluster ({} requested)",
            id(),
            active,
            activeRow.status().orElse(null),
            family.getFamily().getConfigKey(),
            upgradeId);
        return false;
      }
    }
    if (!upgradeId.equals(active)) {
      pointer.put(POINTER_ACTIVE_UPGRADE_ID, upgradeId);
      pointer.persist(DataHubUpgradeState.SUCCEEDED);
    }
    return true;
  }

  /** {@code ClusterMigration_<family>}: the row that names the family's active migration. */
  @Nonnull
  static String pointerId(@Nonnull final SearchComponent family) {
    return ClusterMigrationState.UPGRADE_ID_PREFIX
        + "_"
        + family.getConfigKey().toLowerCase(Locale.ROOT);
  }

  private boolean targetIndicesExist(
      @Nonnull final ClusterHttp targetHttp,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final List<String> logicalNames)
      throws IOException {
    for (String logicalName : logicalNames) {
      final String targetName = migration.getNames().target(opContext, logicalName);
      if (family.getFamily() == SearchComponent.USAGE) {
        // A rollover alias or a data stream, not a concrete index.
        final String targetAlias =
            migration.getNames().targetToken(opContext) + Constants.DATAHUB_USAGE_EVENT_INDEX;
        if (IndexLayoutResolver.resolve(targetHttp, SearchComponent.USAGE, targetAlias) == null) {
          return false;
        }
      } else if (!targetHttp.indexExists(targetName)) {
        return false;
      }
    }
    return true;
  }

  private void createTargetIndices(
      @Nonnull final SearchClusterRegistry.ClusterConnection target,
      @Nonnull final ClusterHttp targetHttp,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration)
      throws Exception {
    if (family.getFamily() == SearchComponent.USAGE) {
      createUsageLayout(target, migration);
      return;
    }
    final ESIndexBuilder targetBuilder = target.getIndexBuilder();
    final List<ReindexConfig> configs =
        family
            .getService()
            .buildReindexConfigsFor(
                opContext,
                List.of(),
                targetBuilder,
                name -> migration.getNames().target(opContext, name));
    for (ReindexConfig config : configs) {
      if (config.exists() && config.requiresReindex()) {
        throw new IllegalStateException(
            "Target index "
                + config.name()
                + " exists with mappings or settings that would need a reindex; delete it or"
                + " restore a compatible snapshot before migrating");
      }
      if (!config.exists()) {
        log.info("Creating {} on cluster '{}'", config.name(), migration.getTargetCluster());
      }
      targetBuilder.buildIndex(opContext, config);
    }
  }

  private void createUsageLayout(
      @Nonnull final SearchClusterRegistry.ClusterConnection target,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration)
      throws Exception {
    final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components =
        target.asComponents(indexConvention);
    final String prefix = migration.getNames().targetToken(opContext);
    final int numShards = target.getConfig().getIndex().getNumShards();
    final int numReplicas = target.getConfig().getIndex().getNumReplicas();
    final RefreshIntervals intervals = target.getConfig().getIndex().getRefreshIntervals();
    final Integer refreshSeconds = intervals == null ? null : intervals.getUsageSeconds();
    if (refreshSeconds == null) {
      throw new IllegalStateException(
          "elasticsearch.index.refreshIntervals.usageSeconds is not configured");
    }
    UsageEventIndexSetup.createFresh(
        opContext,
        components,
        UsageEventIndexSetup.Names.of(prefix),
        numShards,
        numReplicas,
        refreshSeconds);
    final boolean openSearch = target.getClient().getEngineType().isOpenSearch();
    final String liveStaging =
        migration
            .getNames()
            .target(
                opContext,
                indexConvention.getIndexName(
                    opContext, SearchComponent.USAGE, Constants.DATAHUB_USAGE_EVENT_INDEX));
    UsageEventIndexSetup.createStagingIndex(
        opContext, components, openSearch, liveStaging, numShards, numReplicas, refreshSeconds);
  }

  private void ensureLogIndex(
      @Nonnull final ClusterHttp targetHttp,
      @Nonnull final SearchClusterRegistry.FamilyMigration migration,
      @Nonnull final SearchClusterRegistry.ClusterConnection target)
      throws IOException {
    final String logIndex = migration.getNames().logIndex(opContext);
    if (targetHttp.indexExists(logIndex)) {
      return;
    }
    final ObjectNode body = targetHttp.object();
    final ObjectNode settings = body.putObject("settings");
    settings.put("index.number_of_shards", 1);
    settings.put("index.number_of_replicas", target.getConfig().getIndex().getNumReplicas());
    final ObjectNode properties = body.putObject("mappings").putObject("properties");
    properties
        .putObject(MirroredWriteLog.FIELD_TS)
        .put("type", "date")
        .put("format", "epoch_millis");
    properties.putObject(MirroredWriteLog.FIELD_KIND).put("type", "keyword");
    properties.putObject(MirroredWriteLog.FIELD_INDEX).put("type", "keyword");
    properties.putObject(MirroredWriteLog.FIELD_ID).put("type", "keyword");
    properties.putObject(MirroredWriteLog.FIELD_QUERY).put("type", "text").put("index", false);
    final JsonNode reply = targetHttp.json("PUT", "/" + logIndex, body, "400");
    if (reply.has("error")) {
      // After the check above the only 400 expected is a creation that raced this one; a rejected
      // mapping or setting would leave the family without its delete log.
      if (!"resource_already_exists_exception".equals(reply.path("error").path("type").asText())) {
        throw new IOException("Could not create mirrored write log " + logIndex + ": " + reply);
      }
      log.info(
          "Mirrored write log {} already exists on cluster '{}'",
          logIndex,
          migration.getTargetCluster());
      return;
    }
    log.info(
        "Created mirrored write log {} on cluster '{}'", logIndex, migration.getTargetCluster());
  }
}
