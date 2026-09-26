package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.UsageEventIndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateUsageEventIndicesStep implements UpgradeStep {
  private static final String SKIP_LEGACY_INDEX_MIGRATION_ENV =
      "SKIP_LEGACY_USAGE_EVENT_INDEX_MIGRATION";

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  private final ConfigurationProvider configurationProvider;
  @Nullable private final SearchClusterRegistry searchClusterRegistry;
  // Retries of this step within one run do not move the legacy index aside again: each attempt
  // would
  // leave another clone, all copied back later, and a rollover between those copies duplicates
  // events. Copying backups back still runs on every attempt.
  private boolean legacyIndexMoveAttempted = false;
  // Identifies this run on the legacy migration lease, so its own retries acquire it again.
  private final String leaseOwner = UUID.randomUUID().toString();

  public CreateUsageEventIndicesStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      ConfigurationProvider configurationProvider) {
    this(esComponents, configurationProvider, null);
  }

  public CreateUsageEventIndicesStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      ConfigurationProvider configurationProvider,
      @Nullable SearchClusterRegistry searchClusterRegistry) {
    this.esComponents = esComponents;
    this.configurationProvider = configurationProvider;
    this.searchClusterRegistry = searchClusterRegistry;
  }

  @Override
  public String id() {
    return "CreateUsageEventIndicesStep";
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    boolean skipViaEnvVar =
        EnvironmentUtils.getBoolean("SKIP_CREATE_USAGE_EVENT_INDICES_STEP", false);
    if (skipViaEnvVar) {
      log.info(
          "Environment variable SKIP_CREATE_USAGE_EVENT_INDICES_STEP is set to true. Skipping usage event index setup.");
      return true;
    }

    boolean analyticsEnabled = configurationProvider.getPlatformAnalytics().isEnabled();
    if (!analyticsEnabled) {
      log.info("DataHub analytics is disabled, skipping usage event index setup");
    }
    return !analyticsEnabled;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        BaseElasticSearchComponentsFactory.BaseElasticSearchComponents usageComponents =
            usageClusterComponents();

        final String indexPrefix = usageComponents.getConfig().getIndex().getFinalPrefix();
        boolean useOpenSearch = usageComponents.getSearchClient().getEngineType().isOpenSearch();
        int numShards = usageComponents.getConfig().getIndex().getNumShards();
        int numReplicas = usageComponents.getConfig().getIndex().getNumReplicas();

        log.info(
            "Creating usage event indices on engine {} shards={} replicas={}",
            usageComponents.getSearchClient().getEngineType(),
            numShards,
            numReplicas);

        if (useOpenSearch) {
          setupOpenSearchUsageEvents(
              usageComponents, indexPrefix, numShards, numReplicas, context.opContext());
        } else {
          setupElasticsearchUsageEvents(
              usageComponents, context.opContext(), indexPrefix, numShards, numReplicas);
        }

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("CreateUsageEventIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Nonnull
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents usageClusterComponents() {
    if (searchClusterRegistry == null) {
      return esComponents;
    }
    return searchClusterRegistry
        .connectionFor(SearchComponent.USAGE)
        .asComponents(esComponents.getIndexConvention());
  }

  private void setupElasticsearchUsageEvents(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      OperationContext operationContext,
      String prefix,
      int numShards,
      int numReplicas)
      throws Exception {
    String prefixedPolicy = prefix + "datahub_usage_event_policy";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedDataStream = prefix + "datahub_usage_event";

    UsageEventIndexUtils.createIlmPolicy(operationContext, cluster, prefixedPolicy);
    UsageEventIndexUtils.createIndexTemplate(
        operationContext,
        cluster,
        prefixedTemplate,
        prefixedPolicy,
        numShards,
        numReplicas,
        prefix);
    withLegacyMigration(
        cluster,
        operationContext,
        prefix,
        false,
        true,
        () -> UsageEventIndexUtils.createDataStream(operationContext, cluster, prefixedDataStream));
  }

  private void setupOpenSearchUsageEvents(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      String prefix,
      int numShards,
      int numReplicas,
      OperationContext operationContext)
      throws Exception {
    String prefixedPolicy = prefix + "datahub_usage_event_policy";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedAlias = prefix + "datahub_usage_event";
    String prefixedIndex = prefix + "datahub_usage_event-000001";

    boolean policyCreated =
        UsageEventIndexUtils.createIsmPolicy(cluster, prefixedPolicy, prefix, operationContext);
    log.info("ISM policy creation result: {}", policyCreated);

    if (policyCreated) {
      log.info("ISM policy created successfully, proceeding with template and index creation");
      log.info("Creating index template: {}", prefixedTemplate);
      UsageEventIndexUtils.createOpenSearchIndexTemplate(
          operationContext, cluster, prefixedTemplate, numShards, numReplicas, prefix);
      withLegacyMigration(
          cluster,
          operationContext,
          prefix,
          true,
          true,
          () -> {
            log.info("Creating initial index: {} with alias: {}", prefixedIndex, prefixedAlias);
            UsageEventIndexUtils.createOpenSearchUsageEventIndex(
                operationContext, cluster, prefixedIndex, prefixedAlias);
          });
    } else {
      log.warn(
          "ISM policy creation failed or is not supported. Skipping template and index creation to avoid configuration issues.");
      log.info("Usage event tracking will not be available without proper policy configuration.");
      // A layout an earlier run created may still have backups to copy back.
      withLegacyMigration(cluster, operationContext, prefix, true, false, null);
    }
  }

  @FunctionalInterface
  private interface LayoutSetup {
    void run() throws Exception;
  }

  /**
   * Creates the usage event layout with a legacy index moved out of its way and its backups copied
   * back into it, under the legacy migration lease. Copying back runs on every attempt, so a retry
   * recovers a move that failed after removing the original.
   */
  private void withLegacyMigration(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      OperationContext operationContext,
      String prefix,
      boolean useOpenSearch,
      boolean moveAllowed,
      @Nullable LayoutSetup layoutSetup)
      throws Exception {
    if (EnvironmentUtils.getBoolean(SKIP_LEGACY_INDEX_MIGRATION_ENV, false)) {
      log.info(
          "Environment variable {} is set to true. Skipping legacy usage event index migration.",
          SKIP_LEGACY_INDEX_MIGRATION_ENV);
      if (layoutSetup != null) {
        layoutSetup.run();
      }
      return;
    }
    UsageEventIndexUtils.LegacyMigrationLease lease =
        UsageEventIndexUtils.acquireLegacyMigrationLease(
            operationContext,
            cluster,
            prefix,
            leaseOwner,
            moveAllowed && !legacyIndexMoveAttempted);
    try {
      if (lease != null && moveAllowed && !legacyIndexMoveAttempted) {
        legacyIndexMoveAttempted = true;
        moveLegacyIndexAside(cluster, operationContext, prefix, useOpenSearch, lease);
      }
      if (layoutSetup != null) {
        layoutSetup.run();
      }
      if (lease != null) {
        UsageEventIndexUtils.startLegacyBackupCopies(
            operationContext, cluster, prefix, useOpenSearch, lease);
      }
    } finally {
      if (lease != null) {
        lease.release();
      }
    }
    if (lease != null) {
      UsageEventIndexUtils.finishLegacyBackupCopies(operationContext, cluster, prefix);
    }
  }

  private void moveLegacyIndexAside(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      OperationContext operationContext,
      String prefix,
      boolean useOpenSearch,
      UsageEventIndexUtils.LegacyMigrationLease lease) {
    try {
      UsageEventIndexUtils.moveLegacyUsageEventIndexAside(
          operationContext, cluster, prefix, useOpenSearch, lease);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while moving the legacy usage event index for '{}' aside", prefix, e);
    } catch (Exception e) {
      // A move that failed after removing the original is recovered by copying back below; one
      // that failed earlier left the original in place for a later run.
      log.error("Failed to move the legacy usage event index for '{}' aside", prefix, e);
    }
  }
}
