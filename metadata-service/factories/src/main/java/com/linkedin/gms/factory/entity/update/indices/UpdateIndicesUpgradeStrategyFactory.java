package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.entity.upgrade.EntityClientUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.IncrementalReindexWriteTargets;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ElasticSearchServiceFactory.class)
@Slf4j
public class UpdateIndicesUpgradeStrategyFactory {

  /**
   * Rollback dual-write reads its targets from, and persists {@code dualWriteStartTime} to, the
   * {@code dataHubUpgradeResult} aspect. That goes through {@link SystemEntityClient} rather than
   * {@code EntityService} so these beans work in every context that indexes MCLs, including the
   * standalone MAE consumer, which runs {@code entityClient.impl=restli} and has no datasource.
   *
   * <p>One target set is shared by the V2 rollback strategy and the V3 update strategy, so both
   * generations stop dual-writing to an index at the same moment Phase 2 disables it.
   *
   * <p>The bean exists whether or not rollback dual-write is enabled: a live delete is mirrored
   * into the index a rebuild is filling regardless, and the flag only decides whether rollback
   * targets are loaded.
   */
  @Bean("incrementalReindexWriteTargets")
  @Nonnull
  protected IncrementalReindexWriteTargets createIncrementalReindexWriteTargets(
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      GitVersion gitVersion,
      @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}") String revision,
      final ConfigurationProvider configProvider) {

    final DataHubUpgradeResultStore upgradeResultStore =
        new EntityClientUpgradeResultStore(systemEntityClient);

    final String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    final Urn upgradeIdUrn =
        BootstrapStep.getUpgradeUrn(
            IncrementalReindexState.UPGRADE_ID_PREFIX + "_" + upgradeVersion);

    return IncrementalReindexWriteTargets.fromUpgradeResult(
        new IncrementalReindexWriteTargets.StateSource(
            systemOpContext, upgradeResultStore, upgradeIdUrn),
        (indexName, startTimeMillis) ->
            persistDualWriteStartTime(
                upgradeResultStore, systemOpContext, upgradeIdUrn, indexName, startTimeMillis),
        configProvider.getElasticSearch().getBuildIndices().isRollbackDualWriteEnabled());
  }

  @Bean("updateIndicesUpgradeStrategy")
  @ConditionalOnProperty(
      name = "elasticsearch.buildIndices.rollbackDualWriteEnabled",
      havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesUpgradeStrategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      @Qualifier("incrementalReindexWriteTargets")
          IncrementalReindexWriteTargets incrementalReindexWriteTargets) {
    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService, searchDocumentTransformer, incrementalReindexWriteTargets);
  }

  private void persistDualWriteStartTime(
      @Nonnull final DataHubUpgradeResultStore upgradeResultStore,
      OperationContext opContext,
      @Nonnull final Urn upgradeIdUrn,
      @Nonnull final String indexName,
      final long startTimeMillis) {
    try {
      Optional<DataHubUpgradeResult> existing =
          getUpgradeResult(upgradeResultStore, opContext, upgradeIdUrn);
      if (existing.isEmpty() || existing.get().getResult() == null) {
        return;
      }
      DataHubUpgradeResult prior = existing.get();
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          opContext,
          upgradeResultStore,
          upgradeIdUrn,
          DataHubUpgradeResultConditionalPersist.putResultEntry(
              IncrementalReindexState.key(indexName, IncrementalReindexState.DUAL_WRITE_START_TIME),
              String.valueOf(startTimeMillis),
              prior.getState()),
          DataHubUpgradeResultConditionalPersist.CLIENT_MAX_ATTEMPTS);
      log.info("Persisted dual-write start time for index '{}': {}", indexName, startTimeMillis);
    } catch (Exception e) {
      log.error(
          "Failed to persist dual-write start time for index '{}': {}", indexName, e.getMessage());
    }
  }

  private Optional<DataHubUpgradeResult> getUpgradeResult(
      @Nonnull final DataHubUpgradeResultStore upgradeResultStore,
      OperationContext opContext,
      @Nonnull final Urn upgradeIdUrn) {
    try {
      return Optional.ofNullable(
          DataHubUpgradeResultConditionalPersist.fromEnveloped(
              upgradeResultStore.readLatest(opContext, upgradeIdUrn)));
    } catch (Exception e) {
      log.debug("Could not fetch upgrade result for {}: {}", upgradeIdUrn, e.getMessage());
    }
    return Optional.empty();
  }
}
