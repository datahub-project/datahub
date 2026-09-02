package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
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

  private static final String UPGRADE_ID_PREFIX = "BuildIndicesIncremental";

  /**
   * Dual-write reads its old-index targets from, and persists {@code dualWriteStartTime} to, the
   * {@code dataHubUpgradeResult} aspect. That goes through {@link SystemEntityClient} rather than
   * {@code EntityService} so this bean works in every context that indexes MCLs — including the
   * standalone MAE consumer, which runs {@code entityClient.impl=restli} and has no datasource.
   */
  @Bean("updateIndicesUpgradeStrategy")
  @ConditionalOnProperty(
      name = "elasticsearch.buildIndices.rollbackDualWriteEnabled",
      havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesUpgradeStrategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      @Qualifier("systemEntityClient") final SystemEntityClient systemEntityClient,
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      GitVersion gitVersion,
      @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}") String revision) {

    final DataHubUpgradeResultStore upgradeResultStore =
        DataHubUpgradeResultStore.of(systemEntityClient);

    final String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    final Urn upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);

    final UpdateIndicesUpgradeStrategy.DualWriteStartTimeCallback callback =
        (entityName, startTimeMillis) -> {
          final String originalIndexName =
              systemOpContext
                  .getSearchContext()
                  .getIndexConvention()
                  .getEntityIndexName(systemOpContext, entityName);
          persistDualWriteStartTime(
              upgradeResultStore,
              systemOpContext,
              upgradeIdUrn,
              originalIndexName,
              startTimeMillis);
        };

    // Targets are derived by the strategy itself, which reconciles them against persisted state on
    // construction and on every poll, so a read that fails at startup recovers instead of leaving
    // dual-write permanently off.
    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService,
        searchDocumentTransformer,
        Map.of(),
        callback,
        systemOpContext,
        upgradeResultStore,
        upgradeIdUrn,
        0);
  }

  /**
   * Throws on failure by design. The caller only records the start time once per index, so
   * swallowing a transient read or write error here would lose it permanently — the exception is
   * what lets the caller reset its flag and try again on the next batch.
   */
  private void persistDualWriteStartTime(
      @Nonnull final DataHubUpgradeResultStore upgradeResultStore,
      OperationContext opContext,
      @Nonnull final Urn upgradeIdUrn,
      @Nonnull final String indexName,
      final long startTimeMillis)
      throws Exception {

    // Read failures propagate rather than being flattened into "no upgrade record": the latter
    // looks like success to the caller, which then never asks again.
    final DataHubUpgradeResult prior =
        DataHubUpgradeResultConditionalPersist.fromEnveloped(
            upgradeResultStore.readLatest(opContext, upgradeIdUrn));

    if (prior == null || prior.getResult() == null) {
      // Genuinely no upgrade record: nothing to merge into, and retrying will not change that.
      log.warn(
          "No incremental reindex state at {} while recording dual-write start for index '{}'",
          upgradeIdUrn,
          indexName);
      return;
    }

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
  }
}
