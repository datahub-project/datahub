package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultStore;
import com.linkedin.metadata.entity.upgrade.EntityClientUpgradeResultStore;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IncrementalReindexState;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
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
        new EntityClientUpgradeResultStore(systemEntityClient);

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

    final Map<String, String> oldIndexTargets =
        loadOldIndexTargets(upgradeResultStore, systemOpContext, upgradeIdUrn);

    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService,
        searchDocumentTransformer,
        oldIndexTargets,
        callback,
        systemOpContext,
        upgradeResultStore,
        upgradeIdUrn,
        0);
  }

  /**
   * Reads Phase 1 upgrade result and builds entity name → old backing index mappings. After Phase 1
   * swaps the alias to the next index, dual-write keeps the OLD backing index current for rollback.
   *
   * <p>Phase 1 state is keyed by index name (e.g. "datasetindex_v2"), but the dual-write strategy
   * matches MCL events by entity name (e.g. "dataset"). This method resolves the mapping using the
   * index convention.
   */
  private Map<String, String> loadOldIndexTargets(
      @Nonnull final DataHubUpgradeResultStore upgradeResultStore,
      OperationContext opContext,
      @Nonnull final Urn upgradeIdUrn) {
    final Map<String, String> entityToOldIndex = new HashMap<>();

    try {
      Optional<DataHubUpgradeResult> upgradeResult =
          getUpgradeResult(upgradeResultStore, opContext, upgradeIdUrn);

      if (upgradeResult.isEmpty() || upgradeResult.get().getResult() == null) {
        log.info("No Phase 1 incremental reindex state found");
        return entityToOldIndex;
      }

      Map<String, Map<String, String>> allStates =
          IncrementalReindexState.getAllIndexStates(upgradeResult.get().getResult());
      IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();

      for (Map.Entry<String, Map<String, String>> entry : allStates.entrySet()) {
        String indexName = entry.getKey();
        Map<String, String> indexState = entry.getValue();

        String status = indexState.get(IncrementalReindexState.STATUS);
        String oldBackingIndexName = indexState.get(IncrementalReindexState.OLD_BACKING_INDEX_NAME);

        // Only dual-write to indices that completed Phase 1 and have an old backing index recorded
        if (oldBackingIndexName == null || oldBackingIndexName.isEmpty()) {
          continue;
        }
        if (!IncrementalReindexState.Status.COMPLETED.name().equals(status)) {
          continue;
        }

        indexConvention
            .getEntityName(opContext, indexName)
            .ifPresent(name -> entityToOldIndex.put(name, oldBackingIndexName));
      }

      log.info(
          "Loaded {} old index targets for rollback dual-write: {}",
          entityToOldIndex.size(),
          entityToOldIndex);
    } catch (Exception e) {
      log.warn("Failed to load Phase 1 incremental reindex state: {}", e.getMessage());
    }

    return entityToOldIndex;
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

  /** Shared read helper, so the load and the persist path resolve the aspect the same way. */
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
