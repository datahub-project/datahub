package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.upgrade.DataHubUpgradeResultConditionalPersist;
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
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ElasticSearchServiceFactory.class)
@Slf4j
public class UpdateIndicesUpgradeStrategyFactory {

  private static final String UPGRADE_ID_PREFIX = "BuildIndicesIncremental";

  // State key constants matching IncrementalReindexState
  private static final String SEPARATOR = ".";
  private static final String NEXT_INDEX_NAME = "nextIndexName";
  private static final String STATUS = "status";

  @Bean("updateIndicesUpgradeStrategy")
  @ConditionalOnProperty(
      name = "elasticsearch.buildIndices.incrementalReindexEnabled",
      havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesUpgradeStrategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityService<?> entityService,
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      GitVersion gitVersion,
      @Qualifier("revision") String revision,
      ElasticSearchConfiguration esConfig) {

    String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    Urn upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);

    Map<String, String> indexTargets =
        loadNextIndexTargets(entityService, systemOpContext, upgradeIdUrn);

    UpdateIndicesUpgradeStrategy.DualWriteStartTimeCallback callback =
        (entityName, startTimeMillis) -> {
          String originalIndexName =
              systemOpContext
                  .getSearchContext()
                  .getIndexConvention()
                  .getEntityIndexName(entityName);
          persistDualWriteStartTime(
              entityService, systemOpContext, upgradeIdUrn, originalIndexName, startTimeMillis);
        };

    long pollIntervalSeconds =
        esConfig.getBuildIndices() != null
            ? esConfig.getBuildIndices().getDualWritePollIntervalSeconds()
            : 300;

    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService,
        searchDocumentTransformer,
        indexTargets,
        callback,
        systemOpContext,
        entityService,
        upgradeIdUrn,
        pollIntervalSeconds);
  }

  /**
   * Reads Phase 1 upgrade result and builds entity name → index mappings for indices that completed
   * Phase 1 but haven't had their alias swapped yet.
   *
   * <p>Phase 1 state is keyed by index name (e.g. "datasetindex_v2"), but the dual-write strategy
   * matches MCL events by entity name (e.g. "dataset"). This method resolves the mapping by
   * iterating all entity specs and matching their index names against the upgrade state.
   */
  private Map<String, String> loadNextIndexTargets(
      EntityService<?> entityService, OperationContext opContext, Urn upgradeIdUrn) {
    Map<String, String> entityToNextIndex = new HashMap<>();

    try {
      Optional<DataHubUpgradeResult> upgradeResult =
          getUpgradeResult(entityService, opContext, upgradeIdUrn);

      if (upgradeResult.isEmpty() || upgradeResult.get().getResult() == null) {
        log.info("No Phase 1 incremental reindex state found");
        return entityToNextIndex;
      }

      Map<String, String> resultMap = upgradeResult.get().getResult();

      // Collect index names with COMPLETED status and their next index names
      Map<String, String> completedIndices = new HashMap<>();
      String statusSuffix = SEPARATOR + STATUS;
      for (Map.Entry<String, String> entry : resultMap.entrySet()) {
        if (entry.getKey().endsWith(statusSuffix) && "COMPLETED".equals(entry.getValue())) {
          String indexName =
              entry.getKey().substring(0, entry.getKey().length() - statusSuffix.length());
          String nextIndexName = resultMap.get(indexName + SEPARATOR + NEXT_INDEX_NAME);
          if (nextIndexName != null && !nextIndexName.isEmpty()) {
            completedIndices.put(indexName, nextIndexName);
          }
        }
      }

      if (completedIndices.isEmpty()) {
        return entityToNextIndex;
      }

      // Resolve index names to entity names
      IndexConvention indexConvention = opContext.getSearchContext().getIndexConvention();
      for (Map.Entry<String, String> completed : completedIndices.entrySet()) {
        Optional<String> entityName = indexConvention.getEntityName(completed.getKey());
        entityName.ifPresent(name -> entityToNextIndex.put(name, completed.getValue()));
      }

      log.info(
          "Loaded {} next index targets from Phase 1 upgrade state: {}",
          entityToNextIndex.size(),
          entityToNextIndex);
    } catch (Exception e) {
      log.warn("Failed to load Phase 1 incremental reindex state: {}", e.getMessage());
    }

    return entityToNextIndex;
  }

  private void persistDualWriteStartTime(
      EntityService<?> entityService,
      OperationContext opContext,
      Urn upgradeIdUrn,
      String indexName,
      long startTimeMillis) {
    try {
      Optional<DataHubUpgradeResult> existing =
          getUpgradeResult(entityService, opContext, upgradeIdUrn);
      if (existing.isEmpty() || existing.get().getResult() == null) {
        return;
      }
      DataHubUpgradeResult prior = existing.get();
      DataHubUpgradeResultConditionalPersist.mergeAndPersist(
          opContext,
          entityService,
          upgradeIdUrn,
          DataHubUpgradeResultConditionalPersist.putResultEntry(
              IncrementalReindexState.key(indexName, IncrementalReindexState.DUAL_WRITE_START_TIME),
              String.valueOf(startTimeMillis),
              prior.getState()));
      log.info("Persisted dual-write start time for index '{}': {}", indexName, startTimeMillis);
    } catch (Exception e) {
      log.error(
          "Failed to persist dual-write start time for index '{}': {}", indexName, e.getMessage());
    }
  }

  private Optional<DataHubUpgradeResult> getUpgradeResult(
      EntityService<?> entityService, OperationContext opContext, Urn upgradeIdUrn) {
    try {
      EntityResponse response =
          entityService.getEntityV2(
              opContext,
              upgradeIdUrn.getEntityType(),
              upgradeIdUrn,
              Set.of("dataHubUpgradeResult"));
      if (response != null && response.getAspects().containsKey("dataHubUpgradeResult")) {
        return Optional.of(
            new DataHubUpgradeResult(
                response.getAspects().get("dataHubUpgradeResult").getValue().data()));
      }
    } catch (Exception e) {
      log.debug("Could not fetch upgrade result for {}: {}", upgradeIdUrn, e.getMessage());
    }
    return Optional.empty();
  }
}
