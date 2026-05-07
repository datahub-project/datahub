package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapStep;
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

  @Bean("updateIndicesUpgradeStrategy")
  @ConditionalOnProperty(
      name = "elasticsearch.buildIndices.rollbackDualWriteEnabled",
      havingValue = "true")
  @Nonnull
  protected UpdateIndicesStrategy createUpdateIndicesUpgradeStrategy(
      ElasticSearchService elasticSearchService,
      SearchDocumentTransformer searchDocumentTransformer,
      EntityService<?> entityService,
      @Qualifier("systemOperationContext") OperationContext systemOpContext,
      GitVersion gitVersion,
      @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}") String revision) {

    String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    Urn upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);

    Map<String, String> oldIndexTargets =
        loadOldIndexTargets(entityService, systemOpContext, upgradeIdUrn);

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

    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService,
        searchDocumentTransformer,
        oldIndexTargets,
        callback,
        systemOpContext,
        entityService,
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
      EntityService<?> entityService, OperationContext opContext, Urn upgradeIdUrn) {
    Map<String, String> entityToOldIndex = new HashMap<>();

    try {
      Optional<DataHubUpgradeResult> upgradeResult =
          getUpgradeResult(entityService, opContext, upgradeIdUrn);

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

        Optional<String> entityName = indexConvention.getEntityName(indexName);
        entityName.ifPresent(name -> entityToOldIndex.put(name, oldBackingIndexName));
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
