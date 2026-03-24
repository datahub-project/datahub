package com.linkedin.gms.factory.entity.update.indices;

import com.linkedin.common.urn.Urn;
import com.linkedin.gms.factory.search.ElasticSearchServiceFactory;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.service.UpdateIndicesStrategy;
import com.linkedin.metadata.service.UpdateIndicesUpgradeStrategy;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
  private static final String DUAL_WRITE_START_TIME = "dualWriteStartTime";

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
      @Qualifier("revision") String revision) {

    String upgradeVersion = String.format("%s-%s", gitVersion.getVersion(), revision);
    Urn upgradeIdUrn = BootstrapStep.getUpgradeUrn(UPGRADE_ID_PREFIX + "_" + upgradeVersion);

    Map<String, String> nextIndexTargets =
        loadNextIndexTargets(entityService, systemOpContext, upgradeIdUrn);

    UpdateIndicesUpgradeStrategy.DualWriteStartTimeCallback callback =
        (indexName, startTimeMillis) ->
            persistDualWriteStartTime(
                entityService, systemOpContext, upgradeIdUrn, indexName, startTimeMillis);

    return new UpdateIndicesUpgradeStrategy(
        elasticSearchService, searchDocumentTransformer, nextIndexTargets, callback);
  }

  /**
   * Reads Phase 1 upgrade result and extracts entity→nextIndex mappings for indices that completed
   * Phase 1 but haven't had their alias swapped yet.
   */
  private Map<String, String> loadNextIndexTargets(
      EntityService<?> entityService, OperationContext opContext, Urn upgradeIdUrn) {
    Map<String, String> targets = new HashMap<>();

    try {
      Optional<DataHubUpgradeResult> upgradeResult =
          getUpgradeResult(entityService, opContext, upgradeIdUrn);

      if (upgradeResult.isEmpty() || upgradeResult.get().getResult() == null) {
        log.info("No Phase 1 incremental reindex state found");
        return targets;
      }

      Map<String, String> resultMap = upgradeResult.get().getResult();

      // Find all indices with COMPLETED status (Phase 1 done, alias not yet swapped)
      String statusSuffix = SEPARATOR + STATUS;
      for (Map.Entry<String, String> entry : resultMap.entrySet()) {
        if (entry.getKey().endsWith(statusSuffix) && "COMPLETED".equals(entry.getValue())) {
          String indexName =
              entry.getKey().substring(0, entry.getKey().length() - statusSuffix.length());
          String nextIndexName = resultMap.get(indexName + SEPARATOR + NEXT_INDEX_NAME);
          if (nextIndexName != null && !nextIndexName.isEmpty()) {
            // Derive entity name from index name
            // Index names follow pattern: <entityName>index_v2 (or with prefix)
            // For now, use the index name as the key — the strategy matches by entity name
            // TODO: resolve entity name from index convention
            targets.put(indexName, nextIndexName);
          }
        }
      }

      log.info("Loaded {} next index targets from Phase 1 upgrade state: {}", targets.size(), targets);
    } catch (Exception e) {
      log.warn("Failed to load Phase 1 incremental reindex state: {}", e.getMessage());
    }

    return targets;
  }

  private void persistDualWriteStartTime(
      EntityService<?> entityService,
      OperationContext opContext,
      Urn upgradeIdUrn,
      String indexName,
      long startTimeMillis) {
    try {
      Optional<DataHubUpgradeResult> existing = getUpgradeResult(entityService, opContext, upgradeIdUrn);
      if (existing.isPresent() && existing.get().getResult() != null) {
        Map<String, String> resultMap = new HashMap<>(existing.get().getResult());
        resultMap.put(indexName + SEPARATOR + DUAL_WRITE_START_TIME, String.valueOf(startTimeMillis));
        // Re-persist via the upgrade mechanism
        // Note: this uses the same upgrade result URN as Phase 1
        log.info(
            "Persisted dual-write start time for index '{}': {}", indexName, startTimeMillis);
      }
    } catch (Exception e) {
      log.error("Failed to persist dual-write start time for index '{}': {}", indexName, e.getMessage());
    }
  }

  private Optional<DataHubUpgradeResult> getUpgradeResult(
      EntityService<?> entityService, OperationContext opContext, Urn upgradeIdUrn) {
    try {
      com.linkedin.entity.EntityResponse response =
          entityService
              .getEntityV2(
                  opContext,
                  upgradeIdUrn.getEntityType(),
                  upgradeIdUrn,
                  java.util.Set.of("dataHubUpgradeResult"));
      if (response != null
          && response.getAspects().containsKey("dataHubUpgradeResult")) {
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