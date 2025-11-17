package com.linkedin.datahub.upgrade.system.semanticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.reindex.ReindexRequest;

/**
 * Upgrade step that copies documents from base entity indices to semantic search indices.
 *
 * <p>The embeddings field will be empty after copying - a separate backfill process is needed to
 * populate embeddings.
 */
@Slf4j
public class CopyDocumentsToSemanticIndexStep implements UpgradeStep {

  private static final String UPGRADE_ID_PREFIX = "CopyDocumentsToSemanticIndex";

  private final OperationContext opContext;
  private final String entityName;
  private final String upgradeId;
  private final Urn upgradeIdUrn;
  private final SearchClientShim<?> searchClient;
  private final EntityService<?> entityService;
  private final IndexConvention indexConvention;

  public CopyDocumentsToSemanticIndexStep(
      OperationContext opContext,
      String entityName,
      SearchClientShim<?> searchClient,
      EntityService<?> entityService,
      IndexConvention indexConvention) {
    this.opContext = opContext;
    this.entityName = entityName;
    this.searchClient = searchClient;
    this.entityService = entityService;
    this.indexConvention = indexConvention;

    upgradeId = UPGRADE_ID_PREFIX + "_" + entityName;
    upgradeIdUrn = BootstrapStep.getUpgradeUrn(upgradeId);
  }

  @Override
  public String id() {
    return upgradeId;
  }

  private UpgradeStepResult execute(UpgradeContext context) {
    try {
      String baseIndexName = indexConvention.getEntityIndexName(entityName);
      String semanticIndexName = indexConvention.getEntityIndexNameSemantic(entityName);
      log.info(
          "Starting document copy for entity '{}': {} -> {}",
          entityName,
          baseIndexName,
          semanticIndexName);

      // Check if semantic index exists
      GetIndexRequest getIndexRequest = new GetIndexRequest(semanticIndexName);
      if (!searchClient.indexExists(getIndexRequest, RequestOptions.DEFAULT)) {
        log.error("Semantic index '{}' does not exist. Skipping.", semanticIndexName);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      // Submit reindex task
      ReindexRequest reindexRequest =
          new ReindexRequest()
              .setSourceIndices(baseIndexName)
              .setDestIndex(semanticIndexName)
              .setMaxRetries(3)
              .setAbortOnVersionConflict(false);

      String taskId = searchClient.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
      log.info("Document copy task submitted for entity '{}'. Task ID: {}", entityName, taskId);

      // TODO: Properly handle asynchrony of the reindex task.
      // - Persist task id with step state IN_PROGRESS
      // - Wait until reindex task completes before storing step state as SUCCEEDED

      BootstrapStep.setUpgradeResult(opContext, upgradeIdUrn, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    } catch (Exception e) {
      log.error("Failed to copy documents for entity: {}", entityName, e);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return this::execute;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    // Don't skip - let the enabled entities check handle this
    return false;
  }

  @Override
  public boolean isOptional() {
    // This is optional - don't block other upgrades if this fails
    return true;
  }
}
