package com.linkedin.datahub.upgrade.system.schemafield;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;
import static com.linkedin.metadata.utils.GenericRecordUtils.entityResponseToSystemAspectMap;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

/**
 * Performs a migration from one document id convention to another.
 *
 * <p>This step identifies documents using the old convention, if any, and generates MCLs for
 * aspects for updating the data in the new document.
 *
 * <p>Finally, a DELETE is executed on the legacy document id.
 */
@Slf4j
public class MigrateSchemaFieldDocIdsStep implements UpgradeStep {

  private final OperationContext opContext;
  private final EntityRegistry entityRegistry;
  private final RestHighLevelClient elasticsearchClient;
  private final BulkProcessor bulkProcessor;
  private final String indexName;
  private final EntityService<?> entityService;
  private final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(5L));
  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public MigrateSchemaFieldDocIdsStep(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents elasticSearchComponents,
      EntityService<?> entityService,
      int batchSize,
      int batchDelayMs,
      int limit) {
    this.opContext = opContext;
    this.entityRegistry = opContext.getEntityRegistry();
    this.elasticsearchClient = elasticSearchComponents.getSearchClient();
    this.entityService = entityService;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
    this.indexName =
        elasticSearchComponents.getIndexConvention().getEntityIndexName(SCHEMA_FIELD_ENTITY_NAME);
    this.bulkProcessor = buildBuildProcessor();
    log.info("MigrateSchemaFieldDocIdsStep initialized");
  }

  @Override
  public String id() {
    return "schema-field-doc-id-v1";
  }

  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable SKIP_MIGRATE_SCHEMA_FIELDS_DOC_ID to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    if (Boolean.parseBoolean(System.getenv("SKIP_MIGRATE_SCHEMA_FIELDS_DOC_ID"))) {
      log.info("Environment variable SKIP_MIGRATE_SCHEMA_FIELDS_DOC_ID is set to true. Skipping.");
      return true;
    }

    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    return prevResult
        .filter(
            result ->
                DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                    || DataHubUpgradeState.ABORTED.equals(result.getState()))
        .isPresent();
  }

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final SearchRequest searchRequest = buildSearchRequest();
      String scrollId = null;
      int migratedCount = 0;

      try {
        do {
          log.info(
              "Upgrading batch of schemaFields {}-{}", migratedCount, migratedCount + batchSize);
          scrollId = updateDocId(searchRequest, scrollId);
          migratedCount += batchSize;

          if (limit > 0 && migratedCount >= limit) {
            log.info("Exiting early due to limit.");
            break;
          }

          if (batchDelayMs > 0) {
            log.info("Sleeping for {} ms", batchDelayMs);
            try {
              Thread.sleep(batchDelayMs);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        } while (scrollId != null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        bulkProcessor.flush();
      }

      BootstrapStep.setUpgradeResult(context.opContext(), getUpgradeIdUrn(), entityService);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private BulkProcessor buildBuildProcessor() {
    return BulkProcessor.builder(
            (request, bulkListener) ->
                elasticsearchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
            new BulkProcessor.Listener() {
              @Override
              public void beforeBulk(long executionId, BulkRequest request) {
                log.debug("Deleting {} legacy schemaField documents", request.numberOfActions());
              }

              @Override
              public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.debug(
                    "Delete executed {} failures", response.hasFailures() ? "with" : "without");
              }

              @Override
              public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.warn("Error while executing legacy schemaField documents", failure);
              }
            })
        .setBulkActions(batchSize)
        .build();
  }

  private SearchRequest buildSearchRequest() {
    Script oldDocumentIdQuery =
        new Script(
            Script.DEFAULT_SCRIPT_TYPE,
            "painless",
            "doc['_id'][0].indexOf('urn%3Ali%3AschemaField%3A%28urn%3Ali%3Adataset%3A') > -1",
            Map.of());
    QueryBuilder queryBuilder = QueryBuilders.scriptQuery(oldDocumentIdQuery);

    SearchRequest searchRequest = new SearchRequest(indexName);
    searchRequest.scroll(scroll);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(queryBuilder);
    searchSourceBuilder.size(batchSize);
    searchSourceBuilder.sort("urn", SortOrder.ASC);
    searchRequest.source(searchSourceBuilder);

    return searchRequest;
  }

  private String updateDocId(final SearchRequest searchRequest, final String scrollId)
      throws IOException, URISyntaxException {
    final SearchResponse searchResponse;

    if (scrollId == null) {
      searchResponse = elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);
    } else {
      SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
      scrollRequest.scroll(scroll);
      searchResponse = elasticsearchClient.scroll(scrollRequest, RequestOptions.DEFAULT);
    }

    final SearchHit[] searchHits = searchResponse.getHits().getHits();
    final String nextScrollId = searchResponse.getScrollId();

    if (searchHits.length > 0) {
      Set<String> documentIds =
          Arrays.stream(searchHits).map(SearchHit::getId).collect(Collectors.toSet());
      Set<Urn> batchUrns =
          Arrays.stream(searchHits)
              .map(hit -> hit.getSourceAsMap().get("urn").toString())
              .map(UrnUtils::getUrn)
              .collect(Collectors.toSet());

      log.info("Sending MCLs for {} entities", batchUrns.size());
      emitMCLs(batchUrns);
      log.info("Removing old document ids for {} documents", documentIds.size());
      deleteDocumentIds(documentIds);

      return nextScrollId;
    }

    return null;
  }

  private void emitMCLs(Set<Urn> batchUrns) throws URISyntaxException {
    Set<SystemAspect> batchAspects =
        entityResponseToSystemAspectMap(
                entityService.getEntitiesV2(
                    opContext,
                    SCHEMA_FIELD_ENTITY_NAME,
                    batchUrns,
                    opContext.getEntityAspectNames(SCHEMA_FIELD_ENTITY_NAME),
                    false),
                entityRegistry)
            .values()
            .stream()
            .flatMap(m -> m.values().stream())
            .collect(Collectors.toSet());

    Set<Future<?>> futures =
        batchAspects.stream()
            .map(
                systemAspect ->
                    entityService
                        .alwaysProduceMCLAsync(
                            opContext,
                            systemAspect.getUrn(),
                            systemAspect.getUrn().getEntityType(),
                            systemAspect.getAspectName(),
                            systemAspect.getAspectSpec(),
                            null,
                            systemAspect.getRecordTemplate(),
                            null,
                            systemAspect.getSystemMetadata(),
                            AuditStampUtils.createDefaultAuditStamp(),
                            ChangeType.UPSERT)
                        .getFirst())
            .collect(Collectors.toSet());

    futures.forEach(
        f -> {
          try {
            f.get();
          } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void deleteDocumentIds(Set<String> documentIds) {
    documentIds.forEach(docId -> bulkProcessor.add(new DeleteRequest(indexName, docId)));
  }
}
