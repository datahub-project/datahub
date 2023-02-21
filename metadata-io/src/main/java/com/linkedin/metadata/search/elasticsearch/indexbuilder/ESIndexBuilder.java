package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.ImmutableMap;

import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.version.GitVersion;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.linkedin.metadata.config.ElasticSearchConfiguration;
import com.linkedin.util.Pair;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.tasks.TaskSubmissionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.TaskInfo;


@Slf4j
public class ESIndexBuilder {

  private final RestHighLevelClient searchClient;
  @Getter
  private final int numShards;

  @Getter
  private final int numReplicas;

  @Getter
  private final int numRetries;

  @Getter
  private final int refreshIntervalSeconds;

  @Getter
  private final Map<String, Map<String, String>> indexSettingOverrides;

  @Getter
  private final boolean enableIndexSettingsReindex;

  @Getter
  private final boolean enableIndexMappingsReindex;

  @Getter
  private final ElasticSearchConfiguration elasticSearchConfiguration;

  @Getter
  private final GitVersion gitVersion;

  final private static RequestOptions REQUEST_OPTIONS = RequestOptions.DEFAULT.toBuilder()
          .setRequestConfig(RequestConfig.custom()
                  .setSocketTimeout(180 * 1000).build()).build();

  private final RetryRegistry retryRegistry;

  public ESIndexBuilder(RestHighLevelClient searchClient, int numShards, int numReplicas, int numRetries,
                        int refreshIntervalSeconds, Map<String, Map<String, String>> indexSettingOverrides,
                        boolean enableIndexSettingsReindex, boolean enableIndexMappingsReindex,
                        ElasticSearchConfiguration elasticSearchConfiguration, GitVersion gitVersion) {
    this.searchClient = searchClient;
    this.numShards = numShards;
    this.numReplicas = numReplicas;
    this.numRetries = numRetries;
    this.refreshIntervalSeconds = refreshIntervalSeconds;
    this.indexSettingOverrides = indexSettingOverrides;
    this.enableIndexSettingsReindex = enableIndexSettingsReindex;
    this.enableIndexMappingsReindex = enableIndexMappingsReindex;
    this.elasticSearchConfiguration = elasticSearchConfiguration;
    this.gitVersion = gitVersion;

    RetryConfig config = RetryConfig.custom()
            .maxAttempts(Math.max(1, numRetries))
            .waitDuration(Duration.ofSeconds(10))
            .retryOnException(e -> e instanceof ElasticsearchException)
            .failAfterMaxAttempts(true)
            .build();

    // Create a RetryRegistry with a custom global configuration
    this.retryRegistry = RetryRegistry.of(config);
  }

  public ReindexConfig buildReindexState(String indexName, Map<String, Object> mappings, Map<String, Object> settings) throws IOException {
    ReindexConfig.ReindexConfigBuilder builder = ReindexConfig.builder()
            .name(indexName)
            .enableIndexSettingsReindex(enableIndexSettingsReindex)
            .enableIndexMappingsReindex(enableIndexMappingsReindex)
            .targetMappings(mappings)
            .version(gitVersion.getVersion());

    Map<String, Object> baseSettings = new HashMap<>(settings);
    baseSettings.put("number_of_shards", numShards);
    baseSettings.put("number_of_replicas", numReplicas);
    baseSettings.put("refresh_interval", String.format("%ss", refreshIntervalSeconds));
    baseSettings.putAll(indexSettingOverrides.getOrDefault(indexName, Map.of()));
    Map<String, Object> targetSetting = ImmutableMap.of("index", baseSettings);
    builder.targetSettings(targetSetting);

    // Check if index exists
    boolean exists = searchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    builder.exists(exists);

    // If index doesn't exist, no reindex
    if (!exists) {
      return builder.build();
    }

    Settings currentSettings = searchClient.indices()
            .getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
            .getIndexToSettings()
            .valuesIt()
            .next();
    builder.currentSettings(currentSettings);

    Map<String, Object> currentMappings = searchClient.indices()
            .getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
            .mappings()
            .values()
            .stream()
            .findFirst()
            .get()
            .getSourceAsMap();
    builder.currentMappings(currentMappings);

    return builder.build();
  }

  @Deprecated
  public void buildIndex(String indexName, Map<String, Object> mappings, Map<String, Object> settings) throws IOException {
    buildIndex(buildReindexState(indexName, mappings, settings));
  }

  public void buildIndex(ReindexConfig indexState) throws IOException {
    // If index doesn't exist, create index
    if (!indexState.exists()) {
      createIndex(indexState.name(), indexState);
      return;
    }

    // If there are no updates to mappings and settings, return
    if (!indexState.requiresApplyMappings() && !indexState.requiresApplySettings()) {
      log.info("No updates to index {}", indexState.name());
      return;
    }

    if (!indexState.requiresReindex()) {
      // no need to reindex and only new mappings or dynamic settings

      // Just update the additional mappings
      if (indexState.isPureMappingsAddition()) {
        log.info("Updating index {} mappings in place.", indexState.name());
        PutMappingRequest request = new PutMappingRequest(indexState.name()).source(indexState.targetMappings());
        searchClient.indices().putMapping(request, RequestOptions.DEFAULT);
        log.info("Updated index {} with new mappings", indexState.name());
      }

      if (indexState.requiresApplySettings()) {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexState.name());
        Map<String, Object> indexSettings = ((Map<String, Object>) indexState.targetSettings().get("index"))
                .entrySet().stream()
                .filter(e -> ReindexConfig.SETTINGS_DYNAMIC.contains(e.getKey()))
                .collect(Collectors.toMap(e -> "index." + e.getKey(), Map.Entry::getValue));
        request.settings(indexSettings);

        boolean ack = searchClient.indices().putSettings(request, RequestOptions.DEFAULT).isAcknowledged();
        log.info("Updated index {} with new settings. Settings: {}, Acknowledged: {}", indexState.name(),
                ReindexConfig.OBJECT_MAPPER.writeValueAsString(indexSettings), ack);
      }
    } else {
      try {
        reindex(indexState);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void reindex(ReindexConfig indexState) throws Throwable {
    final long startTime = System.currentTimeMillis();

    final int maxReindexHours = 8;
    final long initialCheckIntervalMilli = 1000;
    final long finalCheckIntervalMilli = 60000;
    final long timeoutAt = startTime + (1000 * 60 * 60 * maxReindexHours);

    String tempIndexName = indexState.name() + "_" + startTime;

    try {
      Optional<TaskInfo> previousTaskInfo = getTaskInfoByHeader(indexState.name());

      String parentTaskId;
      if (previousTaskInfo.isPresent()) {
        log.info("Reindex task {} in progress with description {}. Attempting to continue task from breakpoint.",
                previousTaskInfo.get().getTaskId(), previousTaskInfo.get().getDescription());
        parentTaskId = previousTaskInfo.get().getParentTaskId().toString();
        tempIndexName = ESUtils.extractTargetIndex(previousTaskInfo.get().getHeaders().get(ESUtils.OPAQUE_ID_HEADER));
      } else {
        // Create new index
        createIndex(tempIndexName, indexState);

        parentTaskId = submitReindex(indexState.name(), tempIndexName);
      }

      int reindexCount = 1;
      int count = 0;
      boolean reindexTaskCompleted = false;
      Pair<Long, Long> documentCounts = getDocumentCounts(indexState.name(), tempIndexName);
      long documentCountsLastUpdated = System.currentTimeMillis();

      while (System.currentTimeMillis() < timeoutAt) {
        log.info("Task: {} - Reindexing from {} to {} in progress...", parentTaskId, indexState.name(), tempIndexName);

        Pair<Long, Long> tempDocumentsCount = getDocumentCounts(indexState.name(), tempIndexName);
        if (!tempDocumentsCount.equals(documentCounts)) {
          documentCountsLastUpdated = System.currentTimeMillis();
          documentCounts = tempDocumentsCount;
        }

        if (documentCounts.getFirst().equals(documentCounts.getSecond())) {
          log.info("Task: {} - Reindexing {} to {} task was successful", parentTaskId, indexState.name(), tempIndexName);
          reindexTaskCompleted = true;
          break;

        } else {
          log.warn("Task: {} - Document counts do not match {} != {}. Complete: {}%", parentTaskId, documentCounts.getFirst(),
                  documentCounts.getSecond(), 100 * (1.0f * documentCounts.getSecond()) / documentCounts.getFirst());

          long lastUpdateDelta = System.currentTimeMillis() - documentCountsLastUpdated;
          if (lastUpdateDelta > (300 * 1000)) {
            if (reindexCount <=  numRetries) {
              log.warn("No change in index count after 5 minutes, re-triggering reindex #{}.", reindexCount);
              submitReindex(indexState.name(), tempIndexName);
              reindexCount = reindexCount + 1;
              documentCountsLastUpdated = System.currentTimeMillis(); // reset timer
            } else {
              log.warn("Reindex retry timeout for {}.", indexState.name());
              break;
            }
          }

          count = count + 1;
          Thread.sleep(Math.min(finalCheckIntervalMilli, initialCheckIntervalMilli * count));
        }
      }

      if (!reindexTaskCompleted) {
        if (elasticSearchConfiguration.getBuildIndices().isAllowDocCountMismatch()
                && elasticSearchConfiguration.getBuildIndices().isCloneIndices()) {
          log.warn("Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}\n"
                          + "This condition is explicitly ALLOWED, please refer to latest clone if original index is required.",
                  indexState.name(), documentCounts.getFirst(), documentCounts.getSecond());
        } else {
          log.error("Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}",
                  indexState.name(), documentCounts.getFirst(), documentCounts.getSecond());
          diff(indexState.name(), tempIndexName, Math.max(documentCounts.getFirst(), documentCounts.getSecond()));
          throw new RuntimeException(String.format("Reindex from %s to %s failed. Document count %s != %s", indexState.name(), tempIndexName,
                  documentCounts.getFirst(), documentCounts.getSecond()));
        }
      }
    } catch (Throwable e) {
      log.error("Failed to reindex {} to {}: Exception {}", indexState.name(), tempIndexName, e.toString());
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw e;
    }

    log.info("Reindex from {} to {} succeeded", indexState.name(), tempIndexName);
    // Check if the original index is aliased or not
    GetAliasesResponse aliasesResponse = searchClient.indices().getAlias(
            new GetAliasesRequest(indexState.name()).indices(indexState.indexPattern()), RequestOptions.DEFAULT);

    // If not aliased, delete the original index
    final Collection<String> aliasedIndexDelete;
    if (aliasesResponse.getAliases().isEmpty()) {
      log.info("Deleting index {} to allow alias creation", indexState.name());
      aliasedIndexDelete = List.of(indexState.name());
    } else {
      log.info("Deleting old indices in existing alias {}", aliasesResponse.getAliases().keySet());
      aliasedIndexDelete = aliasesResponse.getAliases().keySet();
    }

    // Add alias for the new index
    AliasActions removeAction = AliasActions.removeIndex()
            .indices(aliasedIndexDelete.toArray(new String[0]));
    AliasActions addAction = AliasActions.add().alias(indexState.name()).index(tempIndexName);
    searchClient.indices()
        .updateAliases(new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction),
            RequestOptions.DEFAULT);

    log.info("Finished setting up {}", indexState.name());
  }

  private String submitReindex(String sourceIndex, String destinationIndex) throws IOException {
    ReindexRequest reindexRequest = new ReindexRequest()
            .setSourceIndices(sourceIndex)
            .setDestIndex(destinationIndex)
            .setMaxRetries(numRetries)
            .setAbortOnVersionConflict(false)
            .setSourceBatchSize(2500);

    RequestOptions requestOptions = ESUtils.buildReindexTaskRequestOptions(gitVersion.getVersion(), sourceIndex,
            destinationIndex);
    TaskSubmissionResponse reindexTask = searchClient.submitReindexTask(reindexRequest, requestOptions);
    return reindexTask.getTask();
  }

  private Pair<Long, Long> getDocumentCounts(String sourceIndex, String destinationIndex) throws Throwable {
    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so try multiple times
    long originalCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i < this.numRetries; i++) {
      // Check if reindex succeeded by comparing document counts
      originalCount = retryRegistry.retry("retrySourceIndexCount")
              .executeCheckedSupplier(() -> getCount(sourceIndex));
      reindexedCount = retryRegistry.retry("retryDestinationIndexCount")
              .executeCheckedSupplier(() -> getCount(destinationIndex));
      if (originalCount == reindexedCount) {
        break;
      }
      try {
        Thread.sleep(20 * 1000);
      } catch (InterruptedException e) {
        log.warn("Sleep interrupted");
      }
    }

    return Pair.of(originalCount, reindexedCount);
  }

  private Optional<TaskInfo> getTaskInfoByHeader(String indexName) throws Throwable {
    Retry retryWithDefaultConfig = retryRegistry.retry("getTaskInfoByHeader");

    return retryWithDefaultConfig.executeCheckedSupplier(() -> {
      ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(true);
      List<TaskInfo> taskInfos = searchClient.tasks().list(listTasksRequest, REQUEST_OPTIONS).getTasks();
      return taskInfos.stream()
              .filter(info -> ESUtils.prefixMatch(info.getHeaders().get(ESUtils.OPAQUE_ID_HEADER), gitVersion.getVersion(),
                      indexName)).findFirst();
    });
  }

  private void diff(String indexA, String indexB, long maxDocs) {
    if (maxDocs <= 100) {

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(100);
      searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

      SearchRequest indexARequest = new SearchRequest(indexA);
      indexARequest.source(searchSourceBuilder);
      SearchRequest indexBRequest = new SearchRequest(indexB);
      indexBRequest.source(searchSourceBuilder);

      try {
        SearchResponse responseA = searchClient.search(indexARequest, RequestOptions.DEFAULT);
        SearchResponse responseB = searchClient.search(indexBRequest, RequestOptions.DEFAULT);

        Set<String> actual = Arrays.stream(responseB.getHits().getHits())
                .map(SearchHit::getId).collect(Collectors.toSet());

        log.error("Missing {}", Arrays.stream(responseA.getHits().getHits())
                .filter(doc -> !actual.contains(doc.getId()))
                .map(SearchHit::getSourceAsString).collect(Collectors.toSet()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private long getCount(@Nonnull String indexName) throws IOException {
    return searchClient.count(new CountRequest(indexName).query(QueryBuilders.matchAllQuery()), RequestOptions.DEFAULT)
        .getCount();
  }

  private void createIndex(String indexName, ReindexConfig state) throws IOException {
    log.info("Index {} does not exist. Creating", indexName);
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.mapping(state.targetMappings());
    createIndexRequest.settings(state.targetSettings());
    searchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    log.info("Created index {}", indexName);
  }

  public static void cleanIndex(RestHighLevelClient searchClient, ElasticSearchConfiguration esConfig, ReindexConfig indexState) {
    log.info("Checking for orphan index pattern {} older than {} {}", indexState.indexPattern(),
            esConfig.getBuildIndices().getRetentionValue(),
            esConfig.getBuildIndices().getRetentionUnit());

    getOrphanedIndices(searchClient, esConfig, indexState).forEach(orphanIndex -> {
      log.warn("Deleting orphan index {}.", orphanIndex);
      try {
        searchClient.indices().delete(new DeleteIndexRequest().indices(orphanIndex), RequestOptions.DEFAULT);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private static List<String> getOrphanedIndices(RestHighLevelClient searchClient, ElasticSearchConfiguration esConfig,
                                                 ReindexConfig indexState) {
    List<String> orphanedIndices = new ArrayList<>();
    try {
      Date retentionDate = Date.from(Instant.now()
              .minus(Duration.of(esConfig.getBuildIndices().getRetentionValue(),
                      ChronoUnit.valueOf(esConfig.getBuildIndices().getRetentionUnit()))));

      GetIndexResponse response = searchClient.indices().get(new GetIndexRequest(indexState.indexCleanPattern()), RequestOptions.DEFAULT);

      for (String index : response.getIndices()) {
        var creationDateStr = response.getSetting(index, "index.creation_date");
        var creationDateEpoch = Long.parseLong(creationDateStr);
        var creationDate = new Date(creationDateEpoch);

        if (creationDate.after(retentionDate)) {
          continue;
        }

        if (response.getAliases().containsKey(index) && response.getAliases().get(index).size() == 0) {
          log.info("Index {} is orphaned", index);
          orphanedIndices.add(index);
        }
      }
    } catch (Exception e) {
      if (e.getMessage().contains("index_not_found_exception")) {
        log.info("No orphaned indices found with pattern {}", indexState.indexCleanPattern());
      } else {
        log.error("An error occurred when trying to identify orphaned indices. Exception: {}", e.getMessage());
      }
    }
    return orphanedIndices;
  }
}
