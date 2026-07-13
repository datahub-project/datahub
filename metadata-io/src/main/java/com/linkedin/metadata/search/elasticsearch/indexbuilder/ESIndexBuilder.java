package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions.ReplicaHealthException;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.search.utils.RetryConfigUtils;
import com.linkedin.metadata.search.utils.SizeUtils;
import com.linkedin.metadata.timeseries.BatchWriteOperationsOptions;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.OperationContext;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.*;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.TaskInfo;

@Slf4j
public class ESIndexBuilder {

  //  this setting is not allowed to change as of now in AOS:
  // https://docs.aws.amazon.com/opensearch-service/latest/developerguide/supported-operations.html
  //  public static final String INDICES_MEMORY_INDEX_BUFFER_SIZE =
  // "indices.memory.index_buffer_size";
  private static final String INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE =
      "index.translog.flush_threshold_size";
  public static final String REFRESH_INTERVAL = "refresh_interval";
  public static final String INDEX_REFRESH_INTERVAL = "index." + REFRESH_INTERVAL;
  public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
  private static final String INDEX_NUMBER_OF_REPLICAS = "index." + NUMBER_OF_REPLICAS;
  private static final String NUMBER_OF_SHARDS = "number_of_shards";
  private static final String ORIGINALPREFIX = "original";
  private static final Float MINJVMHEAP = 10.F;
  private static final Retry staticRetryer =
      Retry.of("common-static-retryer", RetryConfigUtils.EXPONENTIAL);

  // for debugging
  // private static final Float MINJVMHEAP = 0.1F;

  /**
   * -- GETTER -- Get the underlying search client.
   *
   * @return The SearchClientShim used for elasticsearch/opensearch operations
   */
  @Getter private final SearchClientShim<?> searchClient;

  @Getter @VisibleForTesting private final ElasticSearchConfiguration config;

  private final IndexConfiguration indexConfig;

  @Getter @VisibleForTesting private final StructuredPropertiesConfiguration structPropConfig;

  @Getter private final Map<String, Map<String, String>> indexSettingOverrides;

  @Getter @VisibleForTesting private final GitVersion gitVersion;

  @Getter @VisibleForTesting private final OpenSearchJvmInfo jvminfo;

  /**
   * Extended socket timeout for slow operations (count, refresh, createIndex, reindex, listTasks).
   */
  private final RequestOptions requestOptionsLong;

  private final RetryRegistry retryRegistry;
  private static RetryRegistry deletionRetryRegistry;
  private final int initialSecondsDelete = 10;
  private final int deleteMultiplier = 9;
  // would wait >3000s for the 5th retry
  private static final int deleteMaxAttempts = 5;

  // Retry instances for various operations
  private final Retry healthCheckRetry;
  private final Retry settingsUpdateRetry;
  private final Retry taskStatusRetry;
  private final Retry reindexSubmissionRetry;

  public ESIndexBuilder(
      SearchClientShim<?> searchClient,
      ElasticSearchConfiguration elasticSearchConfiguration,
      StructuredPropertiesConfiguration structuredPropertiesConfiguration,
      Map<String, Map<String, String>> indexSettingOverrides,
      GitVersion gitVersion) {
    this.searchClient = searchClient;
    this.config = elasticSearchConfiguration;
    this.indexConfig = elasticSearchConfiguration.getIndex();
    this.structPropConfig = structuredPropertiesConfiguration;
    this.indexSettingOverrides = indexSettingOverrides;
    this.gitVersion = gitVersion;

    BuildIndicesConfiguration buildIndices =
        Objects.requireNonNull(
            elasticSearchConfiguration.getBuildIndices(), "buildIndices config is required");
    int slowTimeoutSec =
        buildIndices.getSlowOperationTimeoutSeconds() > 0
            ? buildIndices.getSlowOperationTimeoutSeconds()
            : BuildIndicesConfiguration.DEFAULT_SLOW_OPERATION_TIMEOUT_SECONDS;
    this.requestOptionsLong =
        RequestOptions.DEFAULT.toBuilder()
            .setRequestConfig(
                RequestConfig.custom().setSocketTimeout(slowTimeoutSec * 1000).build())
            .build();

    // Create a RetryRegistry with a custom global configuration
    RetryConfig config =
        RetryConfig.custom()
            .maxAttempts(Math.max(1, indexConfig.getNumRetries()))
            .waitDuration(Duration.ofSeconds(10))
            .retryOnException(e -> e instanceof OpenSearchException)
            .failAfterMaxAttempts(true)
            .build();
    this.retryRegistry = RetryRegistry.of(config);

    int countRetryAttempts =
        buildIndices.getCountRetryMaxAttempts() > 0
            ? Math.max(1, buildIndices.getCountRetryMaxAttempts())
            : BuildIndicesConfiguration.DEFAULT_COUNT_RETRY_MAX_ATTEMPTS;
    int countRetryWaitSec =
        buildIndices.getCountRetryWaitSeconds() > 0
            ? buildIndices.getCountRetryWaitSeconds()
            : BuildIndicesConfiguration.DEFAULT_COUNT_RETRY_WAIT_SECONDS;
    RetryConfig countRetryConfig =
        RetryConfig.custom()
            .maxAttempts(countRetryAttempts)
            .waitDuration(Duration.ofSeconds(countRetryWaitSec))
            .retryOnException(
                e ->
                    e instanceof OpenSearchException
                        || e instanceof SocketTimeoutException
                        || (e.getCause() != null
                            && (e.getCause() instanceof OpenSearchException
                                || e.getCause() instanceof SocketTimeoutException)))
            .failAfterMaxAttempts(true)
            .build();
    this.retryRegistry.addConfiguration("countRetry", countRetryConfig);

    // Configure delete retry behavior
    // this is hitting this issue: https://github.com/resilience4j/resilience4j/issues/1404
    // https://github.com/resilience4j/resilience4j/discussions/1854
    // when we go with jdk17, we can upgrade resilience4j to newer version and see if this is
    // fixed...
    //        RetryConfig deletionRetryConfig =
    //                RetryConfig.custom()
    //                        .maxAttempts(deleteMaxAttempts) // Maximum number of attempts
    //                        .waitDuration(Duration.ofSeconds(initialSecondsDelete))
    //                        .retryExceptions(IOException.class) // Retry on IOException
    //                        .retryOnException(
    //                                createElasticsearchRetryPredicate()) // Custom predicate for
    // other exceptions
    //                        .intervalFunction(
    //                                IntervalFunction.ofExponentialBackoff(
    //                                        Duration.ofSeconds(initialSecondsDelete),
    //                                        deleteMultiplier)) // Exponential backoff
    //                        .failAfterMaxAttempts(true) // Throw exception after max attempts
    //                        .build();
    RetryConfig deletionRetryConfig =
        RetryConfig.custom()
            .maxAttempts(deleteMaxAttempts) // Maximum number of attempts
            .waitDuration(Duration.ofSeconds(initialSecondsDelete))
            .retryExceptions(IOException.class) // Retry on IOException
            .retryExceptions(OpenSearchStatusException.class) // this is thrown if read only
            .retryExceptions(
                Exception.class) // not sure what is thrown when snapshots are being taken...be
            // aggressive
            // here, we won't try deleting forever anyway
            .retryOnException(
                createElasticsearchRetryPredicate()) // Custom predicate for other exceptions
            .failAfterMaxAttempts(true) // Throw exception after max attempts
            .build();
    this.deletionRetryRegistry = RetryRegistry.of(deletionRetryConfig);
    // Initialize Retry instances using shared configs
    this.healthCheckRetry = Retry.of("health-check", RetryConfigUtils.HEALTH_CHECK);
    this.settingsUpdateRetry = Retry.of("settings-update", RetryConfigUtils.SETTINGS_UPDATE);
    this.taskStatusRetry = Retry.of("task-status", RetryConfigUtils.TASK_STATUS);
    this.reindexSubmissionRetry = Retry.of("reindex-submission", RetryConfigUtils.COST_ESTIMATION);
    jvminfo = new OpenSearchJvmInfo(this.searchClient);
  }

  /** Creates a predicate to determine which Elasticsearch exceptions should be retried */
  private static Predicate<Throwable> createElasticsearchRetryPredicate() {
    return throwable -> {
      // Retry on connection issues
      if (throwable.getMessage() != null
          && (throwable.getMessage().contains("snapshotted")
              || throwable.getMessage().contains("Connection refused")
              || throwable.getMessage().contains("Connection reset")
              || throwable.getMessage().contains("Connection closed")
              || throwable.getMessage().contains("timeout")
              || throwable.getMessage().contains("temporarily unavailable"))) {
        return true;
      }
      // Retry on specific Elasticsearch errors that might be transient
      // Add any specific Elasticsearch error codes or messages that should be retried
      // Don't retry if the exception doesn't match any of the criteria
      return false;
    };
  }

  /**
   * Utility function to check if the connected server is OpenSearch 2.9 or higher. Returns false if
   * the server is Elasticsearch or OpenSearch below version 2.9.
   *
   * @return true if the server is running OpenSearch 2.9 or higher, false otherwise
   * @throws IOException if there's an error communicating with the server
   */
  @VisibleForTesting
  public boolean isOpenSearch29OrHigher(@Nonnull OperationContext opContext) throws IOException {
    try {
      // We need to use the low-level client to get version information
      RawResponse response =
          searchClient.performLowLevelRequest(opContext, new Request("GET", "/"));
      Map<String, Object> responseMap =
          ObjectMapperContext.defaultMapper.readValue(response.getEntity().getContent(), Map.class);
      // Check if this is Elasticsearch: "You Know, for Search"
      String tagline = (String) responseMap.get("tagline");
      if (tagline.toLowerCase().contains("you know")) {
        return false;
      }
      // Get the version information
      Map<String, Object> versionInfo = (Map<String, Object>) responseMap.get("version");
      String versionString = (String) versionInfo.get("number");
      // Parse the version string
      String[] versionParts = versionString.split("\\.");
      if (versionParts.length < 2) {
        throw new IOException("Invalid version format: " + versionString);
      }
      int majorVersion = Integer.parseInt(versionParts[0]);
      int minorVersion = Integer.parseInt(versionParts[1]);
      // Return true if version is OpenSearch 2.9 or higher
      return majorVersion > 2 || (majorVersion == 2 && minorVersion >= 9);
    } catch (Exception e) {
      // return defensive false
      return false;
    }
  }

  public List<ReindexConfig> buildReindexConfigs(
      @Nonnull OperationContext opContext,
      @Nonnull SettingsBuilder settingsBuilder,
      @Nonnull MappingsBuilder mappingsBuilder,
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    Collection<MappingsBuilder.IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappings(opContext, properties);

    return indexMappings.stream()
        .map(
            indexMap -> {
              try {
                // Get settings for this specific index
                Map<String, Object> settings =
                    settingsBuilder.getSettings(indexConfig, indexMap.getIndexName());
                return buildReindexState(
                    opContext, indexMap.getIndexName(), indexMap.getMappings(), settings);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }

  public List<ReindexConfig> buildReindexConfigsWithNewStructProp(
      @Nonnull OperationContext opContext,
      @Nonnull SettingsBuilder settingsBuilder,
      @Nonnull MappingsBuilder mappingsBuilder,
      Urn urn,
      StructuredPropertyDefinition property) {
    Collection<MappingsBuilder.IndexMapping> indexMappings =
        mappingsBuilder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);

    return indexMappings.stream()
        .map(
            indexMap -> {
              try {
                // Get settings for this specific index
                Map<String, Object> settings =
                    settingsBuilder.getSettings(indexConfig, indexMap.getIndexName());
                return buildReindexState(
                    opContext, indexMap.getIndexName(), indexMap.getMappings(), settings, true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .filter(Objects::nonNull)
        .filter(ReindexConfig::hasNewStructuredProperty)
        .collect(Collectors.toList());
  }

  public ReindexConfig buildReindexState(
      @Nonnull OperationContext opContext,
      String indexName,
      Map<String, Object> mappings,
      Map<String, Object> settings)
      throws IOException {
    return buildReindexState(opContext, indexName, mappings, settings, false);
  }

  public ReindexConfig buildReindexState(
      @Nonnull OperationContext opContext,
      String indexName,
      Map<String, Object> mappings,
      Map<String, Object> settings,
      boolean copyStructuredPropertyMappings)
      throws IOException {
    ReindexConfig.ReindexConfigBuilder builder =
        ReindexConfig.builder()
            .name(indexName)
            .enableIndexSettingsReindex(indexConfig.isEnableSettingsReindex())
            .enableIndexMappingsReindex(indexConfig.isEnableMappingsReindex())
            .enableStructuredPropertiesReindex(
                structPropConfig.isEnabled()
                    && structPropConfig.isSystemUpdateEnabled()
                    && !copyStructuredPropertyMappings)
            .version(gitVersion.getVersion())
            .settingsComparisonShim(searchClient);

    Map<String, Object> baseSettings = new HashMap<>(settings);
    baseSettings.put(NUMBER_OF_SHARDS, indexConfig.getNumShards());
    baseSettings.put(NUMBER_OF_REPLICAS, indexConfig.getNumReplicas());
    baseSettings.put(
        REFRESH_INTERVAL, String.format("%ss", indexConfig.getRefreshIntervalSeconds()));
    // Use zstd in OS only and only if KNN is not enabled (codec settings conflict with KNN)
    // In ES we can use it in the future with best_compression
    if (isOpenSearch29OrHigher(opContext) && !isKnnEnabled(baseSettings)) {
      baseSettings.put("codec", "zstd_no_dict");
    }
    baseSettings.putAll(indexSettingOverrides.getOrDefault(indexName, Map.of()));
    Map<String, Object> targetSetting = ImmutableMap.of("index", baseSettings);
    builder.targetSettings(targetSetting);

    // Check if index exists
    boolean exists =
        searchClient.indexExists(opContext, new GetIndexRequest(indexName), requestOptionsLong);
    builder.exists(exists);

    // If index doesn't exist, no reindex
    if (!exists) {
      builder.targetMappings(mappings);
      return builder.build();
    }

    Settings currentSettings =
        searchClient
            .getIndexSettings(
                opContext, new GetSettingsRequest().indices(indexName), requestOptionsLong)
            .getIndexToSettings()
            .values()
            .iterator()
            .next();
    builder.currentSettings(currentSettings);

    Map<String, Object> currentMappings =
        searchClient
            .getIndexMapping(
                opContext, new GetMappingsRequest().indices(indexName), requestOptionsLong)
            .mappings()
            .values()
            .stream()
            .findFirst()
            .get()
            .getSourceAsMap();
    builder.currentMappings(currentMappings);

    if (copyStructuredPropertyMappings) {
      mergeStructuredPropertyMappings(mappings, currentMappings);
    }

    builder.targetMappings(mappings);
    return builder.build();
  }

  private static boolean isKnnEnabled(Map<String, Object> baseSettings) {
    return baseSettings.get("knn") == Boolean.TRUE;
  }

  @SuppressWarnings("unchecked")
  private void mergeStructuredPropertyMappings(
      Map<String, Object> targetMappings, Map<String, Object> currentMappings) {
    // Extract current structured property mapping (entire object, not just properties)
    Map<String, Object> currentStructuredPropertyMapping =
        (Map<String, Object>)
            Optional.ofNullable(currentMappings.get(PROPERTIES))
                .map(props -> ((Map<String, Object>) props).get(STRUCTURED_PROPERTY_MAPPING_FIELD))
                .orElse(new HashMap<>());

    if (currentStructuredPropertyMapping.isEmpty()) {
      return;
    }

    // Get or create target structured property mapping
    Map<String, Object> targetProperties =
        (Map<String, Object>)
            Optional.ofNullable(targetMappings.get(PROPERTIES)).orElse(new HashMap<>());

    Map<String, Object> targetStructuredPropertyMapping =
        (Map<String, Object>)
            targetProperties.computeIfAbsent(
                STRUCTURED_PROPERTY_MAPPING_FIELD, k -> new HashMap<>());

    // Merge top-level fields from current mapping (type, dynamic, etc.)
    mergeTopLevelFields(targetStructuredPropertyMapping, currentStructuredPropertyMapping);

    // Merge properties separately to handle nested field conflicts properly
    mergeStructuredProperties(targetStructuredPropertyMapping, currentStructuredPropertyMapping);
  }

  @SuppressWarnings("unchecked")
  // ES8+ includes additional top level fields that we don't want to wipe out or detect as
  // differences
  private void mergeTopLevelFields(Map<String, Object> target, Map<String, Object> current) {
    current.entrySet().stream()
        .filter(entry -> !PROPERTIES.equals(entry.getKey())) // Skip properties field
        .forEach(entry -> target.putIfAbsent(entry.getKey(), entry.getValue()));
  }

  @SuppressWarnings("unchecked")
  private void mergeStructuredProperties(Map<String, Object> target, Map<String, Object> current) {
    Map<String, Object> currentProperties =
        (Map<String, Object>) current.getOrDefault(PROPERTIES, new HashMap<>());

    if (currentProperties.isEmpty()) {
      return;
    }

    Map<String, Object> targetProperties =
        (Map<String, Object>) target.computeIfAbsent(PROPERTIES, k -> new HashMap<>());

    // Merge properties - current properties take precedence over target for conflicts
    Map<String, Object> mergedProperties = new HashMap<>(targetProperties);
    mergedProperties.putAll(currentProperties);

    target.put(PROPERTIES, mergedProperties);
  }

  public ReindexResult buildIndex(@Nonnull OperationContext opContext, ReindexConfig indexState)
      throws IOException {
    ReindexResult result;
    // If index doesn't exist, create index
    if (!indexState.exists()) {
      createIndex(opContext, indexState.name(), indexState);
      result = ReindexResult.CREATED_NEW;
      return result;
    }
    log.info("Current mappings for index {}", indexState.name());
    log.info("{}", indexState.currentMappings());
    log.info("Target mappings for index {}", indexState.name());
    log.info("{}", indexState.targetMappings());

    // If there are no updates to mappings and settings, return
    if (!indexState.requiresApplyMappings() && !indexState.requiresApplySettings()) {
      log.info("No updates to index {}", indexState.name());
      result = ReindexResult.NOT_REINDEXED_NOTHING_APPLIED;
      return result;
    }

    if (!indexState.requiresReindex()) {
      // no need to reindex and only new mappings or dynamic settings
      result = ReindexResult.NOT_REQUIRED_MAPPINGS_SETTINGS_APPLIED;

      // Just update the additional mappings
      applyMappings(opContext, indexState, true);

      if (indexState.requiresApplySettings()) {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexState.name());
        Map<String, Object> indexSettings =
            ((Map<String, Object>) indexState.targetSettings().get("index"))
                .entrySet().stream()
                    .filter(e -> ReindexConfig.SETTINGS_DYNAMIC.contains(e.getKey()))
                    .collect(Collectors.toMap(e -> "index." + e.getKey(), Map.Entry::getValue));
        request.settings(indexSettings);

        boolean ack =
            searchClient
                .updateIndexSettings(opContext, request, requestOptionsLong)
                .isAcknowledged();
        log.info(
            "Updated index {} with new settings. Settings: {}, Acknowledged: {}",
            indexState.name(),
            ReindexConfig.OBJECT_MAPPER.writeValueAsString(indexSettings),
            ack);
      }
    } else {
      try {
        result = reindex(opContext, indexState);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  /**
   * Check if a specific index has 0 replicas and >0 documents, then increase its replica count to
   * 1.
   *
   * @param indexName The name of the index to check
   * @param dryRun If true, report what would happen without making changes
   * @return Map containing operation details
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  private Map<String, Object> increaseReplicasForActiveIndices(
      @Nonnull OperationContext opContext, String indexName, boolean dryRun) throws IOException {
    Map<String, Object> result = new HashMap<>();
    result.put("indexName", indexName);
    result.put("changed", false);
    result.put("dryRun", dryRun);
    GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
    GetIndexResponse response =
        searchClient.getIndex(opContext, getIndexRequest, requestOptionsLong);
    Map<String, Settings> indexToSettings = response.getSettings();
    Optional<String> key = indexToSettings.keySet().stream().findFirst();
    String thekey = key.get();
    Settings indexSettings = indexToSettings.get(thekey);
    int replicaCount = Integer.parseInt(indexSettings.get("index.number_of_replicas", "1"));
    CountRequest countRequest = new CountRequest(indexName);
    CountResponse countResponse = searchClient.count(opContext, countRequest, requestOptionsLong);
    long docCount = countResponse.getCount();
    result.put("currentReplicas", replicaCount);
    result.put("documentCount", docCount);
    // Check if index has 0 replicas and >0 documents
    if (replicaCount == 0 && docCount > 0) {
      if (!dryRun) {
        // Update replica count to X
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);
        Settings.Builder settingsBuilder =
            Settings.builder().put("index.number_of_replicas", indexConfig.getNumReplicas());
        updateSettingsRequest.settings(settingsBuilder);
        searchClient.updateIndexSettings(opContext, updateSettingsRequest, requestOptionsLong);
      }
      result.put("changed", true);
      result.put("action", "Increase replicas from 0 to " + indexConfig.getNumReplicas());
    } else {
      result.put("action", "No change needed");
    }
    return result;
  }

  /**
   * Check if a specific index has 0 documents and replicas > 0, then set its replica count to 0.
   *
   * @param indexName The name of the index to check
   * @param dryRun If true, report what would happen without making changes
   * @return Map containing operation details
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  private Map<String, Object> reduceReplicasForEmptyIndices(
      @Nonnull OperationContext opContext, String indexName, boolean dryRun) throws IOException {
    Map<String, Object> result = new HashMap<>();
    result.put("indexName", indexName);
    result.put("changed", false);
    result.put("dryRun", dryRun);
    GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
    if (!searchClient.indexExists(opContext, getIndexRequest, requestOptionsLong)) {
      result.put("skipped", true);
      result.put("reason", "Index does not exist");
      return result;
    }
    GetIndexResponse response =
        searchClient.getIndex(opContext, getIndexRequest, requestOptionsLong);
    Map<String, Settings> indexToSettings = response.getSettings();
    Optional<String> key = indexToSettings.keySet().stream().findFirst();
    String thekey = key.get();
    Settings indexSettings = indexToSettings.get(thekey);
    int replicaCount = Integer.parseInt(indexSettings.get("index.number_of_replicas", "1"));
    CountRequest countRequest = new CountRequest(indexName);
    CountResponse countResponse = searchClient.count(opContext, countRequest, requestOptionsLong);
    long docCount = countResponse.getCount();
    result.put("currentReplicas", replicaCount);
    result.put("documentCount", docCount);
    // Check if index has 0 documents and replicas > 0
    if (docCount == 0 && replicaCount > 0) {
      if (!dryRun) {
        // Set replica count to 0
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);
        Settings.Builder settingsBuilder = Settings.builder().put("index.number_of_replicas", 0);
        updateSettingsRequest.settings(settingsBuilder);
        searchClient.updateIndexSettings(opContext, updateSettingsRequest, requestOptionsLong);
      }
      result.put("changed", true);
      result.put("action", "Decrease replicas from " + replicaCount + " to 0");
    } else {
      result.put("action", "No change needed");
    }
    return result;
  }

  public String createOperationSummary(
      Map<String, Object> increaseResult, Map<String, Object> reduceResult) {
    StringBuilder summary = new StringBuilder();
    String indexName = (String) increaseResult.get("indexName");
    boolean dryRun = (Boolean) increaseResult.get("dryRun");
    summary
        .append("Index: ")
        .append(indexName)
        .append(" (")
        .append(dryRun ? "DRY RUN" : "LIVE")
        .append(")\n");
    // Document count info
    long docCount = (long) increaseResult.get("documentCount");
    summary
        .append("Status: ")
        .append(docCount > 0 ? "Active" : "Empty")
        .append(" (")
        .append(docCount)
        .append(" docs)\n");
    // Replica changes summary
    int currentReplicas = (int) increaseResult.get("currentReplicas");
    summary.append("Replicas: ").append(currentReplicas).append(" → ");
    boolean increased =
        increaseResult.containsKey("changed") && (Boolean) increaseResult.get("changed");
    boolean reduced = reduceResult.containsKey("changed") && (Boolean) reduceResult.get("changed");
    if (increased) {
      summary.append("" + indexConfig.getNumReplicas() + " (increased)");
    } else if (reduced) {
      summary.append("0 (reduced)");
    } else {
      summary.append(currentReplicas).append(" (unchanged)");
    }
    // Add reason if no change
    if (!increased && !reduced) {
      if (docCount > 0 && currentReplicas > 0) {
        summary.append(" - already optimized");
      } else if (docCount == 0 && currentReplicas == 0) {
        summary.append(" - already optimized");
      }
    }
    return summary.toString();
  }

  public void tweakReplicas(
      @Nonnull OperationContext opContext, ReindexConfig indexState, boolean dryRun)
      throws IOException {
    Map<String, Object> result =
        increaseReplicasForActiveIndices(opContext, indexState.name(), dryRun);
    Map<String, Object> resultb =
        reduceReplicasForEmptyIndices(opContext, indexState.name(), dryRun);
    log.info(
        "Tweaked replicas index {}: {}",
        indexState.name(),
        createOperationSummary(result, resultb));
  }

  /**
   * Apply mappings changes if reindex is not required
   *
   * @param indexState the state of the current and target index settings/mappings
   * @param suppressError during reindex logic this is not an error, for structured properties it is
   *     an error
   * @throws IOException communication issues with ES
   */
  public void applyMappings(
      @Nonnull OperationContext opContext, ReindexConfig indexState, boolean suppressError)
      throws IOException {
    if (indexState.isPureMappingsAddition() || indexState.isPureStructuredPropertyAddition()) {
      log.info("Updating index {} mappings in place.", indexState.name());
      PutMappingRequest request =
          new PutMappingRequest(indexState.name()).source(indexState.targetMappings());
      searchClient.putIndexMapping(opContext, request, requestOptionsLong);
      log.info("Updated index {} with new mappings", indexState.name());
    } else {
      if (!suppressError) {
        log.error(
            "Attempted to apply invalid mappings. Current: {} Target: {}",
            indexState.currentMappings(),
            indexState.targetMappings());
      }
    }
  }

  public String reindexInPlaceAsync(
      @Nonnull OperationContext opContext,
      String indexAlias,
      @Nullable QueryBuilder filterQuery,
      BatchWriteOperationsOptions options,
      ReindexConfig config)
      throws Exception {
    GetAliasesResponse aliasesResponse =
        searchClient.getIndexAliases(
            opContext, new GetAliasesRequest(indexAlias), requestOptionsLong);
    if (aliasesResponse.getAliases().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Input to reindexInPlaceAsync should be an alias. %s is not", indexAlias));
    }

    // Point alias at new index
    String nextIndexName = getNextIndexName(indexAlias, System.currentTimeMillis());
    createIndex(opContext, nextIndexName, config);
    renameReindexedIndices(
        searchClient, opContext, indexAlias, null, nextIndexName, false, requestOptionsLong);
    int targetShards = extractTargetShards(config);

    Map<String, Object> reinfo =
        submitReindex(
            opContext,
            aliasesResponse.getAliases().keySet().toArray(new String[0]),
            nextIndexName,
            options.getBatchSize(),
            TimeValue.timeValueSeconds(options.getTimeoutSeconds()),
            filterQuery,
            targetShards);
    return (String) reinfo.get("taskId");
  }

  /**
   * Generate a unique index name based on timestamp. Used for creating temporary indices during
   * reindex operations.
   *
   * @param base The base index name
   * @param startTime The timestamp to use for uniqueness
   * @return A unique index name in the format base_timestamp
   */
  public static String getNextIndexName(String base, long startTime) {
    return base + "_" + startTime;
  }

  /**
   * Generates the name for a 'next' index used during incremental reindex. Uses a {@code _next_}
   * infix to distinguish from the standard {@code _<timestamp>} pattern used by in-place reindex.
   */
  /**
   * Generates the name for a 'next' index used during incremental reindex. Includes the upgrade
   * version so the backing index is identifiable after alias swap, and a timestamp for uniqueness.
   *
   * <p>ES index names cannot contain dots, so dots in the version are replaced with underscores.
   *
   * @param base the current index/alias name (e.g. "datasetindex_v2")
   * @param upgradeVersion the code version (e.g. "0.13.1-0")
   * @param startTime epoch millis for uniqueness
   */
  public static String getIncrementalNextIndexName(
      String base, String upgradeVersion, long startTime) {
    String sanitizedVersion = upgradeVersion.replace('.', '_');
    return base + "_" + sanitizedVersion + "_" + startTime;
  }

  /** Result of initiating an incremental reindex (Phase 1). */
  public record IncrementalReindexResult(
      String nextIndexName,
      long reindexStartTime,
      String taskId,
      boolean skippedEmpty,
      int targetShards,
      long sourceDocCount,
      Map<String, Object> reindexInfo) {}

  /**
   * Creates a 'next' index with the target mappings/settings and submits an async ES _reindex from
   * the current index, without blocking writes on the current index or swapping the alias.
   *
   * <p>This is Phase 1 of incremental reindex. The caller is responsible for persisting the
   * returned state (next index name, start time) and later triggering alias swap. After polling
   * completes, the caller should call {@link #undoReindexOptimalSettings} to restore normal index
   * settings.
   *
   * @param indexState the reindex config describing current vs target state
   * @param upgradeVersion the code version string for index naming (e.g. "0.13.1-0")
   * @return info about the created next index, including reindex metadata needed for polling
   */
  public IncrementalReindexResult buildIndexIncremental(
      @Nonnull OperationContext opContext, ReindexConfig indexState, String upgradeVersion)
      throws Throwable {
    final long startTime = System.currentTimeMillis();
    String nextIndexName =
        getIncrementalNextIndexName(indexState.name(), upgradeVersion, startTime);
    int targetShards = extractTargetShards(indexState);

    createIndex(opContext, nextIndexName, indexState);

    long sourceDocCount = getSourceDocCount(opContext, indexState.name());
    if (sourceDocCount == 0) {
      log.info(
          "Incremental reindex: skipping _reindex for {} -> {} (0 docs in source)",
          indexState.name(),
          nextIndexName);
      return new IncrementalReindexResult(
          nextIndexName, startTime, null, true, targetShards, 0, Map.of());
    }

    Map<String, Object> reindexInfo =
        submitReindex(
            opContext,
            new String[] {indexState.name()},
            nextIndexName,
            getReindexBatchSize(),
            null,
            null,
            targetShards);
    String taskId = (String) reindexInfo.get("taskId");

    log.info(
        "Incremental reindex: submitted _reindex task {} from {} to {} (source docs: {})",
        taskId,
        indexState.name(),
        nextIndexName,
        sourceDocCount);

    return new IncrementalReindexResult(
        nextIndexName, startTime, taskId, false, targetShards, sourceDocCount, reindexInfo);
  }

  /**
   * Restores index settings that were optimized for reindex (replicas, refresh interval, translog).
   * Should be called after {@link #pollReindexCompletion} succeeds.
   */
  public void undoReindexOptimalSettings(
      @Nonnull OperationContext opContext,
      String indexName,
      ReindexConfig indexState,
      Map<String, Object> reindexInfo)
      throws IOException {
    String targetReplicas =
        String.valueOf(((Map) indexState.targetSettings().get("index")).get(NUMBER_OF_REPLICAS));
    String targetRefresh =
        String.valueOf(((Map) indexState.targetSettings().get("index")).get(REFRESH_INTERVAL));
    setReindexOptimalSettingsUndo(opContext, indexName, targetReplicas, targetRefresh, reindexInfo);
  }

  /** Result of polling a reindex to completion. */
  public record PollReindexResult(
      boolean completed,
      Map<String, Object> latestReindexInfo,
      Pair<Long, Long> finalDocumentCounts) {}

  /**
   * Polls an in-progress reindex until document counts converge or timeout. Includes stall
   * detection with automatic reindex re-submission and progress estimation.
   *
   * <p>The {@code expectedCountSupplier} controls how the expected document count is resolved each
   * poll iteration. For the legacy blocked-writes path, pass {@code () -> getCount(sourceIndex)} to
   * re-query the live source (stable since writes are blocked). For incremental reindex where
   * writes continue to the source, pass a fixed snapshot: {@code () -> snapshotCount}.
   *
   * @param sourceIndex the source index name (for logging and stall-retry resubmission)
   * @param destIndex the destination index name
   * @param expectedCountSupplier supplies the expected doc count; called each poll iteration
   * @param targetShards target shard count (needed if re-submitting reindex on stall)
   * @param reindexInfo mutable reindex info map from {@code submitReindex}; updated on stall-retry
   * @param taskId ES task ID for log correlation (may be empty if resuming a previous task)
   * @return poll result containing completion status, latest reindex info, and final doc counts
   */
  public PollReindexResult pollReindexCompletion(
      @Nonnull OperationContext opContext,
      String sourceIndex,
      String destIndex,
      Callable<Long> expectedCountSupplier,
      int targetShards,
      Map<String, Object> reindexInfo,
      String taskId)
      throws Throwable {
    final long initialCheckIntervalMilli = 1000;
    final long finalCheckIntervalMilli = 60000;
    final long timeoutAt = computeTimeoutAt();

    Map<String, Object> latestReindexInfo = new HashMap<>(reindexInfo);
    int reindexCount = 1;
    int count = 0;
    Pair<Long, Long> documentCounts =
        getDocumentCounts(opContext, expectedCountSupplier, destIndex);
    long documentCountsLastUpdated = System.currentTimeMillis();
    final long pollStartTimeMillis = documentCountsLastUpdated;
    final long pollStartDocCount = documentCounts.getSecond();
    long estimatedMinutesRemaining = 0;

    while (System.currentTimeMillis() < timeoutAt) {
      log.info(
          "Task: {} - Reindexing from {} to {} in progress...", taskId, sourceIndex, destIndex);

      Pair<Long, Long> latestCounts =
          getDocumentCounts(opContext, expectedCountSupplier, destIndex);
      long currentTime = System.currentTimeMillis();

      if (!latestCounts.equals(documentCounts)) {
        // Stall-detection bookkeeping only; the ETA below is computed unconditionally.
        documentCountsLastUpdated = currentTime;
        documentCounts = latestCounts;
      }

      estimatedMinutesRemaining =
          estimateMinutesRemaining(
              latestCounts.getSecond() - pollStartDocCount,
              currentTime - pollStartTimeMillis,
              latestCounts.getFirst() - latestCounts.getSecond());

      if (documentCounts.getFirst().equals(documentCounts.getSecond())) {
        log.info(
            "Reindex {} -> {} complete. Doc count: {}",
            sourceIndex,
            destIndex,
            documentCounts.getFirst());
        return new PollReindexResult(true, latestReindexInfo, documentCounts);
      }

      float progressPercentage =
          documentCounts.getFirst() > 0
              ? (100 * (1.0f * documentCounts.getSecond())) / documentCounts.getFirst()
              : 0;
      log.warn(
          "Document counts do not match {} != {}. Complete: {}%. Estimated time remaining: {} minutes",
          documentCounts.getFirst(),
          documentCounts.getSecond(),
          progressPercentage,
          estimatedMinutesRemaining);

      // Stall detection: re-trigger reindex if no progress
      long lastUpdateDelta = System.currentTimeMillis() - documentCountsLastUpdated;
      int noProgressRetryMinutes = getReindexNoProgressRetryMinutes();
      if (lastUpdateDelta > (noProgressRetryMinutes * 60L * 1000)) {
        if (reindexCount <= indexConfig.getNumRetries()) {
          log.warn(
              "No change in index count after {} minutes, re-triggering reindex #{}.",
              noProgressRetryMinutes,
              reindexCount);
          latestReindexInfo =
              submitReindex(
                  opContext,
                  new String[] {sourceIndex},
                  destIndex,
                  getReindexBatchSize(),
                  null,
                  null,
                  targetShards);
          reindexCount++;
          documentCountsLastUpdated = System.currentTimeMillis();
        } else {
          log.warn("Reindex retry timeout for {}.", sourceIndex);
          break;
        }
      }

      count++;
      Thread.sleep(Math.min(finalCheckIntervalMilli, initialCheckIntervalMilli * count));
    }

    log.warn("Reindex {} -> {} timed out or exhausted retries", sourceIndex, destIndex);
    return new PollReindexResult(false, latestReindexInfo, documentCounts);
  }

  // --- Shared helper methods used by both legacy reindex() and incremental path ---

  /**
   * Submits an async ES _reindex from source to destination with an optional filter query. Does not
   * swap aliases or block writes. Useful for copying a subset of documents (e.g. a time range) from
   * one index to another.
   *
   * @return the ES task ID for the submitted reindex
   */
  public String submitFilteredReindex(
      @Nonnull OperationContext opContext,
      @Nonnull String sourceIndex,
      @Nonnull String destIndex,
      @Nullable QueryBuilder filterQuery,
      int targetShards)
      throws IOException {
    Map<String, Object> reinfo =
        submitReindex(
            opContext,
            new String[] {sourceIndex},
            destIndex,
            getReindexBatchSize(),
            null,
            filterQuery,
            targetShards);
    return (String) reinfo.get("taskId");
  }

  /**
   * Extract target shard count from a ReindexConfig's target settings. Handles both the nested
   * structure from {@code buildReindexConfig} ({@code {"index": {"number_of_shards": N}}}) and the
   * flat structure ({@code {"number_of_shards": N}}).
   */
  public static int extractTargetShards(ReindexConfig indexState) {
    Map<String, Object> settings = indexState.targetSettings();

    // Try nested: {"index": {"number_of_shards": N}}
    Optional<Integer> nested =
        Optional.ofNullable(settings.get("index"))
            .filter(Map.class::isInstance)
            .map(Map.class::cast)
            .map(indexMap -> indexMap.get(NUMBER_OF_SHARDS))
            .map(Object::toString)
            .map(Integer::parseInt);
    if (nested.isPresent()) {
      return nested.get();
    }

    // Try flat: {"number_of_shards": N}
    return Optional.ofNullable(settings.get(NUMBER_OF_SHARDS))
        .map(Object::toString)
        .map(Integer::parseInt)
        .orElseThrow(() -> new IllegalArgumentException("Number of shards not specified"));
  }

  /** Get doc count for a source index with retry. */
  public long getSourceDocCount(@Nonnull OperationContext opContext, String indexName)
      throws Throwable {
    return retryRegistry
        .retry("retryCurDocCount", "countRetry")
        .executeCheckedSupplier(() -> getCount(opContext, indexName));
  }

  /** Compute the timeout timestamp based on maxReindexHours config. */
  public long computeTimeoutAt() {
    return indexConfig.getMaxReindexHours() > 0
        ? System.currentTimeMillis() + (1000L * 60 * 60 * indexConfig.getMaxReindexHours())
        : Long.MAX_VALUE;
  }

  /**
   * Computes estimated minutes remaining for a reindex based on the cumulative average indexing
   * rate since the start of the current polling loop (not the most recent poll-to-poll delta). A
   * cumulative average smooths out bursty ES bulk-indexing throughput and the polling loop's own
   * growing sleep interval, both of which make a single-sample rate estimate very noisy.
   */
  public static long estimateMinutesRemaining(
      long docsIndexedSinceStart, long elapsedMillisSinceStart, long remainingDocs) {
    double indexingRate =
        elapsedMillisSinceStart > 0 ? (double) docsIndexedSinceStart / elapsedMillisSinceStart : 0;
    long estimatedMillisRemaining = indexingRate > 0 ? (long) (remainingDocs / indexingRate) : 0;
    return estimatedMillisRemaining / (1000 * 60);
  }

  private ReindexResult reindex(@Nonnull OperationContext opContext, ReindexConfig indexState)
      throws Throwable {
    ReindexResult result;
    final long startTime = System.currentTimeMillis();

    String tempIndexName = getNextIndexName(indexState.name(), startTime);
    Map<String, Object> reinfo = new HashMap<>();
    try {
      Optional<TaskInfo> previousTaskInfo = getTaskInfoByHeader(opContext, indexState.name());

      int targetShards = extractTargetShards(indexState);
      String parentTaskId = "";
      boolean reindexTaskCompleted = false;

      // Guard against falsely resuming a reindex whose target mappings are now stale: a prior run
      // may have been building into an index created from an older mapping version, or the task may
      // have run amok. Resuming such a task would silently complete into a wrong-mapping index, so
      // cancel it (with confirmation) and fall through to a fresh reindex instead of adopting it.
      if (previousTaskInfo.isPresent()
          && !resumedReindexMatchesTarget(opContext, indexState, previousTaskInfo.get())) {
        String staleTemp =
            ESUtils.extractTargetIndex(
                previousTaskInfo.get().getHeaders().get(ESUtils.OPAQUE_ID_HEADER));
        log.warn(
            "Index: {} - resumed reindex into {} no longer matches target mappings; cancelling task"
                + " and restarting.",
            indexState.name(),
            staleTemp);
        // Must confirm the runaway task is actually stopped before deleting its target and
        // submitting a replacement - otherwise two reindexes would write the same index.
        cancelReindexTaskAndConfirm(opContext, previousTaskInfo.get());
        deleteActionWithRetry(searchClient, opContext, staleTemp, requestOptionsLong);
        previousTaskInfo = Optional.empty(); // force the fresh-reindex path below
      }

      if (previousTaskInfo.isPresent()) {
        log.info(
            "Reindex task {} in progress with description {}. Attempting to continue task from breakpoint.",
            previousTaskInfo.get().getTaskId(),
            previousTaskInfo.get().getDescription());
        parentTaskId = previousTaskInfo.get().getParentTaskId().toString();
        tempIndexName =
            ESUtils.extractTargetIndex(
                previousTaskInfo.get().getHeaders().get(ESUtils.OPAQUE_ID_HEADER));
        result = ReindexResult.REINDEXING_ALREADY;
      } else {
        // Create new index
        createIndex(opContext, tempIndexName, indexState);
        long curDocCount = getSourceDocCount(opContext, indexState.name());
        if (curDocCount == 0) {
          reindexTaskCompleted = true;
          result = ReindexResult.REINDEXED_SKIPPED_0DOCS;
          log.info(
              "Reindex skipped for {} -> {} due to 0 docs .", indexState.name(), tempIndexName);
        } else {
          reinfo =
              submitReindex(
                  opContext,
                  new String[] {indexState.name()},
                  tempIndexName,
                  getReindexBatchSize(),
                  null,
                  null,
                  targetShards);
          parentTaskId = (String) reinfo.get("taskId");
          result = ReindexResult.REINDEXING;
        }
      }

      if (!reindexTaskCompleted) {
        PollReindexResult pollResult =
            pollReindexCompletion(
                opContext,
                indexState.name(),
                tempIndexName,
                () -> getCount(opContext, indexState.name()),
                targetShards,
                reinfo,
                parentTaskId);
        reindexTaskCompleted = pollResult.completed();
        reinfo = pollResult.latestReindexInfo();
        Pair<Long, Long> documentCounts = pollResult.finalDocumentCounts();

        if (!reindexTaskCompleted) {
          if (config.getBuildIndices().isAllowDocCountMismatch()
              && config.getBuildIndices().isCloneIndices()) {
            log.warn(
                "Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}\n"
                    + "This condition is explicitly ALLOWED, please refer to latest clone if original index is required.",
                indexState.name(),
                documentCounts.getFirst(),
                documentCounts.getSecond());
          } else {
            log.error(
                "Index: {} - Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}",
                indexState.name(),
                documentCounts.getFirst(),
                documentCounts.getSecond());
            diff(
                opContext,
                indexState.name(),
                tempIndexName,
                Math.max(documentCounts.getFirst(), documentCounts.getSecond()));
            throw new RuntimeException(
                String.format(
                    "Reindex from %s to %s failed. Document count %s != %s",
                    indexState.name(),
                    tempIndexName,
                    documentCounts.getFirst(),
                    documentCounts.getSecond()));
          }
        }
      }
    } catch (Throwable e) {
      log.error(
          "Failed to reindex {} to {}: Exception {}",
          indexState.name(),
          tempIndexName,
          e.toString());
      deleteActionWithRetry(searchClient, opContext, tempIndexName, requestOptionsLong);
      throw e;
    }

    log.info("Reindex from {} to {} succeeded", indexState.name(), tempIndexName);
    String targetReplicas =
        String.valueOf(((Map) indexState.targetSettings().get("index")).get(NUMBER_OF_REPLICAS));
    String targetRefresh =
        String.valueOf(((Map) indexState.targetSettings().get("index")).get(REFRESH_INTERVAL));
    if (result != ReindexResult.REINDEXED_SKIPPED_0DOCS) {
      setReindexOptimalSettingsUndo(
          opContext, tempIndexName, targetReplicas, targetRefresh, reinfo);
    }
    renameReindexedIndices(
        searchClient,
        opContext,
        indexState.name(),
        indexState.indexPattern(),
        tempIndexName,
        true,
        requestOptionsLong);
    log.info("Finished setting up {}", indexState.name());
    return result;
  }

  public void deleteActionWithRetry(@Nonnull OperationContext opContext, String tempIndexName)
      throws Exception {
    deleteActionWithRetry(searchClient, opContext, tempIndexName, requestOptionsLong);
  }

  /**
   * Delete Elasticsearch index with exponential backoff retry using resilience4j
   *
   * @param searchClient
   * @param tempIndexName Index name to delete
   * @throws Exception If deletion ultimately fails after all retries
   */
  private static void deleteActionWithRetry(
      SearchClientShim<?> searchClient,
      @Nonnull OperationContext opContext,
      String tempIndexName,
      RequestOptions requestOptions)
      throws Exception {
    Retry retry = deletionRetryRegistry.retry("elasticsearchDeleteIndex");
    Callable<Void> deleteOperation =
        () -> {
          try {
            log.info("Attempting to delete index: {}", tempIndexName);
            // Check if index exists before deleting
            boolean indexExists =
                searchClient.indexExists(
                    opContext, new GetIndexRequest(tempIndexName), requestOptions);
            if (indexExists) {
              // Configure delete request with timeout
              DeleteIndexRequest request = new DeleteIndexRequest(tempIndexName);
              request.timeout(TimeValue.timeValueSeconds(30));
              // Execute delete
              searchClient.deleteIndex(opContext, request, requestOptions);
              log.info("Successfully deleted index: {}", tempIndexName);
            } else {
              log.info("Index {} does not exist, no need to delete", tempIndexName);
            }
            return null;
          } catch (Exception e) {
            log.warn("Failed to delete index: {}, error: {}", tempIndexName, e.getMessage());
            throw e;
          }
        };
    executeWithRetry(tempIndexName, retry, deleteOperation);
  }

  private static void updateAliasWithRetry(
      SearchClientShim<?> searchClient,
      @Nonnull OperationContext opContext,
      AliasActions removeAction,
      AliasActions addAction,
      String tempIndexName,
      RequestOptions requestOptions)
      throws Exception {
    Retry retry = deletionRetryRegistry.retry("elasticsearchDeleteIndex");
    Callable<Void> deleteOperation =
        () -> {
          try {
            log.info("Attempting to delete index/indices(behind alias): {}", tempIndexName);
            // Configure delete request with timeout
            IndicesAliasesRequest request;
            request =
                new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction);
            request.timeout(TimeValue.timeValueSeconds(30));
            // Execute delete
            searchClient.updateIndexAliases(opContext, request, requestOptions);
            log.info("Successfully deleted index/indices(behind alias): {}", tempIndexName);
            return null;
          } catch (Exception e) {
            log.warn(
                "Failed to deleted index/indices(behind alias): {}, error: {}",
                tempIndexName,
                e.getMessage());
            throw e;
          }
        };
    executeWithRetry(tempIndexName, retry, deleteOperation);
  }

  private static void executeWithRetry(String tempIndexName, Retry retry, Callable<Void> operation)
      throws Exception {
    // Execute with retry
    try {
      retry.executeCallable(operation);
    } catch (Exception e) {
      log.error(
          "Failed to delete index {} after {} attempts: {}",
          tempIndexName,
          deleteMaxAttempts,
          e.getMessage());
      throw e;
    }
  }

  private int getReindexBatchSize() {
    return Objects.requireNonNull(
        config.getBuildIndices().getReindexBatchSize(),
        "elasticsearch.buildIndices.reindexBatchSize must be set (e.g. in application.yaml)");
  }

  private int getReindexNoProgressRetryMinutes() {
    return Objects.requireNonNull(
        config.getBuildIndices().getReindexNoProgressRetryMinutes(),
        "elasticsearch.buildIndices.reindexNoProgressRetryMinutes must be set (e.g. in application.yaml)");
  }

  private int calculateOptimalSlices(int targetShards) {
    int maxSlices =
        Objects.requireNonNull(
            config.getBuildIndices().getReindexMaxSlices(),
            "elasticsearch.buildIndices.reindexMaxSlices must be set (e.g. in application.yaml)");
    return Math.min(maxSlices, targetShards);
  }

  private Map<String, Object> setReindexOptimalSettings(
      @Nonnull OperationContext opContext, String tempIndexName, int targetShards)
      throws IOException {
    Map<String, Object> res = new HashMap<>();
    // When reindex optimization is disabled, skip all settings writes and the cluster-level
    // /_nodes/stats heap query. The latter fails in reduced-permission deployments (e.g.
    // non-blocking system upgrades) and would otherwise abort the reindex.
    if (!config.getBuildIndices().isReindexOptimizationEnabled()) {
      res.put("optimalSlices", calculateOptimalSlices(targetShards));
      return res;
    }
    setIndexSetting(opContext, tempIndexName, "0", INDEX_NUMBER_OF_REPLICAS);
    setIndexSetting(opContext, tempIndexName, "-1", INDEX_REFRESH_INTERVAL);
    // flush_threshold_size: 512MB by def. Increasing to 1gb, if heap at least 16gb (this is more
    // conservative than %25 mentioned
    // https://docs.opensearch.org/docs/2.11/tuning-your-cluster/performance/
    //    "index.translog.flush_threshold_size": "512mb",
    // The heap query hits the cluster-level /_nodes/stats endpoint which may fail in
    // reduced-permission deployments. Guard the whole flush_threshold optimization so a
    // failure here only skips this tuning rather than aborting the reindex.
    try {
      double jvmheapgb = jvminfo.getAverageDataNodeMaxHeapSizeGB(opContext);
      String setting = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE;
      String optimValue = "1024mb";
      String curval = getIndexSetting(opContext, tempIndexName, setting);
      if (SizeUtils.isGreaterSize(optimValue, curval) && jvmheapgb >= MINJVMHEAP) {
        setIndexSetting(opContext, tempIndexName, optimValue, setting);
        res.put(ORIGINALPREFIX + setting, curval);
      }
    } catch (Exception e) {
      log.warn(
          "Failed to optimize translog.flush_threshold_size for {}, keeping current value: {}",
          tempIndexName,
          e.getMessage());
    }
    // this is a cluster setting "index_buffer_size": "10%",
    // GET _cluster/settings?include_defaults&filter_path=defaults.indices.memory
    //    setting = INDICES_MEMORY_INDEX_BUFFER_SIZE;
    //    optimValue = "25%";
    //    curval = getClusterSetting(setting);
    //    if (SizeUtils.isGreaterPercent(optimValue, curval) && jvmheapgb >= MINJVMHEAP) {
    //      setClusterSettings(setting, optimValue);
    //      res.put(ORIGINALPREFIX + setting, curval);
    //    }
    // calculate best slices number..., by def == primary shards
    int slices = calculateOptimalSlices(targetShards);
    res.put("optimalSlices", slices);
    return res;
  }

  private void setReindexOptimalSettingsUndo(
      @Nonnull OperationContext opContext,
      String tempIndexName,
      String targetReplicas,
      String refreshinterval,
      Map<String, Object> reinfo)
      throws IOException {
    // Symmetric with setReindexOptimalSettings: if optimization was disabled, nothing was
    // changed, so there is nothing to restore.
    if (!config.getBuildIndices().isReindexOptimizationEnabled()) {
      return;
    }
    // set the original values
    setIndexSetting(opContext, tempIndexName, targetReplicas, INDEX_NUMBER_OF_REPLICAS);
    setIndexSetting(opContext, tempIndexName, refreshinterval, INDEX_REFRESH_INTERVAL);
    // Restore translog settings if they were saved (may be empty if not originally set)
    String setting = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE;
    if (reinfo.containsKey(ORIGINALPREFIX + setting)) {
      setIndexSetting(
          opContext, tempIndexName, (String) reinfo.get(ORIGINALPREFIX + setting), setting);
    }
    //    setting = INDICES_MEMORY_INDEX_BUFFER_SIZE;
    //    if (reinfo.containsKey(ORIGINALPREFIX + setting)) {
    //      setClusterSettings(setting, (String) reinfo.get(ORIGINALPREFIX + setting));
    //    }
  }

  /**
   * Returns the physical backing index name(s) that the given alias currently points to. Returns an
   * empty set if the name is not an alias.
   */
  public Set<String> getBackingIndices(
      @Nonnull OperationContext opContext, @Nonnull String aliasName) throws IOException {
    GetAliasesResponse response =
        searchClient.getIndexAliases(
            opContext, new GetAliasesRequest(aliasName), requestOptionsLong);
    return response.getAliases().keySet();
  }

  /**
   * Validates doc counts match between an alias and a new backing index, then atomically swaps the
   * alias to point to the new index.
   *
   * @param aliasName the alias to swap
   * @param newBackingIndex the physical index to point the alias to
   * @return true if swapped, false if doc counts didn't match
   * @throws Exception if the swap operation fails
   */
  public boolean validateAndSwapAlias(
      @Nonnull OperationContext opContext,
      @Nonnull String aliasName,
      @Nonnull String newBackingIndex)
      throws Exception {
    long currentCount = getCount(opContext, aliasName);
    long nextCount = getCount(opContext, newBackingIndex);

    if (currentCount != nextCount) {
      log.warn(
          "Doc count mismatch for alias swap {} -> {}: current={}, next={}",
          aliasName,
          newBackingIndex,
          currentCount,
          nextCount);
      return false;
    }

    log.info(
        "Doc counts match for {} -> {}: count={}. Swapping alias.",
        aliasName,
        newBackingIndex,
        currentCount);
    renameReindexedIndices(
        searchClient, opContext, aliasName, null, newBackingIndex, false, requestOptionsLong);
    return true;
  }

  public static void renameReindexedIndices(
      SearchClientShim<?> searchClient,
      @Nonnull OperationContext opContext,
      String originalName,
      @Nullable String pattern,
      String newName,
      boolean deleteOld,
      RequestOptions requestOptions)
      throws Exception {
    GetAliasesRequest getAliasesRequest = new GetAliasesRequest(originalName);
    if (pattern != null) {
      getAliasesRequest.indices(pattern);
    }
    GetAliasesResponse aliasesResponse =
        staticRetryer.executeCallable(
            () -> searchClient.getIndexAliases(opContext, getAliasesRequest, requestOptions));

    // If not aliased, delete the original index
    final Collection<String> aliasedIndexDelete;
    String delinfo;
    if (aliasesResponse.getAliases().isEmpty()) {
      log.info("Deleting index {} to allow alias creation", originalName);
      aliasedIndexDelete = List.of(originalName);
      delinfo = originalName;
    } else {
      log.info(
          "Deleting old indices in existing alias {}: {}",
          originalName,
          aliasesResponse.getAliases().keySet());
      aliasedIndexDelete = aliasesResponse.getAliases().keySet();
      delinfo = String.join(",", aliasedIndexDelete);
    }

    // Add alias for the new index
    AliasActions removeAction =
        (deleteOld || aliasesResponse.getAliases().isEmpty())
            ? AliasActions.removeIndex()
            : AliasActions.remove().alias(originalName);
    removeAction.indices(aliasedIndexDelete.toArray(new String[0]));
    AliasActions addAction = AliasActions.add().alias(originalName).index(newName);
    updateAliasWithRetry(searchClient, opContext, removeAction, addAction, delinfo, requestOptions);
    log.info(
        "Successfully swapped alias {} to {}, deleted old indices: {}",
        originalName,
        newName,
        delinfo);
  }

  public static RequestOptions buildRequestOptionsLong(
      @Nullable ElasticSearchConfiguration elasticSearchConfiguration) {
    int timeoutSec = BuildIndicesConfiguration.DEFAULT_SLOW_OPERATION_TIMEOUT_SECONDS;
    if (elasticSearchConfiguration != null
        && elasticSearchConfiguration.getBuildIndices() != null
        && elasticSearchConfiguration.getBuildIndices().getSlowOperationTimeoutSeconds() > 0) {
      timeoutSec = elasticSearchConfiguration.getBuildIndices().getSlowOperationTimeoutSeconds();
    }
    return RequestOptions.DEFAULT.toBuilder()
        .setRequestConfig(RequestConfig.custom().setSocketTimeout(timeoutSec * 1000).build())
        .build();
  }

  /**
   * Get a specific setting value for an index. Package-private for cost estimation.
   *
   * @param indexName The name of the index
   * @param setting The setting key (e.g., "index.number_of_shards")
   * @return The setting value, or null if not found
   * @throws IOException If there's an error fetching index settings
   */
  String getIndexSetting(@Nonnull OperationContext opContext, String indexName, String setting)
      throws IOException {
    GetSettingsRequest request =
        new GetSettingsRequest()
            .indices(indexName)
            .includeDefaults(true) // Include default settings if not explicitly set
            .names(setting); // Optionally filter to just the setting we want
    GetSettingsResponse response =
        searchClient.getIndexSettings(opContext, request, requestOptionsLong);
    String indexSetting = response.getSetting(indexName, setting);
    return indexSetting;
  }

  /**
   * Set a specific setting value for an index. Package-private for cost estimation and destination
   * optimization.
   *
   * @param indexName The name of the index
   * @param value The setting value to set
   * @param setting The setting key (e.g., "index.number_of_shards")
   */
  void setIndexSetting(
      @Nonnull OperationContext opContext, String indexName, String value, String setting)
      throws IOException {
    UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
    Settings settings = Settings.builder().put(setting, value).build();
    request.settings(settings);
    try {
      retryRegistry
          .retry("retryIndexSetting", "countRetry")
          .executeCallable(
              () -> searchClient.updateIndexSettings(opContext, request, requestOptionsLong));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get primary shard count for an index or alias. Works with both aliases and physical indices.
   * When querying an alias, response keys contain the physical index names, allowing us to extract
   * shard count correctly.
   *
   * @param indexName The index name or alias
   * @return Number of primary shards (defaults to 1 if unable to determine)
   * @throws IOException If unable to fetch shard information
   */
  public int getPrimaryShardCount(@Nonnull OperationContext opContext, String indexName)
      throws IOException {
    try {
      GetSettingsRequest request =
          new GetSettingsRequest()
              .indices(indexName)
              .includeDefaults(true)
              .names("index.number_of_shards");
      GetSettingsResponse response =
          searchClient.getIndexSettings(opContext, request, RequestOptions.DEFAULT);

      // Response keys are physical index names even when querying an alias
      // Get the first entry's shard count
      if (!response.getIndexToSettings().isEmpty()) {
        Settings settings = response.getIndexToSettings().values().iterator().next();
        String shardCountStr = settings.get("index.number_of_shards");
        if (shardCountStr != null && !shardCountStr.isEmpty()) {
          int count = Integer.parseInt(shardCountStr);
          if (count > 0) {
            log.debug("Primary shard count for index {}: {}", indexName, count);
            return count;
          }
        }
      }

      log.warn("Could not determine shard count for index {}, defaulting to 1", indexName);
      return 1;
    } catch (Exception e) {
      log.warn(
          "Failed to get shard count for index {}, defaulting to 1: {}", indexName, e.getMessage());
      return 1;
    }
  }

  /**
   * Sets the refresh interval for an index.
   *
   * @param indexName the name of the index
   * @param refreshInterval the refresh interval value (e.g., "1s", "-1" for disabled)
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public void setIndexRefreshInterval(
      @Nonnull OperationContext opContext, String indexName, String refreshInterval)
      throws IOException {
    setIndexSetting(opContext, indexName, refreshInterval, INDEX_REFRESH_INTERVAL);
  }

  /**
   * Sets the replica count for an index.
   *
   * @param indexName the name of the index
   * @param replicaCount the number of replicas
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public void setIndexReplicaCount(
      @Nonnull OperationContext opContext, String indexName, int replicaCount) throws IOException {
    setIndexSetting(opContext, indexName, String.valueOf(replicaCount), INDEX_NUMBER_OF_REPLICAS);
  }

  /**
   * Get multiple index settings in a single API call (batch operation).
   *
   * @param indexName the name of the index
   * @param settingNames the names of the settings to fetch (e.g., "index.refresh_interval",
   *     "index.number_of_replicas")
   * @return Map of setting names to their values (includes defaults if not explicitly set)
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public Map<String, String> getIndexSettings(
      @Nonnull OperationContext opContext, String indexName, String... settingNames)
      throws IOException {
    GetSettingsRequest request =
        new GetSettingsRequest()
            .indices(indexName)
            .includeDefaults(true); // Include default settings if not explicitly set

    if (settingNames != null && settingNames.length > 0) {
      request.names(settingNames); // Filter to requested settings for efficiency
    }

    try {
      GetSettingsResponse response =
          settingsUpdateRetry.executeCallable(
              () -> searchClient.getIndexSettings(opContext, request, RequestOptions.DEFAULT));
      Map<String, String> result = new HashMap<>();

      // Use response.getSetting() to correctly retrieve both explicit AND default settings
      // (avoid using getIndexToSettings() which loses defaults even with includeDefaults(true))
      if (settingNames != null) {
        for (String settingName : settingNames) {
          String value = response.getSetting(indexName, settingName);
          if (value != null) {
            result.put(settingName, value);
          }
        }
      }

      return result;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Update multiple index settings in a single atomic API call using Settings object (batch
   * operation).
   *
   * @param indexName the name of the index
   * @param settings OpenSearch Settings object with all settings to apply
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public void updateIndexSettings(
      @Nonnull OperationContext opContext, String indexName, Settings settings) throws IOException {
    if (settings == null || settings.isEmpty()) {
      log.debug("No settings to update for index {}", indexName);
      return;
    }

    UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
    request.settings(settings);
    try {
      settingsUpdateRetry.executeCallable(
          () -> {
            searchClient.updateIndexSettings(opContext, request, requestOptionsLong);
            return null;
          });
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Bulk update settings on multiple indices in a single atomic API call.
   *
   * <p>More efficient than individual updates when applying the same settings to many indices
   * (e.g., refresh_interval during health state changes).
   *
   * @param indexNames array of index names to update
   * @param settings OpenSearch Settings object with all settings to apply
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public void updateIndexSettings(
      @Nonnull OperationContext opContext, String[] indexNames, Settings settings)
      throws IOException {
    if (indexNames == null || indexNames.length == 0) {
      log.debug("No indices provided for settings update");
      return;
    }
    if (settings == null || settings.isEmpty()) {
      log.debug("No settings to update for {} indices", indexNames.length);
      return;
    }

    try {
      UpdateSettingsRequest request = new UpdateSettingsRequest(indexNames);
      request.settings(settings);
      retryRegistry
          .retry("retryIndexSetting", "countRetry")
          .executeCallable(
              () -> searchClient.updateIndexSettings(opContext, request, requestOptionsLong));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    log.debug("Bulk updated settings on {} indices", indexNames.length);
  }

  /**
   * Submit a reindex task for parallel execution. Extracted for reuse by parallel reindex
   * orchestrator.
   *
   * @param sourceIndices Source index names to reindex from
   * @param destinationIndex Destination index name to reindex to
   * @param indexConfig The reindex configuration containing settings and mappings
   * @param requestsPerSecond
   * @return Map containing taskId and other reindex info
   * @throws IOException If there's an error submitting the reindex task
   */
  public Map<String, Object> submitReindexInternal(
      @Nonnull OperationContext opContext,
      String[] sourceIndices,
      String destinationIndex,
      ReindexConfig indexConfig,
      float requestsPerSecond)
      throws IOException {
    int targetShards = getTargetShards(indexConfig);
    return submitReindexWithoutOptimization(
        opContext,
        sourceIndices,
        destinationIndex,
        getReindexBatchSize(),
        targetShards,
        requestsPerSecond);
  }

  /**
   * Extract target shard count from reindex configuration.
   *
   * @param indexConfig The reindex configuration
   * @return The target number of shards
   */
  public int getTargetShards(ReindexConfig indexConfig) {
    return Optional.ofNullable(indexConfig.targetSettings().get("index"))
        .filter(Map.class::isInstance)
        .map(Map.class::cast)
        .map(indexMap -> indexMap.get(NUMBER_OF_SHARDS))
        .map(Object::toString)
        .map(Integer::parseInt)
        .orElseThrow(
            () ->
                new IllegalArgumentException("Number of shards not specified in target settings"));
  }

  /**
   * Submit reindex task without optimal settings optimization.
   *
   * <p>This is used for the parallel reindex path to avoid double-optimization. It performs the
   * same reindex submission as submitReindex() but skips the setReindexOptimalSettings() call.
   *
   * @param sourceIndices Array of source index names to reindex from
   * @param destinationIndex Target index to reindex to
   * @param lBatchSize Batch size for reindex scroll operations
   * @param targetShards Number of target shards for optimal slices calculation
   * @param requestsPerSecond
   * @return Map containing reindex info including taskId
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  private Map<String, Object> submitReindexWithoutOptimization(
      @Nonnull OperationContext opContext,
      String[] sourceIndices,
      String destinationIndex,
      int lBatchSize,
      int targetShards,
      float requestsPerSecond)
      throws IOException {

    // Refresh source index to ensure all documents are visible to reindex scroll reader
    // This is required for correctness - without refresh, uncommitted documents may be missed
    // Use SETTINGS_UPDATE retry config which handles both IOException and RuntimeException
    Retry.of("refreshSourceIndex", RetryConfigUtils.DOC_COUNT_REFRESH)
        .executeRunnable(
            () -> {
              try {
                searchClient.refreshIndex(
                    opContext, new RefreshRequest(sourceIndices), requestOptionsLong);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

    int slices = calculateOptimalSlices(targetShards);

    Map<String, Object> reindexInfo = new HashMap<>();

    ReindexRequest reindexRequest =
        new ReindexRequest()
            .setSourceIndices(sourceIndices)
            .setDestIndex(destinationIndex)
            .setMaxRetries(indexConfig.getNumRetries())
            .setAbortOnVersionConflict(false)
            // Use fixed slices from config for consistent parallelism across all health states
            // Dynamic control is handled via RPS throttling, not slices
            .setSlices(slices)
            .setSourceBatchSize(lBatchSize)
            .setRequestsPerSecond(requestsPerSecond);

    RequestOptions requestOptions =
        ESUtils.buildReindexTaskRequestOptions(
            gitVersion.getVersion(), sourceIndices[0], destinationIndex);
    try {
      String reindexTask =
          this.reindexSubmissionRetry.executeCallable(
              () -> searchClient.submitReindexTask(opContext, reindexRequest, requestOptions));
      reindexInfo.put("taskId", reindexTask);
      return reindexInfo;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Rethrottle an active reindex task to change its request rate.
   *
   * <p>Updates the request rate for an in-flight reindex task. This allows tuning performance
   * without stopping and restarting the task.
   *
   * @param taskId The reindex task ID in format "nodeId:taskId"
   * @param requestsPerSecond Desired request rate. Use -1 for unlimited, or any positive number for
   *     requests per second
   */
  public void rethrottleTask(
      @Nonnull OperationContext opContext, String taskId, float requestsPerSecond) {
    try {
      // Construct the rethrottle endpoint URL
      String endpoint = String.format("/_reindex/%s/_rethrottle", taskId);

      // Create a POST request with the requests_per_second parameter
      Request request = new Request("POST", endpoint);
      request.addParameter("requests_per_second", String.valueOf(requestsPerSecond));

      settingsUpdateRetry.executeRunnable(
          () -> {
            try {
              searchClient.performLowLevelRequest(opContext, request);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
      // Execute the request

      log.info(
          "Successfully rethrottled reindex task {} to {} requests/sec", taskId, requestsPerSecond);
    } catch (RuntimeException e) {
      log.warn(
          "Failed to rethrottle reindex task {} to {} requests/sec. Task will continue at current rate. Error: {}",
          taskId,
          requestsPerSecond,
          e.getMessage(),
          e);
      // Non-fatal - task continues at current rate
    }
  }

  /**
   * Max time to wait for a cancelled reindex task to actually stop before giving up (and failing).
   */
  private static final long REINDEX_CANCEL_CONFIRM_TIMEOUT_MS = 120_000L;

  /** Interval between task-status checks while confirming a cancellation. */
  private static final long REINDEX_CANCEL_CONFIRM_POLL_MS = 2_000L;

  /**
   * Cancel a reindex task and confirm it has actually stopped.
   *
   * <p>Unlike {@link #rethrottleTask}, failure here is fatal and must never be swallowed: callers
   * cancel a task precisely because they are about to submit a replacement reindex into the same
   * target, and proceeding while the old task is still running would corrupt that target with two
   * concurrent writers (and the stale task could be re-discovered on a later resume). A 200 from
   * the cancel API only means cancellation was *requested*, so we poll until the task is
   * gone/completed and throw if it has not stopped within the grace window.
   *
   * @param opContext operation context
   * @param taskInfo the reindex task discovered via the opaque-id header
   * @throws Exception if the task cannot be confirmed stopped within {@link
   *     #REINDEX_CANCEL_CONFIRM_TIMEOUT_MS}
   */
  public void cancelReindexTaskAndConfirm(
      @Nonnull OperationContext opContext, @Nonnull TaskInfo taskInfo) throws Exception {
    cancelReindexTaskAndConfirm(
        opContext, taskInfo, REINDEX_CANCEL_CONFIRM_TIMEOUT_MS, REINDEX_CANCEL_CONFIRM_POLL_MS);
  }

  @VisibleForTesting
  public void cancelReindexTaskAndConfirm(
      @Nonnull OperationContext opContext, @Nonnull TaskInfo taskInfo, long timeoutMs, long pollMs)
      throws Exception {
    // For a sliced reindex the header-matched task may be a child slice; cancelling the parent
    // cancels the whole reindex (and all slices). If there is no parent, it IS the reindex task.
    String taskId =
        taskInfo.getParentTaskId() != null && taskInfo.getParentTaskId().isSet()
            ? taskInfo.getParentTaskId().toString()
            : taskInfo.getTaskId().toString();

    // Issue the cancel. Best-effort, but logged (never silently dropped) - the confirmation poll
    // below is the source of truth, and it also handles the benign "already gone" race without a
    // false failure.
    try {
      Request request = new Request("POST", String.format("/_tasks/%s/_cancel", taskId));
      settingsUpdateRetry.executeRunnable(
          () -> {
            try {
              searchClient.performLowLevelRequest(opContext, request);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
      log.info("Requested cancellation of reindex task {}", taskId);
    } catch (RuntimeException e) {
      log.warn(
          "Cancel request for reindex task {} failed; verifying task state before deciding.",
          taskId,
          e);
    }

    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      try {
        Optional<GetTaskResponse> status = getTaskStatus(opContext, taskId);
        if (status.isEmpty() || status.get().isCompleted()) {
          log.info("Confirmed reindex task {} is stopped.", taskId);
          return;
        }
      } catch (IOException e) {
        // Transient communication error - keep polling within the window rather than assume
        // stopped.
        log.warn("Transient error confirming cancellation of task {}; retrying.", taskId, e);
      }
      Thread.sleep(pollMs);
    }
    throw new IllegalStateException(
        String.format(
            "Failed to confirm cancellation of reindex task %s within %d ms; aborting to avoid "
                + "concurrent reindexes into the same target.",
            taskId, timeoutMs));
  }

  /**
   * Whether a resumable in-flight reindex is building into an index whose mappings still match what
   * we would build today. Settings are intentionally ignored: a reindex target temporarily runs
   * with reindex-optimal settings (e.g. 0 replicas, refresh disabled), so only the mappings are
   * compared (reusing the same {@link ReindexConfig} comparison used to decide reindexing in the
   * first place). Any failure to validate is treated as "does not match" so the caller restarts
   * cleanly.
   */
  @VisibleForTesting
  public boolean resumedReindexMatchesTarget(
      @Nonnull OperationContext opContext, ReindexConfig indexState, TaskInfo taskInfo) {
    String adoptedTemp = null;
    try {
      adoptedTemp = ESUtils.extractTargetIndex(taskInfo.getHeaders().get(ESUtils.OPAQUE_ID_HEADER));
      ReindexConfig tempState =
          buildReindexState(
              opContext, adoptedTemp, indexState.targetMappings(), Collections.emptyMap());
      // requiresApplyMappings() is false only when the temp index's mappings already equal the
      // current target - i.e. it was built from the same mapping version we would build now.
      return !tempState.requiresApplyMappings();
    } catch (Exception e) {
      log.warn(
          "Index: {} - could not validate resumed reindex target {}; treating as stale.",
          indexState.name(),
          adoptedTemp,
          e);
      return false;
    }
  }

  /**
   * Get the status of a reindex task using the Task API. Task ID format is "nodeId:taskId"
   *
   * @param taskId The full task ID in format "nodeId:taskId"
   * @return Optional containing GetTaskResponse if task exists
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public Optional<GetTaskResponse> getTaskStatus(
      @Nonnull OperationContext opContext, @Nonnull String taskId) throws IOException {
    // Validate input
    if (taskId == null || taskId.isEmpty()) {
      throw new IllegalArgumentException("Task ID cannot be null or empty");
    }
    if (!taskId.contains(":")) {
      throw new IllegalArgumentException("Invalid task ID format (missing ':'): " + taskId);
    }

    String[] parts = taskId.split(":");
    if (parts.length != 2) {
      throw new IllegalArgumentException(
          "Invalid task ID format. Expected 'nodeId:taskId', got: " + taskId);
    }

    String nodeId = parts[0];
    if (nodeId.isEmpty()) {
      throw new IllegalArgumentException("Task ID node part cannot be empty: " + taskId);
    }

    long numericTaskId;
    try {
      numericTaskId = Long.parseLong(parts[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Task ID must be numeric. Expected 'nodeId:taskId', got: " + taskId, e);
    }

    GetTaskRequest request = new GetTaskRequest(nodeId, numericTaskId);
    try {
      return taskStatusRetry.executeSupplier(
          () -> {
            try {
              return searchClient.getTask(request, RequestOptions.DEFAULT);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw e;
    }
  }

  /**
   * Result object for batch task status queries.
   *
   * <p>Distinguishes between: - Successfully fetched task statuses (responses) - Tasks with
   * transient fetch errors that should be retried (failedTaskIds) - Tasks legitimately not found
   * (not in either map/set)
   */
  @Data
  @AllArgsConstructor
  public static class TaskStatusResult {
    /** Map of taskId to GetTaskResponse for tasks that were successfully fetched */
    @Nonnull private final Map<String, GetTaskResponse> responses;

    /**
     * Set of taskIds that had transient fetch errors (network/timeout). These should NOT be treated
     * as missing tasks - they had communication failures and should be retried.
     */
    @Nonnull private final Set<String> failedTaskIds;
  }

  /**
   * Get status for multiple tasks in a single batch. Calls getTaskStatus for each task ID.
   *
   * <p>CRITICAL: This method distinguishes between three cases: - Task successfully fetched (in
   * responses map) - Task not found - 404 returned by ES (not in either map/set) - task completed
   * and removed - Transient fetch error - network/timeout (in failedTaskIds set) - should be
   * retried, not treated as missing
   *
   * <p>This distinction is crucial: if a task had a network error, it may still be running on ES.
   * Treating network errors as "task not found" would cause premature finalization while reindex is
   * still running.
   *
   * @param taskIds Collection of task IDs to query
   * @return TaskStatusResult with responses and failed task IDs
   */
  @Nonnull
  public TaskStatusResult getTaskStatusMultiple(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> taskIds) {
    if (taskIds.isEmpty()) {
      return new TaskStatusResult(Collections.emptyMap(), Collections.emptySet());
    }

    Map<String, GetTaskResponse> results = new HashMap<>();
    Set<String> failedTaskIds = new HashSet<>();

    for (String taskId : taskIds) {
      try {
        Optional<GetTaskResponse> taskResponse = getTaskStatus(opContext, taskId);
        taskResponse.ifPresent(getTaskResponse -> results.put(taskId, getTaskResponse));
        // Task not found (404) - this is normal when task completes and is removed from active list
        // (not in results, not in failedTaskIds - caller treats as legitimately completed)
      } catch (IOException e) {
        // Transient network/timeout error - MUST track separately
        // Caller should NOT treat as "task missing" - it may still be running on ES
        log.warn(
            "Transient fetch error getting status for task {} (will NOT treat as missing): {}",
            taskId,
            e.getMessage());
        failedTaskIds.add(taskId);
      } catch (Exception e) {
        // Unexpected error - also track as failed to retry later
        log.debug(
            "Unexpected error getting status for task {} (will NOT treat as missing): {}",
            taskId,
            e.getMessage());
        failedTaskIds.add(taskId);
      }
    }

    if (!failedTaskIds.isEmpty()) {
      log.error(
          "GetTaskStatusMultiple: {} of {} tasks had fetch errors (network/transient issues), {} returned successfully, {} will be retried",
          failedTaskIds.size(),
          taskIds.size(),
          results.size(),
          failedTaskIds.size());
    }
    return new TaskStatusResult(results, failedTaskIds);
  }

  private Map<String, Object> submitReindex(
      @Nonnull OperationContext opContext,
      String[] sourceIndices,
      String destinationIndex,
      int lBatchSize,
      @Nullable TimeValue timeout,
      @Nullable QueryBuilder sourceFilterQuery,
      int targetShards)
      throws IOException {
    // make sure we get all docs from source
    searchClient.refreshIndex(
        opContext,
        new org.opensearch.action.admin.indices.refresh.RefreshRequest(sourceIndices),
        requestOptionsLong);
    Map<String, Object> reindexInfo =
        setReindexOptimalSettings(opContext, destinationIndex, targetShards);
    ReindexRequest reindexRequest =
        new ReindexRequest()
            .setSourceIndices(sourceIndices)
            .setDestIndex(destinationIndex)
            .setMaxRetries(indexConfig.getNumRetries())
            .setAbortOnVersionConflict(false)
            // we cannot set to 'auto', so explicitely set to the number of target number_of_shards
            .setSlices((Integer) reindexInfo.get("optimalSlices"))
            .setSourceBatchSize(lBatchSize);
    if (timeout != null) {
      reindexRequest.setTimeout(timeout);
    }
    if (sourceFilterQuery != null) {
      reindexRequest.setSourceQuery(sourceFilterQuery);
    }
    RequestOptions requestOptions =
        requestOptionsLong.toBuilder()
            .addHeader(
                ESUtils.OPAQUE_ID_HEADER,
                ESUtils.getOpaqueIdHeaderValue(
                    gitVersion.getVersion(), sourceIndices[0], destinationIndex))
            .build();
    String reindexTask = searchClient.submitReindexTask(opContext, reindexRequest, requestOptions);
    reindexInfo.put("taskId", reindexTask);
    return reindexInfo;
  }

  private Pair<Long, Long> getDocumentCounts(
      @Nonnull OperationContext opContext,
      Callable<Long> expectedCountSupplier,
      String destinationIndex)
      throws Throwable {
    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so
    // try multiple times
    long expectedCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i <= indexConfig.getNumRetries(); i++) {
      // Check if reindex succeeded by comparing document counts
      expectedCount =
          retryRegistry
              .retry("retrySourceIndexCount", "countRetry")
              .executeCheckedSupplier(expectedCountSupplier::call);
      reindexedCount =
          retryRegistry
              .retry("retryDestinationIndexCount", "countRetry")
              .executeCheckedSupplier(() -> getCount(opContext, destinationIndex));
      if (expectedCount == reindexedCount) {
        break;
      }
      try {
        // in the first step wait much less, for very small indices finish in a couple of seconds
        if (i == 0) {
          Thread.sleep(2 * 1000);
        } else {
          Thread.sleep(20 * 1000);
        }
      } catch (InterruptedException e) {
        log.warn("Sleep interrupted");
      }
    }

    return Pair.of(expectedCount, reindexedCount);
  }

  public Optional<TaskInfo> getTaskInfoByHeader(
      @Nonnull OperationContext opContext, String indexName) throws Throwable {
    Retry retryWithDefaultConfig = retryRegistry.retry("getTaskInfoByHeader");

    return retryWithDefaultConfig.executeCheckedSupplier(
        () -> {
          ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(true);
          List<TaskInfo> taskInfos =
              searchClient.listTasks(listTasksRequest, requestOptionsLong).getTasks();
          return taskInfos.stream()
              .filter(
                  info ->
                      ESUtils.prefixMatch(
                          info.getHeaders().get(ESUtils.OPAQUE_ID_HEADER),
                          gitVersion.getVersion(),
                          indexName))
              .findFirst();
        });
  }

  private void diff(
      @Nonnull OperationContext opContext, String indexA, String indexB, long maxDocs) {
    if (maxDocs <= 100) {
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(100);
      searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

      SearchRequest indexARequest = new SearchRequest(indexA);
      indexARequest.source(searchSourceBuilder);
      SearchRequest indexBRequest = new SearchRequest(indexB);
      indexBRequest.source(searchSourceBuilder);

      try {
        SearchResponse responseA =
            searchClient.search(opContext, indexARequest, requestOptionsLong);
        SearchResponse responseB =
            searchClient.search(opContext, indexBRequest, requestOptionsLong);

        Set<String> actual =
            Arrays.stream(responseB.getHits().getHits())
                .map(SearchHit::getId)
                .collect(Collectors.toSet());

        log.error(
            "Missing {}",
            Arrays.stream(responseA.getHits().getHits())
                .filter(doc -> !actual.contains(doc.getId()))
                .map(SearchHit::getSourceAsString)
                .collect(Collectors.toSet()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public long getCount(@Nonnull OperationContext opContext, @Nonnull String indexName)
      throws IOException {
    // we need to refreshIndex cause we are reindexing with refresh_interval=-1
    searchClient.refreshIndex(
        opContext,
        new org.opensearch.action.admin.indices.refresh.RefreshRequest(indexName),
        requestOptionsLong);
    return searchClient
        .count(
            opContext,
            new CountRequest(indexName).query(QueryBuilders.matchAllQuery()),
            requestOptionsLong)
        .getCount();
  }

  /**
   * Get document count without expensive refresh operation. Use this for monitoring where eventual
   * consistency is acceptable.
   *
   * @param indexName The name of the index to count
   * @return The number of documents in the index
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public long getCountWithoutRefresh(@Nonnull OperationContext opContext, @Nonnull String indexName)
      throws IOException {
    try {
      return this.taskStatusRetry.executeCallable(
          () ->
              searchClient
                  .count(
                      opContext,
                      new CountRequest(indexName).query(QueryBuilders.matchAllQuery()),
                      requestOptionsLong)
                  .getCount());
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Check if an index exists.
   *
   * @param indexName The name of the index to check
   * @return true if the index exists, false otherwise
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public boolean indexExists(@Nonnull OperationContext opContext, @Nonnull String indexName)
      throws IOException {
    return searchClient.indexExists(opContext, new GetIndexRequest(indexName), requestOptionsLong);
  }

  /**
   * Check if an alias currently points to a specific index (lightweight safety check for cleanup).
   *
   * <p>Used to prevent accidental deletion of indices that are still live via alias reference
   * during failed reindex cleanup.
   *
   * @param aliasName The alias name to check (e.g., "dashboardindex_v2")
   * @param indexName The concrete index name to check against (e.g.,
   *     "dashboardindex_v2_1234567890")
   * @return true if the alias points to this index, false otherwise (safe default on errors)
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public boolean aliasPointsToIndex(
      @Nonnull OperationContext opContext, @Nonnull String aliasName, @Nonnull String indexName) {
    try {
      GetAliasesRequest request = new GetAliasesRequest().aliases(aliasName);

      // Use filter_path to limit response to only alias names, reducing ES load on clusters
      // with many aliases or heavy metadata
      RequestOptions options =
          requestOptionsLong.toBuilder().addParameter("filter_path", "*.aliases").build();

      GetAliasesResponse response =
          taskStatusRetry.executeCallable(
              () -> searchClient.getIndexAliases(opContext, request, options));

      // Check if target index is in the alias targets
      boolean found = response.getAliases().containsKey(indexName);
      log.debug(
          "Alias {} points to indices: {} (checking for {}, match={})",
          aliasName,
          response.getAliases().keySet(),
          indexName,
          found);
      return found;

    } catch (Exception e) {
      log.error(
          "Could not verify if alias {} points to {}: {} - safe default: true",
          aliasName,
          indexName,
          e.getMessage());
      return true;
    }
  }

  /**
   * Refresh an index to make all operations performed since the last refresh available for search.
   *
   * @param indexName The name of the index to refresh
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public void refreshIndex(@Nonnull OperationContext opContext, @Nonnull String indexName)
      throws IOException {
    searchClient.refreshIndex(
        opContext,
        new org.opensearch.action.admin.indices.refresh.RefreshRequest(indexName),
        requestOptionsLong);
  }

  /**
   * Get the number of data nodes in the cluster. Used for cost estimation.
   *
   * <p>This is an estimate based on cluster health since NodeStats API may not be directly
   * available. The exact node count is not critical for cost estimation - we use this to divide
   * cost across nodes, and a conservative estimate is safer.
   *
   * @return Estimated number of data nodes (defaults to 3 for medium cluster)
   */
  public int getDataNodeCount(@Nonnull OperationContext opContext) {
    try {
      ClusterHealthResponse health =
          searchClient.clusterHealth(opContext, new ClusterHealthRequest(), RequestOptions.DEFAULT);
      // Use getNumberOfDataNodes() to count only data nodes (not master/coordinating/ingest)
      // Cost estimation formula: (docCount * shards) / dataNodes
      // Using total nodes inflates denominator, misclassifying LARGE as NORMAL
      int dataNodeCount = health.getNumberOfDataNodes();
      if (dataNodeCount <= 0) {
        log.warn("Invalid cluster data node count: {}, defaulting to 3", dataNodeCount);
        return 3;
      }
      log.debug("Cluster has {} data nodes for cost estimation", dataNodeCount);
      return dataNodeCount;
    } catch (Exception e) {
      log.warn("Failed to fetch cluster node count: {}, defaulting to 3", e.getMessage());
      return 3; // Safe default for medium cluster
    }
  }

  /**
   * Get detailed cluster health information for monitoring and health checks.
   *
   * <p>Used by ClusterHealthMonitor to check cluster status, heap usage, and shard distribution
   * before submitting new reindex tasks.
   *
   * @return ClusterHealthResponse with cluster status, shards, and node information
   * @throws IOException If unable to reach cluster
   */
  public ClusterHealthResponse getClusterHealth(@Nonnull OperationContext opContext)
      throws IOException {
    try {
      return healthCheckRetry.executeCallable(
          () ->
              searchClient.clusterHealth(
                  opContext, new ClusterHealthRequest(), RequestOptions.DEFAULT));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Wait for index to reach readiness before promotion. Handles both multi-node and single-node
   * clusters.
   *
   * <p>Multi-node: Waits for GREEN status OR (all primaries active + all replicas synced + no
   * initializing shards).
   *
   * <p>Single-node: Waits for GREEN status OR (all primaries active + no initializing shards).
   * Single-node replicas can never assign to the same node as primaries, so we don't wait for
   * replica sync.
   *
   * @param indexName Index to check
   * @param timeoutSeconds Timeout for individual health check request
   */
  public void waitForIndexGreenHealth(
      @Nonnull OperationContext opContext, String indexName, int timeoutSeconds) {
    healthCheckRetry.executeSupplier(
        () -> {
          try {
            ClusterHealthRequest request = new ClusterHealthRequest(indexName);
            request.timeout(TimeValue.timeValueSeconds(timeoutSeconds));
            request.level(ClusterHealthRequest.Level.INDICES);

            ClusterHealthResponse health =
                searchClient.clusterHealth(opContext, request, RequestOptions.DEFAULT);

            ClusterIndexHealth indexHealth = health.getIndices().get(indexName);

            if (indexHealth == null) {
              throw new IOException("Index " + indexName + " not found in cluster health response");
            }

            int primaryShards = indexHealth.getNumberOfShards();
            int activePrimary = indexHealth.getActivePrimaryShards();
            int activeShards = indexHealth.getActiveShards();
            int expectedReplicas = indexHealth.getNumberOfReplicas() * primaryShards;
            int activeReplicas = activeShards - activePrimary;
            int initializing = indexHealth.getInitializingShards();

            if (isIndexHealthy(indexHealth)) {
              return null;
            }

            throw new IOException(
                String.format(
                    "Index %s not ready: status=%s, activePrimary=%d/%d, activeReplicas=%d/%d, initializing=%d, relocating=%d",
                    indexName,
                    indexHealth.getStatus(),
                    activePrimary,
                    primaryShards,
                    activeReplicas,
                    expectedReplicas,
                    initializing,
                    indexHealth.getRelocatingShards()));
          } catch (IOException e) {
            throw new ReplicaHealthException(
                "Failed to verify replica health for index " + indexName, e);
          }
        });
  }

  private boolean isIndexHealthy(ClusterIndexHealth indexHealth) {
    boolean green = indexHealth.getStatus() == ClusterHealthStatus.GREEN;
    boolean yellow = indexHealth.getStatus() == ClusterHealthStatus.YELLOW;

    // Minimum requirement: All primary shards must be active
    // (Replicas are redundancy and sync asynchronously - irrelevant to alias swap safety)
    boolean allPrimariesActive =
        indexHealth.getNumberOfShards() == indexHealth.getActivePrimaryShards();

    // Safe to promote if status is not RED and all primaries are active
    // YELLOW is fine - it just means replicas are still syncing (happens after swap anyway)
    return (green) || (yellow && allPrimariesActive);
  }

  /**
   * Delete an index. Handles both aliases and concrete indices.
   *
   * @param indexName The name of the index or alias to delete
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public void deleteIndex(@Nonnull OperationContext opContext, @Nonnull String indexName)
      throws IOException {
    IndexDeletionUtils.IndexResolutionResult resolution =
        IndexDeletionUtils.resolveIndexForDeletion(searchClient, opContext, indexName);
    if (resolution == null) {
      log.debug("Index {} does not exist, nothing to delete", indexName);
      return;
    }

    for (String concreteIndex : resolution.indicesToDelete()) {
      try {
        deleteActionWithRetry(searchClient, opContext, concreteIndex, requestOptionsLong);
      } catch (Exception e) {
        throw new IOException("Failed to delete index: " + concreteIndex, e);
      }
    }
  }

  /**
   * Swap alias from old index to new index. Used after reindexing to atomically switch to the new
   * index.
   *
   * @param aliasName The alias name to update
   * @param newIndexName The new index to point the alias to
   * @throws Exception If there's an error communicating with Elasticsearch
   */
  public void swapAliases(
      @Nonnull OperationContext opContext,
      @Nonnull String aliasName,
      @Nullable String indexPattern,
      @Nonnull String newIndexName)
      throws Exception {
    renameReindexedIndices(
        searchClient, opContext, aliasName, indexPattern, newIndexName, true, requestOptionsLong);
  }

  public void createIndex(
      @Nonnull OperationContext opContext, String indexName, ReindexConfig state)
      throws IOException {
    log.info("Index {} does not exist. Creating", indexName);
    Map<String, Object> mappings = state.targetMappings();
    Map<String, Object> settings = state.targetSettings();
    log.info("Creating index {} with targetMappings: {}", indexName, mappings);
    log.info("Creating index {} with targetSettings: {}", indexName, settings);

    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.mapping(mappings);
    createIndexRequest.settings(settings);
    boolean createIndexRetryEnabled =
        config.getBuildIndices() == null || config.getBuildIndices().isCreateIndexRetryEnabled();
    try {
      searchClient.createIndex(opContext, createIndexRequest, requestOptionsLong);
    } catch (IOException e) {
      // Timeout or connection error may mean the index was created; check before retrying
      if (searchClient.indexExists(opContext, new GetIndexRequest(indexName), requestOptionsLong)) {
        log.info(
            "Create index {} timed out or failed but index exists, proceeding: {}",
            indexName,
            e.getMessage());
        return;
      }
      if (createIndexRetryEnabled) {
        log.warn("Create index {} failed ({}), retrying once", indexName, e.getMessage());
        searchClient.createIndex(opContext, createIndexRequest, requestOptionsLong);
      } else {
        throw e;
      }
    }
    log.info("Created index {}", indexName);
  }

  /**
   * Efficiently clear an index by deleting it and recreating it with the same configuration. This
   * is much more efficient than deleting all documents using deleteByQuery.
   *
   * @param indexName The name of the index to clear (can be an alias or concrete index)
   * @param config The ReindexConfig containing mappings and settings for the index
   * @throws IOException If the deletion or creation fails
   */
  public void clearIndex(
      @Nonnull OperationContext opContext, String indexName, ReindexConfig config)
      throws IOException {
    deleteIndex(opContext, indexName);
    log.info("Recreating index {} after clearing", indexName);
    createIndex(opContext, indexName, config);
    if (!indexExists(opContext, indexName)) {
      throw new IOException("Index " + indexName + " was not successfully created after clearing!");
    }
    refreshIndex(opContext, indexName);
    log.info("Successfully cleared and recreated index {}", indexName);
  }

  public static void cleanOrphanedIndices(
      SearchClientShim<?> searchClient,
      @Nonnull OperationContext opContext,
      ElasticSearchConfiguration esConfig,
      ReindexConfig indexState) {
    cleanOrphanedIndices(searchClient, opContext, esConfig, indexState, Set.of());
  }

  public static void cleanOrphanedIndices(
      SearchClientShim<?> searchClient,
      @Nonnull OperationContext opContext,
      ElasticSearchConfiguration esConfig,
      ReindexConfig indexState,
      @Nonnull Set<String> excludePhysicalIndices) {
    log.info(
        "Checking for orphan index pattern {} older than {} {}",
        indexState.indexPattern(),
        esConfig.getBuildIndices().getRetentionValue(),
        esConfig.getBuildIndices().getRetentionUnit());

    RequestOptions requestOptions = buildRequestOptionsLong(esConfig);
    getOrphanedIndices(searchClient, opContext, esConfig, indexState, excludePhysicalIndices)
        .forEach(
            orphanIndex -> {
              log.warn("Deleting orphan index {}.", orphanIndex);
              try {
                deleteActionWithRetry(searchClient, opContext, orphanIndex, requestOptions);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static List<String> getOrphanedIndices(
      SearchClientShim<?> searchClient,
      @Nonnull OperationContext opContext,
      ElasticSearchConfiguration esConfig,
      ReindexConfig indexState,
      @Nonnull Set<String> excludePhysicalIndices) {
    List<String> orphanedIndices = new ArrayList<>();
    RequestOptions requestOptions = buildRequestOptionsLong(esConfig);
    try {
      Date retentionDate =
          Date.from(
              Instant.now()
                  .minus(
                      Duration.of(
                          esConfig.getBuildIndices().getRetentionValue(),
                          ChronoUnit.valueOf(esConfig.getBuildIndices().getRetentionUnit()))));

      GetIndexResponse response =
          searchClient.getIndex(
              opContext, new GetIndexRequest(indexState.indexCleanPattern()), requestOptions);

      for (String index : response.getIndices()) {
        if (excludePhysicalIndices.contains(index)) {
          log.info("Skipping protected index {} referenced by incremental reindex state", index);
          continue;
        }

        var creationDateStr = response.getSetting(index, "index.creation_date");
        var creationDateEpoch = Long.parseLong(creationDateStr);
        var creationDate = new Date(creationDateEpoch);

        if (creationDate.after(retentionDate)) {
          continue;
        }

        if (response.getAliases().containsKey(index)
            && response.getAliases().get(index).size() == 0) {
          log.info("Index {} is orphaned", index);
          orphanedIndices.add(index);
        }
      }
    } catch (Exception e) {
      if (e.getMessage().contains("index_not_found_exception")) {
        log.info("No orphaned indices found with pattern {}", indexState.indexCleanPattern());
      } else {
        log.error(
            "An error occurred when trying to identify orphaned indices. Exception: {}",
            e.getMessage());
      }
    }
    return orphanedIndices;
  }
}
