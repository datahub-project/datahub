package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.search.utils.ESUtils.PROPERTIES;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.utils.ESUtils;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
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
  private static final String INDEX_REFRESH_INTERVAL = "index." + REFRESH_INTERVAL;
  public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
  private static final String INDEX_NUMBER_OF_REPLICAS = "index." + NUMBER_OF_REPLICAS;
  private static final String NUMBER_OF_SHARDS = "number_of_shards";
  private static final String ORIGINALPREFIX = "original";
  private static final Integer REINDEX_BATCHSIZE = 5000;
  private static final Float MINJVMHEAP = 10.F;
  // for debugging
  // private static final Float MINJVMHEAP = 0.1F;

  private final SearchClientShim<?> searchClient;

  @Getter @VisibleForTesting private final ElasticSearchConfiguration config;

  private final IndexConfiguration indexConfig;

  @Getter @VisibleForTesting private final StructuredPropertiesConfiguration structPropConfig;

  @Getter private final Map<String, Map<String, String>> indexSettingOverrides;

  @Getter @VisibleForTesting private final GitVersion gitVersion;

  private final OpenSearchJvmInfo jvminfo;

  private static final RequestOptions REQUEST_OPTIONS =
      RequestOptions.DEFAULT.toBuilder()
          .setRequestConfig(RequestConfig.custom().setSocketTimeout(180 * 1000).build())
          .build();

  private final RetryRegistry retryRegistry;
  private static RetryRegistry deletionRetryRegistry;
  private final int initialSecondsDelete = 10;
  private final int deleteMultiplier = 9;
  // would wait >3000s for the 5th retry
  private static final int deleteMaxAttempts = 5;

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

    // Create a RetryRegistry with a custom global configuration
    RetryConfig config =
        RetryConfig.custom()
            .maxAttempts(Math.max(1, indexConfig.getNumRetries()))
            .waitDuration(Duration.ofSeconds(10))
            .retryOnException(e -> e instanceof OpenSearchException)
            .failAfterMaxAttempts(true)
            .build();
    this.retryRegistry = RetryRegistry.of(config);

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
  public boolean isOpenSearch29OrHigher() throws IOException {
    try {
      // We need to use the low-level client to get version information
      RawResponse response = searchClient.performLowLevelRequest(new Request("GET", "/"));
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
                return buildReindexState(indexMap.getIndexName(), indexMap.getMappings(), settings);
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
                    indexMap.getIndexName(), indexMap.getMappings(), settings, true);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .filter(Objects::nonNull)
        .filter(ReindexConfig::hasNewStructuredProperty)
        .collect(Collectors.toList());
  }

  public ReindexConfig buildReindexState(
      String indexName, Map<String, Object> mappings, Map<String, Object> settings)
      throws IOException {
    return buildReindexState(indexName, mappings, settings, false);
  }

  public ReindexConfig buildReindexState(
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
            .version(gitVersion.getVersion());

    Map<String, Object> baseSettings = new HashMap<>(settings);
    baseSettings.put(NUMBER_OF_SHARDS, indexConfig.getNumShards());
    baseSettings.put(NUMBER_OF_REPLICAS, indexConfig.getNumReplicas());
    baseSettings.put(
        REFRESH_INTERVAL, String.format("%ss", indexConfig.getRefreshIntervalSeconds()));
    // Use zstd in OS only and only if KNN is not enabled (codec settings conflict with KNN)
    // In ES we can use it in the future with best_compression
    if (isOpenSearch29OrHigher() && !isKnnEnabled(baseSettings)) {
      baseSettings.put("codec", "zstd_no_dict");
    }
    baseSettings.putAll(indexSettingOverrides.getOrDefault(indexName, Map.of()));
    Map<String, Object> targetSetting = ImmutableMap.of("index", baseSettings);
    builder.targetSettings(targetSetting);

    // Check if index exists
    boolean exists =
        searchClient.indexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    builder.exists(exists);

    // If index doesn't exist, no reindex
    if (!exists) {
      builder.targetMappings(mappings);
      return builder.build();
    }

    Settings currentSettings =
        searchClient
            .getIndexSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
            .getIndexToSettings()
            .values()
            .iterator()
            .next();
    builder.currentSettings(currentSettings);

    Map<String, Object> currentMappings =
        searchClient
            .getIndexMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
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

  public ReindexResult buildIndex(ReindexConfig indexState) throws IOException {
    ReindexResult result;
    // If index doesn't exist, create index
    if (!indexState.exists()) {
      createIndex(indexState.name(), indexState);
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
      applyMappings(indexState, true);

      if (indexState.requiresApplySettings()) {
        UpdateSettingsRequest request = new UpdateSettingsRequest(indexState.name());
        Map<String, Object> indexSettings =
            ((Map<String, Object>) indexState.targetSettings().get("index"))
                .entrySet().stream()
                    .filter(e -> ReindexConfig.SETTINGS_DYNAMIC.contains(e.getKey()))
                    .collect(Collectors.toMap(e -> "index." + e.getKey(), Map.Entry::getValue));
        request.settings(indexSettings);

        boolean ack =
            searchClient.updateIndexSettings(request, RequestOptions.DEFAULT).isAcknowledged();
        log.info(
            "Updated index {} with new settings. Settings: {}, Acknowledged: {}",
            indexState.name(),
            ReindexConfig.OBJECT_MAPPER.writeValueAsString(indexSettings),
            ack);
      }
    } else {
      try {
        result = reindex(indexState);
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
  private Map<String, Object> increaseReplicasForActiveIndices(String indexName, boolean dryRun)
      throws IOException {
    Map<String, Object> result = new HashMap<>();
    result.put("indexName", indexName);
    result.put("changed", false);
    result.put("dryRun", dryRun);
    GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
    GetIndexResponse response = searchClient.getIndex(getIndexRequest, RequestOptions.DEFAULT);
    Map<String, Settings> indexToSettings = response.getSettings();
    Optional<String> key = indexToSettings.keySet().stream().findFirst();
    String thekey = key.get();
    Settings indexSettings = indexToSettings.get(thekey);
    int replicaCount = Integer.parseInt(indexSettings.get("index.number_of_replicas", "1"));
    CountRequest countRequest = new CountRequest(indexName);
    CountResponse countResponse = searchClient.count(countRequest, RequestOptions.DEFAULT);
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
        searchClient.updateIndexSettings(updateSettingsRequest, RequestOptions.DEFAULT);
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
  private Map<String, Object> reduceReplicasForEmptyIndices(String indexName, boolean dryRun)
      throws IOException {
    Map<String, Object> result = new HashMap<>();
    result.put("indexName", indexName);
    result.put("changed", false);
    result.put("dryRun", dryRun);
    GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
    if (!searchClient.indexExists(getIndexRequest, RequestOptions.DEFAULT)) {
      result.put("skipped", true);
      result.put("reason", "Index does not exist");
      return result;
    }
    GetIndexResponse response = searchClient.getIndex(getIndexRequest, RequestOptions.DEFAULT);
    Map<String, Settings> indexToSettings = response.getSettings();
    Optional<String> key = indexToSettings.keySet().stream().findFirst();
    String thekey = key.get();
    Settings indexSettings = indexToSettings.get(thekey);
    int replicaCount = Integer.parseInt(indexSettings.get("index.number_of_replicas", "1"));
    CountRequest countRequest = new CountRequest(indexName);
    CountResponse countResponse = searchClient.count(countRequest, RequestOptions.DEFAULT);
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
        searchClient.updateIndexSettings(updateSettingsRequest, RequestOptions.DEFAULT);
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

  public void tweakReplicas(ReindexConfig indexState, boolean dryRun) throws IOException {
    Map<String, Object> result = increaseReplicasForActiveIndices(indexState.name(), dryRun);
    Map<String, Object> resultb = reduceReplicasForEmptyIndices(indexState.name(), dryRun);
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
  public void applyMappings(ReindexConfig indexState, boolean suppressError) throws IOException {
    if (indexState.isPureMappingsAddition() || indexState.isPureStructuredPropertyAddition()) {
      log.info("Updating index {} mappings in place.", indexState.name());
      PutMappingRequest request =
          new PutMappingRequest(indexState.name()).source(indexState.targetMappings());
      searchClient.putIndexMapping(request, RequestOptions.DEFAULT);
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
      String indexAlias,
      @Nullable QueryBuilder filterQuery,
      BatchWriteOperationsOptions options,
      ReindexConfig config)
      throws Exception {
    GetAliasesResponse aliasesResponse =
        searchClient.getIndexAliases(new GetAliasesRequest(indexAlias), RequestOptions.DEFAULT);
    if (aliasesResponse.getAliases().isEmpty()) {
      throw new IllegalArgumentException(
          String.format("Input to reindexInPlaceAsync should be an alias. %s is not", indexAlias));
    }

    // Point alias at new index
    String nextIndexName = getNextIndexName(indexAlias, System.currentTimeMillis());
    createIndex(nextIndexName, config);
    renameReindexedIndices(searchClient, indexAlias, null, nextIndexName, false);
    int targetShards =
        Optional.ofNullable(config.targetSettings().get(NUMBER_OF_SHARDS))
            .map(Object::toString)
            .map(Integer::parseInt)
            .orElseThrow(() -> new IllegalArgumentException("Number of shards not specified"));

    Map<String, Object> reinfo =
        submitReindex(
            aliasesResponse.getAliases().keySet().toArray(new String[0]),
            nextIndexName,
            options.getBatchSize(),
            TimeValue.timeValueSeconds(options.getTimeoutSeconds()),
            filterQuery,
            targetShards);
    return (String) reinfo.get("taskId");
  }

  private static String getNextIndexName(String base, long startTime) {
    return base + "_" + startTime;
  }

  private ReindexResult reindex(ReindexConfig indexState) throws Throwable {
    ReindexResult result;
    final long startTime = System.currentTimeMillis();

    final long initialCheckIntervalMilli = 1000;
    final long finalCheckIntervalMilli = 60000;
    final long timeoutAt =
        indexConfig.getMaxReindexHours() > 0
            ? startTime + (1000L * 60 * 60 * indexConfig.getMaxReindexHours())
            : Long.MAX_VALUE;

    String tempIndexName = getNextIndexName(indexState.name(), startTime);
    Map<String, Object> reinfo = new HashMap<>();
    try {
      Optional<TaskInfo> previousTaskInfo = getTaskInfoByHeader(indexState.name());

      int targetShards =
          Optional.ofNullable(indexState.targetSettings().get("index"))
              .filter(Map.class::isInstance)
              .map(Map.class::cast)
              .map(indexMap -> indexMap.get(NUMBER_OF_SHARDS))
              .map(Object::toString)
              .map(Integer::parseInt)
              .orElseThrow(() -> new IllegalArgumentException("Number of shards not specified"));
      String parentTaskId = "";
      boolean reindexTaskCompleted = false;
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
        createIndex(tempIndexName, indexState);
        long curDocCount = getCount(indexState.name());
        if (curDocCount == 0) {
          reindexTaskCompleted = true;
          result = ReindexResult.REINDEXED_SKIPPED_0DOCS;
          log.info(
              "Reindex skipped for {} -> {} due to 0 docs .", indexState.name(), tempIndexName);
        } else {
          reinfo =
              submitReindex(
                  new String[] {indexState.name()},
                  tempIndexName,
                  REINDEX_BATCHSIZE,
                  null,
                  null,
                  targetShards);
          parentTaskId = (String) reinfo.get("taskId");
          result = ReindexResult.REINDEXING;
        }
      }

      int reindexCount = 1;
      int count = 0;
      Pair<Long, Long> documentCounts = getDocumentCounts(indexState.name(), tempIndexName);
      long documentCountsLastUpdated = System.currentTimeMillis();
      long previousDocCount = documentCounts.getSecond();
      long estimatedMinutesRemaining = 0;

      while (!reindexTaskCompleted || (System.currentTimeMillis() < timeoutAt)) {
        log.info(
            "Task: {} - Reindexing from {} to {} in progress...",
            parentTaskId,
            indexState.name(),
            tempIndexName);

        Pair<Long, Long> tempDocumentsCount = getDocumentCounts(indexState.name(), tempIndexName);
        if (!tempDocumentsCount.equals(documentCounts)) {
          long currentTime = System.currentTimeMillis();
          long timeElapsed = currentTime - documentCountsLastUpdated;
          long docsIndexed = tempDocumentsCount.getSecond() - previousDocCount;

          // Calculate indexing rate (docs per millisecond)
          double indexingRate = timeElapsed > 0 ? (double) docsIndexed / timeElapsed : 0;

          // Calculate remaining docs and estimated time
          long remainingDocs = tempDocumentsCount.getFirst() - tempDocumentsCount.getSecond();
          long estimatedMillisRemaining =
              indexingRate > 0 ? (long) (remainingDocs / indexingRate) : 0;
          estimatedMinutesRemaining = estimatedMillisRemaining / (1000 * 60);

          documentCountsLastUpdated = currentTime;
          documentCounts = tempDocumentsCount;
          previousDocCount = documentCounts.getSecond();
        }

        if (documentCounts.getFirst().equals(documentCounts.getSecond())) {
          log.info(
              "Task: {} - Reindexing {} to {} task was successful",
              parentTaskId,
              indexState.name(),
              tempIndexName);
          reindexTaskCompleted = true;
          break;
        } else {
          float progressPercentage =
              (100 * (1.0f * documentCounts.getSecond())) / documentCounts.getFirst();
          log.warn(
              "Task: {} - Document counts do not match {} != {}. Complete: {}%. Estimated time remaining: {} minutes",
              parentTaskId,
              documentCounts.getFirst(),
              documentCounts.getSecond(),
              progressPercentage,
              estimatedMinutesRemaining);

          long lastUpdateDelta = System.currentTimeMillis() - documentCountsLastUpdated;
          if (lastUpdateDelta > (300 * 1000)) {
            if (reindexCount <= indexConfig.getNumRetries()) {
              log.warn(
                  "No change in index count after 5 minutes, re-triggering reindex #{}.",
                  reindexCount);
              reinfo =
                  submitReindex(
                      new String[] {indexState.name()},
                      tempIndexName,
                      REINDEX_BATCHSIZE,
                      null,
                      null,
                      targetShards);
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
    } catch (Throwable e) {
      log.error(
          "Failed to reindex {} to {}: Exception {}",
          indexState.name(),
          tempIndexName,
          e.toString());
      deleteActionWithRetry(searchClient, tempIndexName);
      throw e;
    }

    log.info("Reindex from {} to {} succeeded", indexState.name(), tempIndexName);
    String targetReplicas =
        String.valueOf(((Map) indexState.targetSettings().get("index")).get(NUMBER_OF_REPLICAS));
    String targetRefresh =
        String.valueOf(((Map) indexState.targetSettings().get("index")).get(REFRESH_INTERVAL));
    if (result != ReindexResult.REINDEXED_SKIPPED_0DOCS) {
      setReindexOptimalSettingsUndo(tempIndexName, targetReplicas, targetRefresh, reinfo);
    }
    renameReindexedIndices(
        searchClient, indexState.name(), indexState.indexPattern(), tempIndexName, true);
    log.info("Finished setting up {}", indexState.name());
    return result;
  }

  /**
   * Delete Elasticsearch index with exponential backoff retry using resilience4j
   *
   * @param searchClient
   * @param tempIndexName Index name to delete
   * @throws Exception If deletion ultimately fails after all retries
   */
  private static void deleteActionWithRetry(SearchClientShim<?> searchClient, String tempIndexName)
      throws Exception {
    Retry retry = deletionRetryRegistry.retry("elasticsearchDeleteIndex");
    // Wrap the delete operation in a Callable
    Callable<Void> deleteOperation =
        () -> {
          try {
            log.info("Attempting to delete index: {}", tempIndexName);
            // Check if index exists before deleting
            boolean indexExists =
                searchClient.indexExists(
                    new GetIndexRequest(tempIndexName), RequestOptions.DEFAULT);
            if (indexExists) {
              // Configure delete request with timeout
              DeleteIndexRequest request = new DeleteIndexRequest(tempIndexName);
              request.timeout(TimeValue.timeValueSeconds(30));
              // Execute delete
              searchClient.deleteIndex(request, RequestOptions.DEFAULT);
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
      AliasActions removeAction,
      AliasActions addAction,
      String tempIndexName)
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
            searchClient.updateIndexAliases(request, RequestOptions.DEFAULT);
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

  private Map<String, Object> setReindexOptimalSettings(String tempIndexName, int targetShards)
      throws IOException {
    Map<String, Object> res = new HashMap<>();
    if (config.getBuildIndices().isReindexOptimizationEnabled()) {
      setIndexSetting(tempIndexName, "0", INDEX_NUMBER_OF_REPLICAS);
    }
    setIndexSetting(tempIndexName, "-1", INDEX_REFRESH_INTERVAL);
    // these depend on jvm max heap...
    // flush_threshold_size: 512MB by def. Increasing to 1gb, if heap at least 16gb (this is more
    // conservative than %25 mentioned
    // https://docs.opensearch.org/docs/2.11/tuning-your-cluster/performance/
    //    "index.translog.flush_threshold_size": "512mb",
    double jvmheapgb = jvminfo.getAverageDataNodeMaxHeapSizeGB();
    String setting = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE;
    String optimValue = "1024mb";
    String curval = getIndexSetting(tempIndexName, setting);
    if (SizeUtils.isGreaterSize(optimValue, curval) && jvmheapgb >= MINJVMHEAP) {
      setIndexSetting(tempIndexName, optimValue, setting);
      res.put(ORIGINALPREFIX + setting, curval);
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
    int slices = targetShards;
    // but if too large, tone it done
    // we have max of 60 shards as of now in some huge index, and this sounds fine regarding nb of
    // slices, ES sets the max of slices at 1024, so hour number is quite conservative, just cap it
    // lower, like 256
    slices = Math.min(256, slices);
    res.put("optimalSlices", slices);
    return res;
  }

  private void setReindexOptimalSettingsUndo(
      String tempIndexName,
      String targetReplicas,
      String refreshinterval,
      Map<String, Object> reinfo)
      throws IOException {
    // set the original values
    if (config.getBuildIndices().isReindexOptimizationEnabled()) {
      setIndexSetting(tempIndexName, targetReplicas, INDEX_NUMBER_OF_REPLICAS);
    }
    setIndexSetting(tempIndexName, refreshinterval, INDEX_REFRESH_INTERVAL);
    // reinfo could be emtpy (if reindex was already ongoing...)
    String setting = INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE;
    if (reinfo.containsKey(ORIGINALPREFIX + setting)) {
      setIndexSetting(tempIndexName, (String) reinfo.get(ORIGINALPREFIX + setting), setting);
    }
    //    setting = INDICES_MEMORY_INDEX_BUFFER_SIZE;
    //    if (reinfo.containsKey(ORIGINALPREFIX + setting)) {
    //      setClusterSettings(setting, (String) reinfo.get(ORIGINALPREFIX + setting));
    //    }
  }

  public static void renameReindexedIndices(
      SearchClientShim<?> searchClient,
      String originalName,
      @Nullable String pattern,
      String newName,
      boolean deleteOld)
      throws Exception {
    GetAliasesRequest getAliasesRequest = new GetAliasesRequest(originalName);
    if (pattern != null) {
      getAliasesRequest.indices(pattern);
    }
    GetAliasesResponse aliasesResponse =
        searchClient.getIndexAliases(getAliasesRequest, RequestOptions.DEFAULT);

    // If not aliased, delete the original index
    final Collection<String> aliasedIndexDelete;
    String delinfo;
    if (aliasesResponse.getAliases().isEmpty()) {
      log.info("Deleting index {} to allow alias creation", originalName);
      aliasedIndexDelete = List.of(originalName);
      delinfo = originalName;
    } else {
      log.info("Deleting old indices in existing alias {}", aliasesResponse.getAliases().keySet());
      aliasedIndexDelete = aliasesResponse.getAliases().keySet();
      delinfo = String.join(",", aliasedIndexDelete);
    }

    // Add alias for the new index
    AliasActions removeAction =
        deleteOld ? AliasActions.removeIndex() : AliasActions.remove().alias(originalName);
    removeAction.indices(aliasedIndexDelete.toArray(new String[0]));
    AliasActions addAction = AliasActions.add().alias(originalName).index(newName);
    updateAliasWithRetry(searchClient, removeAction, addAction, delinfo);
  }

  private String getIndexSetting(String indexName, String setting) throws IOException {
    GetSettingsRequest request =
        new GetSettingsRequest()
            .indices(indexName)
            .includeDefaults(true) // Include default settings if not explicitly set
            .names(setting); // Optionally filter to just the setting we want
    GetSettingsResponse response = searchClient.getIndexSettings(request, RequestOptions.DEFAULT);
    String indexSetting = response.getSetting(indexName, setting);
    return indexSetting;
  }

  private void setIndexSetting(String indexName, String value, String setting) throws IOException {
    UpdateSettingsRequest request = new UpdateSettingsRequest(indexName);
    Settings settings = Settings.builder().put(setting, value).build();
    request.settings(settings);
    searchClient.updateIndexSettings(request, RequestOptions.DEFAULT);
  }

  /**
   * Sets the refresh interval for an index.
   *
   * @param indexName the name of the index
   * @param refreshInterval the refresh interval value (e.g., "1s", "-1" for disabled)
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public void setIndexRefreshInterval(String indexName, String refreshInterval) throws IOException {
    setIndexSetting(indexName, refreshInterval, INDEX_REFRESH_INTERVAL);
  }

  /**
   * Sets the replica count for an index.
   *
   * @param indexName the name of the index
   * @param replicaCount the number of replicas
   * @throws IOException if there's an error communicating with Elasticsearch
   */
  public void setIndexReplicaCount(String indexName, int replicaCount) throws IOException {
    setIndexSetting(indexName, String.valueOf(replicaCount), INDEX_NUMBER_OF_REPLICAS);
  }

  private Map<String, Object> submitReindex(
      String[] sourceIndices,
      String destinationIndex,
      int lBatchSize,
      @Nullable TimeValue timeout,
      @Nullable QueryBuilder sourceFilterQuery,
      int targetShards)
      throws IOException {
    // make sure we get all docs from source
    searchClient.refreshIndex(
        new org.opensearch.action.admin.indices.refresh.RefreshRequest(sourceIndices),
        RequestOptions.DEFAULT);
    Map<String, Object> reindexInfo = setReindexOptimalSettings(destinationIndex, targetShards);
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
        ESUtils.buildReindexTaskRequestOptions(
            gitVersion.getVersion(), sourceIndices[0], destinationIndex);
    String reindexTask = searchClient.submitReindexTask(reindexRequest, requestOptions);
    reindexInfo.put("taskId", reindexTask);
    return reindexInfo;
  }

  private Pair<Long, Long> getDocumentCounts(String sourceIndex, String destinationIndex)
      throws Throwable {
    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so
    // try multiple times
    long originalCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i <= indexConfig.getNumRetries(); i++) {
      // Check if reindex succeeded by comparing document counts
      originalCount =
          retryRegistry
              .retry("retrySourceIndexCount")
              .executeCheckedSupplier(() -> getCount(sourceIndex));
      reindexedCount =
          retryRegistry
              .retry("retryDestinationIndexCount")
              .executeCheckedSupplier(() -> getCount(destinationIndex));
      if (originalCount == reindexedCount) {
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

    return Pair.of(originalCount, reindexedCount);
  }

  private Optional<TaskInfo> getTaskInfoByHeader(String indexName) throws Throwable {
    Retry retryWithDefaultConfig = retryRegistry.retry("getTaskInfoByHeader");

    return retryWithDefaultConfig.executeCheckedSupplier(
        () -> {
          ListTasksRequest listTasksRequest = new ListTasksRequest().setDetailed(true);
          List<TaskInfo> taskInfos =
              searchClient.listTasks(listTasksRequest, REQUEST_OPTIONS).getTasks();
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

  public long getCount(@Nonnull String indexName) throws IOException {
    // we need to refreshIndex cause we are reindexing with refresh_interval=-1
    searchClient.refreshIndex(
        new org.opensearch.action.admin.indices.refresh.RefreshRequest(indexName),
        RequestOptions.DEFAULT);
    return searchClient
        .count(
            new CountRequest(indexName).query(QueryBuilders.matchAllQuery()),
            RequestOptions.DEFAULT)
        .getCount();
  }

  /**
   * Check if an index exists.
   *
   * @param indexName The name of the index to check
   * @return true if the index exists, false otherwise
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public boolean indexExists(@Nonnull String indexName) throws IOException {
    return searchClient.indexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
  }

  /**
   * Refresh an index to make all operations performed since the last refresh available for search.
   *
   * @param indexName The name of the index to refresh
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public void refreshIndex(@Nonnull String indexName) throws IOException {
    searchClient.refreshIndex(
        new org.opensearch.action.admin.indices.refresh.RefreshRequest(indexName),
        RequestOptions.DEFAULT);
  }

  /**
   * Delete an index. Handles both aliases and concrete indices.
   *
   * @param indexName The name of the index or alias to delete
   * @throws IOException If there's an error communicating with Elasticsearch
   */
  public void deleteIndex(@Nonnull String indexName) throws IOException {
    IndexDeletionUtils.IndexResolutionResult resolution =
        IndexDeletionUtils.resolveIndexForDeletion(searchClient, indexName);
    if (resolution == null) {
      log.debug("Index {} does not exist, nothing to delete", indexName);
      return;
    }

    for (String concreteIndex : resolution.indicesToDelete()) {
      try {
        deleteActionWithRetry(searchClient, concreteIndex);
      } catch (Exception e) {
        throw new IOException("Failed to delete index: " + concreteIndex, e);
      }
    }
  }

  private void createIndex(String indexName, ReindexConfig state) throws IOException {
    log.info("Index {} does not exist. Creating", indexName);
    Map<String, Object> mappings = state.targetMappings();
    Map<String, Object> settings = state.targetSettings();
    log.info("Creating index {} with targetMappings: {}", indexName, mappings);
    log.info("Creating index {} with targetSettings: {}", indexName, settings);

    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.mapping(mappings);
    createIndexRequest.settings(settings);
    searchClient.createIndex(createIndexRequest, RequestOptions.DEFAULT);
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
  public void clearIndex(String indexName, ReindexConfig config) throws IOException {
    deleteIndex(indexName);
    log.info("Recreating index {} after clearing", indexName);
    createIndex(indexName, config);
    if (!indexExists(indexName)) {
      throw new IOException("Index " + indexName + " was not successfully created after clearing!");
    }
    refreshIndex(indexName);
    log.info("Successfully cleared and recreated index {}", indexName);
  }

  public static void cleanOrphanedIndices(
      SearchClientShim<?> searchClient,
      ElasticSearchConfiguration esConfig,
      ReindexConfig indexState) {
    log.info(
        "Checking for orphan index pattern {} older than {} {}",
        indexState.indexPattern(),
        esConfig.getBuildIndices().getRetentionValue(),
        esConfig.getBuildIndices().getRetentionUnit());

    getOrphanedIndices(searchClient, esConfig, indexState)
        .forEach(
            orphanIndex -> {
              log.warn("Deleting orphan index {}.", orphanIndex);
              try {
                deleteActionWithRetry(searchClient, orphanIndex);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static List<String> getOrphanedIndices(
      SearchClientShim<?> searchClient,
      ElasticSearchConfiguration esConfig,
      ReindexConfig indexState) {
    List<String> orphanedIndices = new ArrayList<>();
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
              new GetIndexRequest(indexState.indexCleanPattern()), RequestOptions.DEFAULT);

      for (String index : response.getIndices()) {
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
