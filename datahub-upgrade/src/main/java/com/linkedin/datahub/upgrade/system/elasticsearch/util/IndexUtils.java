package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;

@Slf4j
public class IndexUtils {

  /** Default retry configuration for upgrade operations. */
  private static final RetryConfig DEFAULT_RETRY_CONFIG =
      RetryConfig.custom()
          .maxAttempts(5)
          .waitDuration(Duration.ofSeconds(2))
          .retryOnException(e -> true) // Retry on any exception
          .failAfterMaxAttempts(false) // Return false instead of throwing
          .build();

  /** Retry registry for upgrade operations. */
  private static final RetryRegistry RETRY_REGISTRY = RetryRegistry.of(DEFAULT_RETRY_CONFIG);

  public static final String INDEX_BLOCKS_WRITE_SETTING = "index.blocks.write";
  public static final int INDEX_BLOCKS_WRITE_RETRY = 4;
  public static final int INDEX_BLOCKS_WRITE_WAIT_SECONDS = 10;

  private IndexUtils() {}

  private static List<ReindexConfig> _reindexConfigs = new ArrayList<>();

  public static List<ReindexConfig> getAllReindexConfigs(
      OperationContext opContext,
      List<ElasticSearchIndexed> elasticSearchIndexedList,
      Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties)
      throws IOException {
    // Avoid locking & reprocessing
    List<ReindexConfig> reindexConfigs = new ArrayList<>(_reindexConfigs);
    if (reindexConfigs.isEmpty()) {
      for (ElasticSearchIndexed elasticSearchIndexed : elasticSearchIndexedList) {
        reindexConfigs.addAll(
            elasticSearchIndexed.buildReindexConfigs(opContext, structuredProperties));
      }
      _reindexConfigs = new ArrayList<>(reindexConfigs);
    }

    return reindexConfigs;
  }

  public static boolean validateWriteBlock(
      SearchClientShim<?> esClient, String indexName, boolean expectedState)
      throws IOException, InterruptedException {
    final String finalIndexName = resolveAlias(esClient, indexName);

    GetSettingsRequest request =
        new GetSettingsRequest()
            .indices(finalIndexName)
            .names(INDEX_BLOCKS_WRITE_SETTING)
            .includeDefaults(true);

    int count = INDEX_BLOCKS_WRITE_RETRY;
    while (count > 0) {
      GetSettingsResponse response = esClient.getIndexSettings(request, RequestOptions.DEFAULT);
      if (response
          .getSetting(finalIndexName, INDEX_BLOCKS_WRITE_SETTING)
          .equals(String.valueOf(expectedState))) {
        return true;
      }
      count = count - 1;

      if (count != 0) {
        Thread.sleep(INDEX_BLOCKS_WRITE_WAIT_SECONDS * 1000);
      }
    }

    return false;
  }

  public static String resolveAlias(SearchClientShim<?> esClient, String indexName)
      throws IOException {
    String finalIndexName = indexName;

    GetAliasesResponse aliasResponse =
        esClient.getIndexAliases(new GetAliasesRequest(indexName), RequestOptions.DEFAULT);

    if (!aliasResponse.getAliases().isEmpty()) {
      Set<String> indices = aliasResponse.getAliases().keySet();
      if (indices.size() != 1) {
        throw new NotImplementedException(
            String.format(
                "Clone not supported for %s indices in alias %s. Indices: %s",
                indices.size(), indexName, String.join(",", indices)));
      }
      finalIndexName = indices.stream().findFirst().get();
      log.info("Alias {} resolved to index {}", indexName, finalIndexName);
    }

    return finalIndexName;
  }

  /**
   * Extracts a JSON value from a JSON string using Jackson ObjectMapper.
   *
   * <p>This method uses the OperationContext's ObjectMapper to parse JSON and extract integer
   * values by key. This is more robust than regex-based parsing and handles various JSON formats
   * correctly.
   *
   * @param operationContext the operation context providing the ObjectMapper
   * @param json the JSON string to parse
   * @param key the key to extract
   * @return the integer value, or -1 if not found or parsing fails
   */
  static int extractJsonValue(OperationContext operationContext, String json, String key) {
    try {
      JsonNode jsonNode = operationContext.getObjectMapper().readTree(json);
      JsonNode valueNode = jsonNode.get(key);
      if (valueNode != null && valueNode.isNumber()) {
        return valueNode.asInt();
      }
    } catch (Exception e) {
      log.warn("Error extracting JSON value for key {}: {}", key, e.getMessage());
    }
    return -1;
  }

  /**
   * Loads a resource file as a UTF-8 encoded string.
   *
   * <p>This utility method reads a resource file from the classpath and returns its contents as a
   * string. It's used internally by other methods to load JSON templates and configuration files.
   *
   * <p>The method uses the class loader to locate the resource and reads it using a buffered input
   * stream for efficient memory usage.
   *
   * @param resourcePath the path to the resource file (e.g.,
   *     "/index/usage-event/elasticsearch_policy.json")
   * @return the contents of the resource file as a UTF-8 encoded string
   * @throws IOException if the resource cannot be found or read
   */
  static String loadResourceAsString(String resourcePath) throws IOException {
    try (InputStream inputStream = IndexUtils.class.getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }

  /**
   * Executes a function with retry logic using Resilience4j RetryConfig.
   *
   * <p>This method uses Resilience4j's retry mechanism with exponential backoff. It retries the
   * provided operation up to the configured number of attempts.
   *
   * @param maxAttempts the maximum number of attempts
   * @param initialDelayMs the initial delay in milliseconds (used for custom config)
   * @param operation the operation to retry
   * @return true if the operation succeeded, false if all attempts failed
   */
  public static boolean retryWithBackoff(
      int maxAttempts, long initialDelayMs, RetryableOperation operation) {

    // Create a custom retry config for this specific operation
    RetryConfig customConfig =
        RetryConfig.custom()
            .maxAttempts(maxAttempts)
            .waitDuration(Duration.ofMillis(initialDelayMs))
            .retryOnException(e -> true) // Retry on any exception
            .failAfterMaxAttempts(false) // Return false instead of throwing
            .build();

    RetryRegistry customRegistry = RetryRegistry.of(customConfig);
    Retry retry = customRegistry.retry("upgrade-operation");

    try {
      return retry.executeSupplier(
          () -> {
            try {
              return operation.execute();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } catch (Exception e) {
      log.error("All {} attempts failed", maxAttempts, e);
      return false;
    }
  }

  /**
   * Executes a function with retry logic using the default RetryConfig.
   *
   * <p>This method uses the default retry configuration (5 attempts, 2 second delay). It's a
   * convenience method for common retry scenarios.
   *
   * @param operation the operation to retry
   * @return true if the operation succeeded, false if all attempts failed
   */
  public static boolean retryWithDefaultConfig(RetryableOperation operation) {
    Retry retry = RETRY_REGISTRY.retry("default-upgrade-operation");

    try {
      return retry.executeSupplier(
          () -> {
            try {
              return operation.execute();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    } catch (Exception e) {
      log.error("All attempts failed with default config");
      return false;
    }
  }

  /** Functional interface for retryable operations. */
  /**
   * Detects if the OpenSearch instance is AWS OpenSearch Service based on the host URL.
   *
   * <p>AWS OpenSearch Service instances typically have URLs containing "amazonaws.com" or
   * "es.amazonaws.com". This method checks the host configuration to determine if we're connecting
   * to AWS OpenSearch Service.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @return true if the instance appears to be AWS OpenSearch Service, false otherwise
   */
  public static boolean isAwsOpenSearchService(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents) {
    try {
      // Get the host from the search client configuration
      String host = esComponents.getSearchClient().getShimConfiguration().getHost();
      if (host != null) {
        return host.contains("amazonaws.com") || host.contains("es.amazonaws.com");
      }
    } catch (Exception e) {
      log.debug("Could not determine host for AWS OpenSearch detection: {}", e.getMessage());
    }
    return false;
  }

  @FunctionalInterface
  public interface RetryableOperation {
    boolean execute() throws Exception;
  }

  /**
   * Performs a GET request with consistent logging.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param endpoint the endpoint to GET (e.g., "/_plugins/_ism/policies/policy_name")
   * @return the raw response from the request
   * @throws IOException if the request fails
   */
  public static RawResponse performGetRequest(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents, String endpoint)
      throws IOException {
    log.info("GET => {}", endpoint);
    Request request = new Request("GET", endpoint);
    return esComponents.getSearchClient().performLowLevelRequest(request);
  }

  /**
   * Performs a PUT request with consistent logging.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param endpoint the endpoint to PUT to (e.g., "/_plugins/_ism/policies/policy_name")
   * @param jsonBody the JSON body to send with the request
   * @return the raw response from the request
   * @throws IOException if the request fails
   */
  public static RawResponse performPutRequest(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String endpoint,
      String jsonBody)
      throws IOException {
    log.info("PUT => {}", endpoint);
    Request request = new Request("PUT", endpoint);
    request.setJsonEntity(jsonBody);
    return esComponents.getSearchClient().performLowLevelRequest(request);
  }

  /**
   * Performs a POST request with consistent logging.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param endpoint the endpoint to POST to (e.g., "/_search")
   * @param jsonBody the JSON body to send with the request
   * @return the raw response from the request
   * @throws IOException if the request fails
   */
  public static RawResponse performPostRequest(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String endpoint,
      String jsonBody)
      throws IOException {
    log.info("POST => {}", endpoint);
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(jsonBody);
    return esComponents.getSearchClient().performLowLevelRequest(request);
  }

  /**
   * Performs a PUT request with query parameters and consistent logging.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param endpoint the base endpoint to PUT to (e.g., "/_plugins/_ism/policies/policy_name")
   * @param queryParams the query parameters to append (e.g., "?if_seq_no=123&if_primary_term=456")
   * @param jsonBody the JSON body to send with the request
   * @return the raw response from the request
   * @throws IOException if the request fails
   */
  public static RawResponse performPutRequestWithParams(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String endpoint,
      String queryParams,
      String jsonBody)
      throws IOException {
    String fullEndpoint = endpoint + queryParams;
    log.info("PUT => {}", fullEndpoint);
    Request request = new Request("PUT", fullEndpoint);
    request.setJsonEntity(jsonBody);
    return esComponents.getSearchClient().performLowLevelRequest(request);
  }
}
