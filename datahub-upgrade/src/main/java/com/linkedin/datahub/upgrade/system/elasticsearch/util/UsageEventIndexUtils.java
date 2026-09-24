package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

/**
 * Utility class for creating and managing Elasticsearch/OpenSearch indices and policies for DataHub
 * usage event tracking. This class provides methods to set up ILM/ISM policies, index templates,
 * data streams, and indices required for analytics functionality.
 *
 * <p>The class handles both standard Elasticsearch and AWS OpenSearch scenarios, using appropriate
 * APIs and configurations for each environment.
 */
@Slf4j
public class UsageEventIndexUtils {

  // Outside the PREFIXdatahub_usage_event* template patterns, so no template applies to backups.
  private static final String LEGACY_BACKUP_INFIX = "legacy_datahub_usage_event_";
  // Recorded on the backup, so a later run checks the same copy instead of starting another one.
  private static final String BACKFILL_TASK_META = "datahub_backfill_task";
  // The write index that copy went into: copying again only skips the events already copied, and
  // so only avoids duplicates, while that is still the write index.
  private static final String BACKFILL_WRITE_INDEX_META = "datahub_backfill_write_index";
  private static final Duration BACKFILL_POLL_INTERVAL = Duration.ofSeconds(2);
  private static final Duration BACKFILL_WAIT = Duration.ofMinutes(5);
  // Data streams reject events without @timestamp. Older events may only carry timestamp; events
  // with neither cannot go into a data stream at all, so they are dropped there.
  private static final String BACKFILL_SCRIPT =
      "if (ctx._source['@timestamp'] == null) {"
          + " if (ctx._source['timestamp'] != null) {"
          + " ctx._source['@timestamp'] = ctx._source['timestamp'] }"
          + " else if (params.dataStream) { ctx.op = 'noop' } }";

  /**
   * Creates an Index Lifecycle Management (ILM) policy for Elasticsearch usage events.
   *
   * <p>This method creates an ILM policy that manages the lifecycle of usage event indices,
   * including rollover and retention policies. The policy is loaded from the resource file {@code
   * /index/usage-event/elasticsearch_policy.json} and applied to the specified policy name.
   *
   * <p>The method uses the low-level REST client to make a PUT request to the {@code
   * /_ilm/policy/{policyName}} endpoint, which provides upsert behavior (creates if not exists,
   * updates if exists).
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param policyName the name of the ILM policy to create (e.g., "datahub_usage_event_policy")
   * @throws IOException if there's an error reading the policy template or making the request
   * @throws ResponseException if the request fails with a non-409 status code
   */
  public static void createIlmPolicy(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName)
      throws IOException {
    try {
      String policyJson =
          IndexUtils.loadResourceAsString("/index/usage-event/elasticsearch_policy.json");

      // Use the low-level client to make the PUT request to _ilm/policy endpoint
      String endpoint = "/_ilm/policy/" + policyName;

      // Use retry logic for policy creation
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  RawResponse response =
                      IndexUtils.performPutRequest(opContext, esComponents, endpoint, policyJson);

                  int statusCode = response.getStatusLine().getStatusCode();
                  if (statusCode == 200 || statusCode == 201) {
                    log.info("Successfully created ILM policy: {}", policyName);
                    return true;
                  } else if (statusCode == 409) {
                    log.info("ILM policy {} already exists", policyName);
                    return true; // Consider this a success since policy exists
                  } else {
                    log.error("ILM policy creation returned status: {}", statusCode);
                    return false;
                  }
                } catch (ResponseException e) {
                  if (e.getResponse().getStatusLine().getStatusCode() == 409) {
                    log.info("ILM policy {} already exists", policyName);
                    return true;
                  } else {
                    throw e;
                  }
                }
              });

      if (!success) {
        throw new IOException("Failed to create ILM policy after retries: " + policyName);
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("ILM policy {} already exists", policyName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates or updates an Index State Management (ISM) policy for AWS OpenSearch usage events.
   *
   * <p>This method follows the same pattern as the Docker setup script: first checking if the
   * policy exists, then either updating the existing policy or creating a new one. The policy is
   * loaded from the resource file {@code /index/usage-event/opensearch_policy.json} and applied to
   * the specified policy name.
   *
   * <p>The method uses the low-level REST client to make requests to the {@code
   * /_plugins/_ism/policies/{policyName}} endpoint, which is specific to AWS OpenSearch. The policy
   * template includes placeholder replacement for the index prefix.
   *
   * <p>ISM policies in OpenSearch provide similar functionality to ILM policies in Elasticsearch,
   * including rollover conditions, state transitions, and retention policies.
   *
   * <p>This method handles the following scenarios:
   *
   * <ul>
   *   <li>Policy exists (200): Updates the existing policy using optimistic concurrency control
   *   <li>Policy doesn't exist (404): Creates a new policy
   *   <li>ISM not supported (400): Returns false gracefully
   * </ul>
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param policyName the name of the ISM policy to create/update (e.g.,
   *     "datahub_usage_event_policy")
   * @param prefix the index prefix to apply to policy configurations (e.g., "prod_")
   * @return true if the policy was successfully created, updated, or already exists, false if
   *     policy operation failed due to unsupported features or other errors
   * @throws IOException if there's an error reading the policy template or making the request
   */
  public static boolean createIsmPolicy(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix,
      OperationContext operationContext)
      throws IOException {
    try {
      log.debug("Creating ISM policy: {}", policyName);

      String policyJson = loadPolicyTemplate(prefix);
      String endpoint = "/_plugins/_ism/policies/" + policyName;

      // Use retry logic for the entire policy creation operation (like the Docker script)
      boolean success =
          IndexUtils.retryWithBackoff(
              5,
              2000,
              () -> {
                try {
                  RawResponse getResponse =
                      IndexUtils.performGetRequest(operationContext, esComponents, endpoint);
                  return handleGetResponse(
                      getResponse,
                      esComponents,
                      policyName,
                      prefix,
                      endpoint,
                      policyJson,
                      operationContext);
                } catch (ResponseException e) {
                  return handleResponseException(
                      e, esComponents, policyName, prefix, endpoint, policyJson, operationContext);
                }
              });

      return success;
    } catch (Exception e) {
      log.error("Unexpected error creating ISM policy {}: {}", policyName, e.getMessage(), e);
      return false;
    }
  }

  /**
   * Handles the response from a GET request to check ISM policy existence.
   *
   * @param getResponse the response from the GET request
   * @param esComponents the Elasticsearch components factory
   * @param policyName the name of the ISM policy
   * @param prefix the prefix for index patterns
   * @param endpoint the API endpoint
   * @param policyJson the policy JSON to create
   * @param operationContext the operation context for JSON parsing
   * @return true if successful, false if retry is needed
   */
  private static boolean handleGetResponse(
      RawResponse getResponse,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix,
      String endpoint,
      String policyJson,
      OperationContext operationContext) {

    int getStatusCode = getResponse.getStatusLine().getStatusCode();

    if (getStatusCode == 200) {
      return handleExistingPolicy(operationContext, esComponents, policyName, prefix);
    }

    if (getStatusCode == 404) {
      return createNewPolicy(operationContext, esComponents, endpoint, policyJson, policyName);
    }

    // Handle other GET errors - these are retryable (like the Docker script)
    String getResponseBody = extractResponseBody(getResponse);
    log.warn(
        "Failed to check ISM policy existence. Status: {}. Response: {}. Will retry.",
        getStatusCode,
        getResponseBody);
    throw new RuntimeException("Retryable error: " + getStatusCode + " - " + getResponseBody);
  }

  /**
   * Handles ResponseException from GET request.
   *
   * @param e the ResponseException
   * @param esComponents the Elasticsearch components factory
   * @param policyName the name of the ISM policy
   * @param prefix the prefix for index patterns
   * @param endpoint the API endpoint
   * @param policyJson the policy JSON to create
   * @param operationContext the operation context for JSON parsing
   * @return true if successful, false if retry is needed
   */
  private static boolean handleResponseException(
      ResponseException e,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix,
      String endpoint,
      String policyJson,
      OperationContext operationContext) {

    int statusCode = e.getResponse().getStatusLine().getStatusCode();
    String responseBody = extractResponseBody(e.getResponse());

    if (statusCode == 200) {
      log.info("ISM policy {} already exists (from exception), updating it", policyName);
      return handleExistingPolicy(operationContext, esComponents, policyName, prefix);
    }

    if (statusCode == 404) {
      log.info("ISM policy {} doesn't exist (from exception), creating it", policyName);
      return createNewPolicy(operationContext, esComponents, endpoint, policyJson, policyName);
    }

    // Handle all other errors as retryable (including 400 with .opendistro-ism-config)
    log.warn(
        "ISM policy operation failed with status: {}. Response: {}. Will retry.",
        statusCode,
        responseBody);
    throw new RuntimeException("Retryable error: " + statusCode + " - " + responseBody);
  }

  /**
   * Handles the case when an ISM policy already exists by updating it.
   *
   * @param operationContext the operation context for JSON parsing
   * @param esComponents the Elasticsearch components factory
   * @param policyName the name of the ISM policy
   * @param prefix the prefix for index patterns
   * @return true if successful, false otherwise
   */
  private static boolean handleExistingPolicy(
      OperationContext operationContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix) {

    log.info("ISM policy {} already exists, updating it", policyName);
    try {
      updateIsmPolicy(esComponents, policyName, prefix, operationContext);
      return true;
    } catch (Exception updateException) {
      log.warn(
          "Failed to update existing ISM policy {} (non-fatal): {}",
          policyName,
          updateException.getMessage());
      return true; // Still consider this success since policy exists
    }
  }

  /**
   * Creates a new ISM policy.
   *
   * @param operationContext the operation context
   * @param esComponents the Elasticsearch components factory
   * @param endpoint the API endpoint
   * @param policyJson the policy JSON to create
   * @param policyName the name of the ISM policy
   * @return true if successful, false otherwise
   */
  private static boolean createNewPolicy(
      OperationContext operationContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String endpoint,
      String policyJson,
      String policyName) {

    log.info("ISM policy {} doesn't exist, creating it", policyName);

    try {
      RawResponse createResponse =
          IndexUtils.performPutRequest(operationContext, esComponents, endpoint, policyJson);
      int createStatusCode = createResponse.getStatusLine().getStatusCode();

      if (createStatusCode == 200 || createStatusCode == 201) {
        log.info("Successfully created ISM policy: {}", policyName);
        return true;
      }

      if (createStatusCode == 409) {
        log.info("ISM policy {} already exists", policyName);
        return true; // Consider this a success since policy exists
      }

      log.error("ISM policy creation returned status: {}", createStatusCode);
      return false;
    } catch (IOException e) {
      log.error("Failed to create ISM policy {}: {}", policyName, e.getMessage());
      return false;
    }
  }

  /**
   * Loads the ISM policy template and applies the prefix.
   *
   * @param prefix the prefix to apply to policy configurations
   * @return the policy JSON with the prefix applied
   * @throws IOException if there's an error reading the policy template
   */
  private static String loadPolicyTemplate(String prefix) throws IOException {
    return IndexUtils.loadResourceAsString("/index/usage-event/opensearch_policy.json")
        .replace("PREFIX", prefix);
  }

  /**
   * Extracts the response body from a RawResponse.
   *
   * @param response the RawResponse
   * @return the response body as a string, or a default message if extraction fails
   */
  private static String extractResponseBody(RawResponse response) {
    if (response.getEntity() == null) {
      return "No response body";
    }

    try {
      return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Error reading response body: " + e.getMessage();
    }
  }

  /**
   * Extracts the response body from a Response.
   *
   * @param response the Response
   * @return the response body as a string, or a default message if extraction fails
   */
  private static String extractResponseBody(Response response) {
    if (response.getEntity() == null) {
      return "No response body";
    }

    try {
      return new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "Error reading response body: " + e.getMessage();
    }
  }

  /**
   * Creates an index template for Elasticsearch usage events.
   *
   * <p>This method creates an index template that defines the structure and settings for usage
   * event indices in standard Elasticsearch environments. The template is loaded from the resource
   * file {@code /index/usage-event/elasticsearch_template.json} and configured with the specified
   * parameters.
   *
   * <p>The template includes:
   *
   * <ul>
   *   <li>Index patterns for matching usage event indices
   *   <li>Field mappings for DataHub usage event properties
   *   <li>ILM policy association for lifecycle management
   *   <li>Shard and replica configuration
   * </ul>
   *
   * <p>The method uses the low-level REST client to make a PUT request to the {@code
   * /_index_template/{templateName}} endpoint, which provides upsert behavior.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param templateName the name of the index template to create (e.g.,
   *     "datahub_usage_event_index_template")
   * @param policyName the name of the ILM policy to associate with the template
   * @param numShards the number of shards to configure for indices created from this template
   * @param numReplicas the number of replicas to configure for indices created from this template
   * @param prefix the index prefix to apply to template configurations (e.g., "prod_")
   * @throws IOException if there's an error reading the template or making the request
   * @throws ResponseException if the request fails with a non-409 status code
   */
  public static void createIndexTemplate(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String templateName,
      String policyName,
      int numShards,
      int numReplicas,
      String prefix)
      throws IOException {
    try {
      String templateJson =
          IndexUtils.loadResourceAsString("/index/usage-event/elasticsearch_template.json");
      // Replace placeholders
      templateJson = templateJson.replace("PREFIX", prefix);
      templateJson = templateJson.replace("DUE_SHARDS", String.valueOf(numShards));
      templateJson = templateJson.replace("DUE_REPLICAS", String.valueOf(numReplicas));

      // Use the low-level client for index templates
      String endpoint = "/_index_template/" + templateName;

      RawResponse response =
          IndexUtils.performPutRequest(opContext, esComponents, endpoint, templateJson);

      if (response.getStatusLine().getStatusCode() == 200
          || response.getStatusLine().getStatusCode() == 201) {
        log.info("Successfully created index template: {}", templateName);
      } else {
        log.warn(
            "Index template creation returned status: {}",
            response.getStatusLine().getStatusCode());
      }
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("Index template {} already exists", templateName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates an index template for AWS OpenSearch usage events.
   *
   * <p>This method creates an index template that defines the structure and settings for usage
   * event indices in AWS OpenSearch environments. The template is loaded from the resource file
   * {@code /index/usage-event/opensearch_template.json} and configured with the specified
   * parameters.
   *
   * <p>The template includes:
   *
   * <ul>
   *   <li>Index patterns for matching usage event indices
   *   <li>Field mappings for DataHub usage event properties
   *   <li>ISM rollover alias configuration for lifecycle management
   *   <li>Shard and replica configuration
   * </ul>
   *
   * <p>The method uses the low-level REST client to make a PUT request to the {@code
   * /_template/{templateName}} endpoint, which is the legacy template API used by AWS OpenSearch.
   * This provides upsert behavior.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param templateName the name of the index template to create (e.g.,
   *     "datahub_usage_event_index_template")
   * @param numShards the number of shards to configure for indices created from this template
   * @param numReplicas the number of replicas to configure for indices created from this template
   * @param prefix the index prefix to apply to template configurations (e.g., "prod_")
   * @throws IOException if there's an error reading the template or making the request
   * @throws ResponseException if the request fails with a non-409 status code
   */
  public static void createOpenSearchIndexTemplate(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String templateName,
      int numShards,
      int numReplicas,
      String prefix)
      throws IOException {
    try {
      String templateJson;
      String endpoint;

      // Both AWS OpenSearch Service and self-hosted OpenSearch use the same endpoint and template
      // format
      templateJson = IndexUtils.loadResourceAsString("/index/usage-event/opensearch_template.json");
      endpoint = "/_index_template/" + templateName;

      // Replace placeholders
      templateJson = templateJson.replace("PREFIX", prefix);
      templateJson = templateJson.replace("DUE_SHARDS", String.valueOf(numShards));
      templateJson = templateJson.replace("DUE_REPLICAS", String.valueOf(numReplicas));

      // Use the low-level client to make the PUT request
      RawResponse response =
          IndexUtils.performPutRequest(opContext, esComponents, endpoint, templateJson);

      if (response.getStatusLine().getStatusCode() == 200
          || response.getStatusLine().getStatusCode() == 201) {
        log.info("Successfully created/updated OpenSearch index template: {}", templateName);
      } else {
        log.warn(
            "OpenSearch index template creation returned status: {}",
            response.getStatusLine().getStatusCode());
      }
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("OpenSearch index template {} already exists", templateName);
      } else if (e.getResponse().getStatusLine().getStatusCode() == 400) {
        // Handle 400 Bad Request - this may indicate template format issues or unsupported features
        log.warn(
            "Index template creation failed with 400 Bad Request. This may indicate an issue with the template format or unsupported features. Template: {}",
            templateName);
        throw e;
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates a data stream for Elasticsearch usage events.
   *
   * <p>This method creates a data stream that serves as the primary storage mechanism for usage
   * events in standard Elasticsearch environments. Data streams provide automatic rollover and
   * lifecycle management when combined with ILM policies.
   *
   * <p>The method first checks if the data stream already exists using the high-level client's
   * {@code indexExists} method. If it doesn't exist, it creates a new index with the data stream
   * name, which effectively creates the data stream.
   *
   * <p>Data streams in Elasticsearch are designed for time-series data and provide:
   *
   * <ul>
   *   <li>Automatic index rollover based on size, age, or document count
   *   <li>Seamless querying across multiple backing indices
   *   <li>Integration with ILM policies for retention management
   * </ul>
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param dataStreamName the name of the data stream to create (e.g., "datahub_usage_event")
   * @throws IOException if there's an error checking existence or creating the data stream
   * @throws OpenSearchStatusException if the creation fails with a non-"already exists" error
   */
  public static void createDataStream(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String dataStreamName)
      throws IOException {
    try {
      // Check if the data stream already exists first (matches pattern in
      // createOpenSearchUsageEventIndex)
      String endpoint = "/_data_stream/" + dataStreamName;

      try {
        RawResponse getResponse = IndexUtils.performGetRequest(opContext, esComponents, endpoint);
        int statusCode = getResponse.getStatusLine().getStatusCode();
        if (statusCode == 200) {
          log.info("Data stream {} already exists", dataStreamName);
          return;
        }
      } catch (IOException e) {
        // Handle ResponseException from both ES8 and OpenSearch clients
        // Check if this is a 404 "not found" exception by examining the message
        String message = e.getMessage();
        if (message != null && message.contains("404")) {
          // Data stream doesn't exist, proceed with creation
          log.debug("Data stream {} does not exist, will create", dataStreamName);
        } else {
          // Unexpected error checking existence
          throw e;
        }
      }

      // Use the low-level REST client to create the data stream using the proper API
      // Elasticsearch requires using PUT /_data_stream/{name} for data stream creation
      // when the template has "data_stream": {} configured
      try {
        RawResponse response = IndexUtils.performPutRequest(opContext, esComponents, endpoint, "");

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 || statusCode == 201) {
          log.info("Successfully created data stream: {}", dataStreamName);
        } else {
          log.warn(
              "Data stream creation returned unexpected status {}: {}", statusCode, dataStreamName);
        }
      } catch (IOException e) {
        // Handle ResponseException from both ES8 and OpenSearch clients
        String message = e.getMessage();
        if (message != null && message.contains("resource_already_exists_exception")) {
          log.info("Data stream {} already exists", dataStreamName);
        } else {
          throw e;
        }
      }
    } catch (OpenSearchStatusException e) {
      if (e.getMessage().contains("resource_already_exists_exception")
          || (e.status().getStatus() == 400 && e.getMessage().contains("already exists"))) {
        log.info("Data stream {} already exists", dataStreamName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates an index with a write alias in a single request.
   *
   * <p>This method uses the common syntax supported by both Elasticsearch and OpenSearch to create
   * an index and assign a write alias atomically. This is more efficient than creating the index
   * and alias separately.
   *
   * @param opContext the operation context
   * @param esComponents the Elasticsearch/OpenSearch components factory
   * @param indexName the name of the index to create
   * @param aliasName the name of the alias to assign with is_write_index=true
   * @throws IOException if there's an error creating the index
   */
  private static void createIndexWithWriteAlias(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      String aliasName)
      throws IOException {
    String indexJson = String.format("{\"aliases\":{\"%s\":{\"is_write_index\":true}}}", aliasName);

    CreateIndexRequest request = new CreateIndexRequest(indexName);
    request.source(indexJson, XContentType.JSON);

    CreateIndexResponse response =
        esComponents.getSearchClient().createIndex(opContext, request, RequestOptions.DEFAULT);

    if (response.isAcknowledged()) {
      log.info("Successfully created index: {} with write alias: {}", indexName, aliasName);
    } else {
      log.warn("Index creation not acknowledged: {}", indexName);
    }
  }

  /**
   * Creates an initial index for OpenSearch usage events.
   *
   * <p>This method creates the first index in a time-series setup for usage events in OpenSearch
   * environments. Unlike Elasticsearch data streams, OpenSearch uses numbered indices (e.g.,
   * "datahub_usage_event-000001") with aliases for rollover management.
   *
   * <p>The method first checks if the specific index already exists. If it does, it skips creation.
   * Then it checks if any index with the expected alias already exists. If an index with the alias
   * exists, it skips creation. Otherwise, it creates a new index with the alias.
   *
   * <p>The created index includes:
   *
   * <ul>
   *   <li>An alias pointing to the index for write operations
   *   <li>ISM rollover alias configuration for automatic rollover
   *   <li>Proper field mappings from the associated index template
   * </ul>
   *
   * <p>This index serves as the initial write target, and ISM policies will automatically create
   * subsequent numbered indices (000002, 000003, etc.) based on rollover conditions.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param indexName the name of the initial index to create (e.g., "datahub_usage_event-000001")
   * @param aliasName the name of the alias to assign to the index (e.g., "datahub_usage_event")
   * @throws IOException if there's an error reading the index configuration or making the request
   * @throws OpenSearchStatusException if the creation fails with a non-"already exists" error
   */
  public static void createOpenSearchUsageEventIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      String aliasName)
      throws IOException {
    try {
      // Check if the specific index already exists
      GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
      boolean indexExists =
          esComponents
              .getSearchClient()
              .indexExists(opContext, getIndexRequest, RequestOptions.DEFAULT);

      if (indexExists) {
        log.info("OpenSearch index {} already exists - skipping creation", indexName);
        return;
      }

      // Check if any index with the expected alias already exists
      GetAliasesRequest aliasRequest = new GetAliasesRequest(aliasName);
      GetAliasesResponse aliasResponse =
          esComponents
              .getSearchClient()
              .getIndexAliases(opContext, aliasRequest, RequestOptions.DEFAULT);

      if (!aliasResponse.getAliases().isEmpty()) {
        log.info(
            "Index with alias {} already exists (indices: {}). Skipping creation of {}",
            aliasName,
            aliasResponse.getAliases().keySet(),
            indexName);
        return;
      }

      // No index with the alias exists, create a new one
      log.info("Creating new OpenSearch index: {} with alias: {}", indexName, aliasName);
      createIndexWithWriteAlias(opContext, esComponents, indexName, aliasName);
    } catch (OpenSearchStatusException e) {
      if (e.getMessage().contains("resource_already_exists_exception")
          || (e.status().getStatus() == 400 && e.getMessage().contains("already exists"))) {
        log.info("OpenSearch index {} already exists", indexName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Moves a usage event index that was auto-created before its index template existed onto the
   * layout the template defines: a data stream on Elasticsearch, a rollover alias over numbered
   * indices on OpenSearch.
   *
   * <p>A usage event written while the template is missing creates a concrete index with the data
   * stream or alias name and dynamic mappings: {@code type} becomes {@code text}, so sorting and
   * term filters on it fail, and the managed layout can never be created next to it.
   *
   * <p>The index is write-blocked and cloned to a {@code
   * <prefix>legacy_datahub_usage_event_<millis>} backup. Only once the backup is started and holds
   * every event is the index replaced by the managed layout, and then the backup is reindexed into
   * it. Usage events written during the few seconds the block is in place are rejected.
   *
   * <p>Each backup is copied back by a single reindex task that is recorded on the backup and never
   * started again, because a second copy would duplicate events once the layout has rolled over.
   * The backup is deleted when that task accounts for every event in it and kept otherwise; a task
   * still running after a few minutes is checked again on the next run.
   *
   * @param prefix the index prefix (e.g., "prod_")
   * @param useOpenSearch whether to build the OpenSearch layout instead of a data stream
   */
  public static void migrateLegacyUsageEventIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix,
      boolean useOpenSearch)
      throws IOException, InterruptedException {
    String indexName = prefix + "datahub_usage_event";
    // Nothing locks against two system-update runs at once; each could leave a backup behind.
    if (resolveIndices(opContext, esComponents, indexName).contains(indexName)
        && dropStaleBackups(opContext, esComponents, prefix, indexName)) {
      replaceLegacyIndex(
          opContext,
          esComponents,
          indexName,
          prefix + LEGACY_BACKUP_INFIX + System.currentTimeMillis(),
          useOpenSearch);
    }
    for (String backupName :
        resolveIndices(opContext, esComponents, prefix + LEGACY_BACKUP_INFIX + "*")) {
      try {
        backfillFromBackup(opContext, esComponents, backupName, indexName, !useOpenSearch);
      } catch (IOException | RuntimeException e) {
        log.error(
            "Could not copy usage events from {} back into {}; keeping {}",
            backupName,
            indexName,
            backupName,
            e);
      }
    }
  }

  /**
   * Deletes backups that an earlier, failed attempt cloned from the current legacy index: they have
   * no copy task and are younger than the index, so the index still holds all their events. Without
   * this, retried attempts pile up overlapping backups that are each copied back.
   */
  private static boolean dropStaleBackups(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix,
      String indexName)
      throws IOException {
    long indexCreated = creationDate(opContext, esComponents, indexName);
    if (indexCreated <= 0) {
      // No longer a plain index, so another run has migrated it meanwhile.
      return false;
    }
    for (String backupName :
        resolveIndices(opContext, esComponents, prefix + LEGACY_BACKUP_INFIX + "*")) {
      if (backfillMeta(opContext, esComponents, backupName).path(BACKFILL_TASK_META).isMissingNode()
          && creationDate(opContext, esComponents, backupName) > indexCreated
          && resolveIndices(opContext, esComponents, indexName).contains(indexName)) {
        log.info("Deleting {}, left by an earlier attempt to migrate {}", backupName, indexName);
        deleteIndex(opContext, esComponents, backupName);
      }
    }
    return true;
  }

  private static long creationDate(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName)
      throws IOException {
    return readJson(
            opContext,
            IndexUtils.performGetRequest(
                opContext, esComponents, "/" + indexName + "/_settings/index.creation_date"))
        .path(indexName)
        .path("settings")
        .path("index")
        .path("creation_date")
        .asLong();
  }

  private static JsonNode backfillMeta(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String backupName)
      throws IOException {
    return readJson(
            opContext,
            IndexUtils.performGetRequest(opContext, esComponents, "/" + backupName + "/_mapping"))
        .path(backupName)
        .path("mappings")
        .path("_meta");
  }

  private static void replaceLegacyIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      String backupName,
      boolean useOpenSearch)
      throws IOException {
    log.warn(
        "Usage event index {} was created without its index template; moving it to {} and"
            + " recreating it from the template",
        indexName,
        backupName);
    boolean originalDeleted = false;
    try {
      // Unlike the index setting, the block API waits for in-flight writes to finish.
      IndexUtils.performPutRequest(opContext, esComponents, "/" + indexName + "/_block/write", "");
      // The backup only serves reads from here on, so it does not keep the block.
      JsonNode clone =
          readJson(
              opContext,
              IndexUtils.performPostRequest(
                  opContext,
                  esComponents,
                  "/" + indexName + "/_clone/" + backupName,
                  String.format(
                      "{\"settings\":{\"%s\":null}}", IndexUtils.INDEX_BLOCKS_WRITE_SETTING)));
      long events = count(opContext, esComponents, indexName);
      if (!clone.path("shards_acknowledged").asBoolean()
          || count(opContext, esComponents, backupName) != events) {
        // The original is untouched and still holds every event, so drop the incomplete copy.
        deleteIndex(opContext, esComponents, backupName);
        throw new IOException(
            String.format(
                "Backup %s of %s did not start with all %d events", backupName, indexName, events));
      }
      if (useOpenSearch) {
        replaceWithRolloverAlias(opContext, esComponents, indexName);
      } else {
        deleteIndex(opContext, esComponents, indexName);
        originalDeleted = true;
        createDataStream(opContext, esComponents, indexName);
      }
    } catch (IOException | RuntimeException e) {
      if (!originalDeleted
          && !IndexUtils.retryWithBackoff(
              3,
              1000,
              () -> {
                setWriteBlock(opContext, esComponents, indexName, false);
                return true;
              })) {
        e.addSuppressed(new IOException("Could not lift the write block on " + indexName));
      }
      throw e;
    }
  }

  private static void replaceWithRolloverAlias(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String aliasName)
      throws IOException {
    String firstIndex = aliasName + "-000001";
    boolean created = !resolveIndices(opContext, esComponents, firstIndex).contains(firstIndex);
    if (created) {
      IndexUtils.performPutRequest(opContext, esComponents, "/" + firstIndex, "{}");
    }
    try {
      // One cluster state update, so no usage event write can recreate a bare index in between.
      IndexUtils.performPostRequest(
          opContext,
          esComponents,
          "/_aliases",
          String.format(
              "{\"actions\":[{\"add\":{\"index\":\"%s\",\"alias\":\"%s\",\"is_write_index\":true}},"
                  + "{\"remove_index\":{\"index\":\"%s\"}}]}",
              firstIndex, aliasName, aliasName));
    } catch (IOException e) {
      // The swap is atomic, so while the original is still a plain index it did not happen and
      // the new index is empty: drop it rather than leave it behind without its alias.
      if (created && resolveIndices(opContext, esComponents, aliasName).contains(aliasName)) {
        deleteIndex(opContext, esComponents, firstIndex);
      }
      throw e;
    }
  }

  private static void backfillFromBackup(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String backupName,
      String indexName,
      boolean dataStream)
      throws IOException, InterruptedException {
    JsonNode meta = backfillMeta(opContext, esComponents, backupName);
    String taskId = meta.path(BACKFILL_TASK_META).asText();
    String recordedWriteIndex = meta.path(BACKFILL_WRITE_INDEX_META).asText();
    String writeIndex = writeIndex(opContext, esComponents, indexName, dataStream);
    // A copy may have run untracked: its start was recorded but not its task, or the cluster
    // forgot the task (for example after a restart); either way it may have stopped part way.
    if (!recordedWriteIndex.isEmpty()
        && (taskId.isEmpty() || getTask(opContext, esComponents, taskId) == null)) {
      if (writeIndex.isEmpty() || !writeIndex.equals(recordedWriteIndex)) {
        log.error(
            "An earlier copy of {} into {} did not finish and {} has rolled over since, so"
                + " copying again could duplicate events. {} is kept; copy the remaining events"
                + " yourself, then delete it",
            backupName,
            recordedWriteIndex,
            indexName,
            backupName);
        return;
      }
      log.warn(
          "An earlier copy of {} did not finish; copying it again into {}, which skips events"
              + " already there",
          backupName,
          writeIndex);
      taskId = "";
    }
    if (taskId.isEmpty()) {
      if (writeIndex.isEmpty()) {
        throw new IOException("Could not find the write index behind " + indexName);
      }
      recordCopy(opContext, esComponents, backupName, "", writeIndex);
      String reindex =
          String.format(
              "{\"conflicts\":\"proceed\",\"source\":{\"index\":\"%s\"},"
                  + "\"dest\":{\"index\":\"%s\",\"op_type\":\"create\"},"
                  + "\"script\":{\"lang\":\"painless\",\"source\":\"%s\","
                  + "\"params\":{\"dataStream\":%s}}}",
              backupName, indexName, BACKFILL_SCRIPT, dataStream);
      taskId =
          readJson(
                  opContext,
                  IndexUtils.performPostRequest(
                      opContext, esComponents, "/_reindex?wait_for_completion=false", reindex))
              .path("task")
              .asText();
      if (taskId.isEmpty()) {
        throw new IOException("Reindex from " + backupName + " did not return a task id");
      }
      recordCopy(opContext, esComponents, backupName, taskId, writeIndex);
    }
    JsonNode task = waitForTask(opContext, esComponents, taskId);
    if (task == null) {
      log.info(
          "Copying usage events from {} back into {} (task {}) is still running; the next run"
              + " checks it again",
          backupName,
          indexName,
          taskId);
      return;
    }
    long expected = count(opContext, esComponents, backupName);
    JsonNode result = task.path("response");
    long total = result.path("total").asLong();
    long created = result.path("created").asLong();
    long present = result.path("version_conflicts").asLong();
    long dropped = result.path("noops").asLong();
    JsonNode failures = result.path("failures");
    if (task.has("error")
        || !failures.isEmpty()
        || total != expected
        || created + present + dropped != total) {
      log.error(
          "Copying usage events from {} back into {} (task {}) is incomplete: {} of {} copied,"
              + " first error: {}. {} is kept and not copied again, because a second copy can"
              + " duplicate events once the index has rolled over; copy the remaining events"
              + " yourself, then delete it",
          backupName,
          indexName,
          taskId,
          created + present,
          expected,
          task.has("error") ? task.path("error") : failures.path(0),
          backupName);
      return;
    }
    deleteIndex(opContext, esComponents, backupName);
    log.info(
        "Copied {} usage events from {} back into {} ({} already present, {} without a timestamp"
            + " dropped) and deleted {}",
        created,
        backupName,
        indexName,
        present,
        dropped,
        backupName);
  }

  private static void recordCopy(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String backupName,
      String taskId,
      String writeIndex)
      throws IOException {
    IndexUtils.performPutRequest(
        opContext,
        esComponents,
        "/" + backupName + "/_mapping",
        String.format(
            "{\"_meta\":{\"%s\":\"%s\",\"%s\":\"%s\"}}",
            BACKFILL_TASK_META, taskId, BACKFILL_WRITE_INDEX_META, writeIndex));
  }

  @Nullable
  private static JsonNode waitForTask(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String taskId)
      throws IOException, InterruptedException {
    long deadline = System.currentTimeMillis() + BACKFILL_WAIT.toMillis();
    while (true) {
      JsonNode task = getTask(opContext, esComponents, taskId);
      if (task != null && task.path("completed").asBoolean()) {
        return task;
      }
      // A task that disappears meanwhile is handled like one still running: the next run sees it
      // is gone.
      if (task == null || System.currentTimeMillis() >= deadline) {
        return null;
      }
      Thread.sleep(BACKFILL_POLL_INTERVAL.toMillis());
    }
  }

  /**
   * The task's status, or null when the cluster no longer knows it (for example after a restart).
   */
  @Nullable
  private static JsonNode getTask(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String taskId)
      throws IOException {
    Request request = new Request("GET", "/_tasks/" + taskId);
    request.addParameter("ignore", "404");
    RawResponse response =
        esComponents.getSearchClient().performLowLevelRequest(opContext, request);
    return response.getStatusLine().getStatusCode() == 404 ? null : readJson(opContext, response);
  }

  /** The index new usage events currently go to, behind the data stream or rollover alias. */
  private static String writeIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      boolean dataStream)
      throws IOException {
    if (dataStream) {
      JsonNode backingIndices =
          readJson(
                  opContext,
                  IndexUtils.performGetRequest(
                      opContext, esComponents, "/_data_stream/" + indexName))
              .path("data_streams")
              .path(0)
              .path("indices");
      return backingIndices.path(backingIndices.size() - 1).path("index_name").asText();
    }
    JsonNode indices =
        readJson(
            opContext,
            IndexUtils.performGetRequest(opContext, esComponents, "/_alias/" + indexName));
    Iterator<Map.Entry<String, JsonNode>> it = indices.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> index = it.next();
      if (index.getValue().path("aliases").path(indexName).path("is_write_index").asBoolean()) {
        return index.getKey();
      }
    }
    return "";
  }

  /** Concrete indices matching the expression; aliases and data streams are not included. */
  private static List<String> resolveIndices(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String expression)
      throws IOException {
    Request request = new Request("GET", "/_resolve/index/" + expression);
    // Elasticsearch answers a missing name with 404, OpenSearch with an empty result.
    request.addParameter("ignore", "404");
    RawResponse response =
        esComponents.getSearchClient().performLowLevelRequest(opContext, request);
    List<String> names = new ArrayList<>();
    if (response.getStatusLine().getStatusCode() != 404) {
      readJson(opContext, response)
          .path("indices")
          .forEach(index -> names.add(index.path("name").asText()));
    }
    return names;
  }

  private static long count(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName)
      throws IOException {
    IndexUtils.performPostRequest(opContext, esComponents, "/" + indexName + "/_refresh", "");
    return readJson(
            opContext,
            IndexUtils.performGetRequest(opContext, esComponents, "/" + indexName + "/_count"))
        .path("count")
        .asLong();
  }

  private static void setWriteBlock(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      boolean blocked)
      throws IOException {
    Request request = new Request("PUT", "/" + indexName + "/_settings");
    request.setJsonEntity(
        String.format("{\"%s\":%s}", IndexUtils.INDEX_BLOCKS_WRITE_SETTING, blocked));
    // Harmless on an alias or data stream, and nothing to do if the index is gone.
    request.addParameter("ignore", "404");
    esComponents.getSearchClient().performLowLevelRequest(opContext, request);
  }

  private static void deleteIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName)
      throws IOException {
    log.info("DELETE => /{}", indexName);
    esComponents
        .getSearchClient()
        .performLowLevelRequest(opContext, new Request("DELETE", "/" + indexName));
  }

  private static JsonNode readJson(OperationContext opContext, RawResponse response)
      throws IOException {
    return opContext.getObjectMapper().readTree(response.getEntity().getContent());
  }

  /**
   * Updates an existing ISM policy for AWS OpenSearch usage events.
   *
   * <p>This method updates an existing ISM policy using optimistic concurrency control with
   * sequence numbers and primary terms. It first retrieves the current policy to get the sequence
   * number and primary term, then updates the policy with the new configuration.
   *
   * <p>The method is non-fatal - if the policy cannot be updated (e.g., due to concurrent
   * modifications), it logs a warning but does not throw an exception. This matches the behavior of
   * the Docker script.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param policyName the name of the ISM policy to update (e.g., "datahub_usage_event_policy")
   * @param prefix the index prefix to apply to policy configurations (e.g., "prod_")
   * @throws IOException if there's an error reading the policy template or making the request
   */
  public static void updateIsmPolicy(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix,
      OperationContext operationContext)
      throws IOException {
    try {
      String endpoint = "/_plugins/_ism/policies/" + policyName;

      // Get existing policy to retrieve sequence number and primary term
      RawResponse getResponse =
          IndexUtils.performGetRequest(operationContext, esComponents, endpoint);

      if (getResponse.getStatusLine().getStatusCode() != 200) {
        log.warn("Could not get ISM policy {} for update. Ignoring.", policyName);
        return;
      }

      String responseBody =
          new String(getResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

      // Parse sequence number and primary term from response
      // The response format is: {"policy_id": "...", "_seq_no": 123, "_primary_term": 456,
      // "policy": {...}}
      int seqNo = IndexUtils.extractJsonValue(operationContext, responseBody, "_seq_no");
      int primaryTerm =
          IndexUtils.extractJsonValue(operationContext, responseBody, "_primary_term");

      if (seqNo == -1 || primaryTerm == -1) {
        log.warn(
            "Could not extract sequence number or primary term from ISM policy {}. Skipping update.",
            policyName);
        return;
      }

      // Load new policy configuration
      String policyJson =
          IndexUtils.loadResourceAsString("/index/usage-event/opensearch_policy.json");
      policyJson = policyJson.replace("PREFIX", prefix);

      // Update policy with optimistic concurrency control
      String queryParams = "?if_seq_no=" + seqNo + "&if_primary_term=" + primaryTerm;
      RawResponse updateResponse =
          IndexUtils.performPutRequestWithParams(
              operationContext, esComponents, endpoint, queryParams, policyJson);

      if (updateResponse.getStatusLine().getStatusCode() == 200
          || updateResponse.getStatusLine().getStatusCode() == 201) {
        log.info("Successfully updated ISM policy: {}", policyName);
      } else {
        log.warn(
            "Failed to update ISM policy {} after retries (non-fatal). Status: {}",
            policyName,
            updateResponse.getStatusLine().getStatusCode());
      }

    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.warn("ISM policy {} was modified concurrently. Skipping update.", policyName);
      } else if (e.getResponse().getStatusLine().getStatusCode() == 400) {
        log.warn(
            "Failed to update ISM policy {} (non-fatal). This may indicate that ISM policies are not supported in this environment. Status: {}",
            policyName,
            e.getResponse().getStatusLine().getStatusCode());
      } else {
        log.warn(
            "Failed to update ISM policy {} (non-fatal). Status: {}",
            policyName,
            e.getResponse().getStatusLine().getStatusCode());
      }
    } catch (Exception e) {
      log.warn(
          "Unexpected error updating ISM policy {} (non-fatal): {}", policyName, e.getMessage());
    }
  }
}
