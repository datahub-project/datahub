package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
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
  // A younger backup may belong to a migration still running elsewhere, so it is left alone.
  private static final Duration STALE_BACKUP_AGE = Duration.ofMinutes(10);
  private static final Duration BLOCKED_INDEX_POLL_INTERVAL = Duration.ofSeconds(30);
  // How long a write-blocked legacy index may wait on a recent clone before its block is lifted.
  private static final Duration BLOCKED_INDEX_MAX_WAIT = STALE_BACKUP_AGE.plusMinutes(1);
  private static Duration blockedIndexPollInterval = BLOCKED_INDEX_POLL_INTERVAL;
  private static Duration blockedIndexMaxWait = BLOCKED_INDEX_MAX_WAIT;
  // Completes <prefix>legacy_datahub_usage_event_ into the lease name, so the lease sits under the
  // backup pattern (credentials that may create backups may usually create it) and scans skip it.
  private static final String LEASE_NAME_SUFFIX = "lease";
  private static final String LEASE_OWNER_META = "datahub_lease_owner";
  // Longer than a run holds the lease: the blocked-index wait, clone, verify, swap and starting the
  // copies. An older lease belongs to a run that died, or to one stalled so long it is treated so.
  private static final Duration LEASE_TTL = Duration.ofMinutes(30);
  private static Duration leaseTtl = LEASE_TTL;
  private static final String MIGRATION_METRIC =
      "datahub.system_update.legacy_usage_event_migration";
  private static final String OUTCOME_MANUAL_RECOVERY = "manual_recovery";
  private static final String OUTCOME_SKIPPED = "skipped";

  @VisibleForTesting
  public static void setBlockedIndexWaitForTesting(Duration pollInterval, Duration maxWait) {
    blockedIndexPollInterval = pollInterval;
    blockedIndexMaxWait = maxWait;
  }

  @VisibleForTesting
  public static void clearBlockedIndexWaitForTesting() {
    blockedIndexPollInterval = BLOCKED_INDEX_POLL_INTERVAL;
    blockedIndexMaxWait = BLOCKED_INDEX_MAX_WAIT;
  }

  @VisibleForTesting
  public static void setLeaseTtlForTesting(Duration ttl) {
    leaseTtl = ttl;
  }

  @VisibleForTesting
  public static void clearLeaseTtlForTesting() {
    leaseTtl = LEASE_TTL;
  }

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
   * Held by one system-update run at a time while it moves a legacy usage event index aside and
   * starts copying backups back, so two overlapping system-update pods do not both clone the index
   * or both copy one backup: two copies of a backup duplicate events when the index rolls over
   * between them.
   *
   * <p>The lease is an index because creating one is atomic: exactly one creator wins. The engines
   * cannot make an action conditional on the lease, or delete an index only while its owner is
   * unchanged, so the owner checks are best effort. A run stalled for longer than the lease lives
   * can still act once after another run took the lease over, and a release can race a takeover;
   * both need a run to stall for over {@link #LEASE_TTL} while another one runs.
   *
   * <p>A run that waits {@link #BLOCKED_INDEX_MAX_WAIT} on a write-blocked legacy index lifts the
   * block although the lease has not gone stale, since its holder may have died. A holder that is
   * alive checks right before it destroys the legacy index that the block is still there, so the
   * lift makes it stop; only a lift that lands between that check and the delete loses events.
   */
  public static final class LegacyMigrationLease {
    private final OperationContext opContext;
    private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
    private final String name;
    private final String owner;

    private LegacyMigrationLease(
        OperationContext opContext,
        BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
        String name,
        String owner) {
      this.opContext = opContext;
      this.esComponents = esComponents;
      this.name = name;
      this.owner = owner;
    }

    @VisibleForTesting
    static LegacyMigrationLease heldForTesting(
        OperationContext opContext,
        BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
        String prefix,
        String owner) {
      return new LegacyMigrationLease(opContext, esComponents, leaseName(prefix), owner);
    }

    /** Stops the caller before a destructive or duplicating action once it lost the lease. */
    private void checkHeld() throws IOException {
      if (!owner.equals(leaseOwner(opContext, esComponents, name))) {
        throw new IOException(
            name + " is no longer held by this run; stopping so the run that holds it continues");
      }
    }

    /** Deletes the lease if this run still holds it. */
    public void release() {
      try {
        if (owner.equals(leaseOwner(opContext, esComponents, name))) {
          deleteIndex(opContext, esComponents, name);
        }
      } catch (IOException | RuntimeException e) {
        log.warn("Could not release {}; another run takes it over after {}", name, leaseTtl, e);
      }
    }
  }

  /**
   * Acquires the lease when this run has work: a legacy usage event index to move aside, or a
   * backup to copy back into a layout that replaced it. Returns null when there is nothing to do,
   * or when this run must leave the work to another run because the lease is held, cannot be
   * created, or cannot be confirmed.
   *
   * @param owner identifies this run; the same owner acquires its own lease again on a step retry
   * @param moveAside whether this run may move a legacy index aside; a run that only copies back
   *     does not take the lease from one that could move the index
   */
  @Nullable
  public static LegacyMigrationLease acquireLegacyMigrationLease(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix,
      String owner,
      boolean moveAside) {
    String indexName = prefix + "datahub_usage_event";
    String leaseName = leaseName(prefix);
    try {
      boolean plainIndex = resolveIndices(opContext, esComponents, indexName).contains(indexName);
      if (plainIndex ? !moveAside : legacyBackups(opContext, esComponents, prefix).isEmpty()) {
        return null;
      }
      long waitUntil = System.currentTimeMillis() + blockedIndexMaxWait.toMillis();
      boolean tookOver = false;
      while (true) {
        if (createLease(opContext, esComponents, leaseName, owner) == 403) {
          log.error(
              "System update may not create {} (403), so it leaves {} and its backups alone; allow"
                  + " it to create and delete {}* indices",
              leaseName,
              indexName,
              prefix + LEGACY_BACKUP_INFIX);
          recordOutcome(opContext, OUTCOME_MANUAL_RECOVERY);
          return null;
        }
        // Whether the create succeeded, lost its response, or lost to another run, the owner
        // recorded on the lease decides.
        String holder = leaseOwner(opContext, esComponents, leaseName);
        if (owner.equals(holder)) {
          return new LegacyMigrationLease(opContext, esComponents, leaseName, owner);
        }
        if (holder == null) {
          log.warn(
              "Could not confirm {}; leaving {} and its backups to a later run",
              leaseName,
              indexName);
          recordOutcome(opContext, OUTCOME_SKIPPED);
          return null;
        }
        long heldFor =
            System.currentTimeMillis() - creationDate(opContext, esComponents, leaseName);
        if (!tookOver && heldFor > leaseTtl.toMillis()) {
          log.warn(
              "Taking over {} from {}, which has held it for {}, longer than {}",
              leaseName,
              holder,
              Duration.ofMillis(heldFor),
              leaseTtl);
          deleteIndex(opContext, esComponents, leaseName);
          tookOver = true;
          continue;
        }
        // If the run holding the lease died after blocking writes on the legacy index, nothing
        // else lifts that block, so wait for the lease to be released or to go stale, and lift the
        // block after the same wait as for a recent clone. A holder that is alive after all finds
        // the block gone and stops before it destroys the index.
        if (!plainIndex || !isWriteBlocked(opContext, esComponents, indexName)) {
          log.warn(
              "{} is held by another system-update run ({}); leaving {} and its backups to it",
              leaseName,
              holder,
              indexName);
          recordOutcome(opContext, OUTCOME_SKIPPED);
          return null;
        }
        if (System.currentTimeMillis() >= waitUntil) {
          log.error(
              "{} stayed write-blocked for {} while {} held {}; lifting the block and leaving the"
                  + " migration to a later run",
              indexName,
              blockedIndexMaxWait,
              holder,
              leaseName);
          liftWriteBlock(opContext, esComponents, indexName);
          recordOutcome(opContext, OUTCOME_SKIPPED);
          return null;
        }
        Thread.sleep(blockedIndexPollInterval.toMillis());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting for {}", leaseName, e);
    } catch (IOException | RuntimeException e) {
      log.error(
          "Could not acquire {}; leaving {} and its backups to a later run",
          leaseName,
          indexName,
          e);
      recordOutcome(opContext, OUTCOME_SKIPPED);
      // The create may have succeeded before the failure; do not leave this run's lease behind.
      new LegacyMigrationLease(opContext, esComponents, leaseName, owner).release();
    }
    return null;
  }

  private static String leaseName(String prefix) {
    return prefix + LEGACY_BACKUP_INFIX + LEASE_NAME_SUFFIX;
  }

  /**
   * Creates the lease for {@code owner}; returns the HTTP status, or -1 when there is no answer.
   */
  private static int createLease(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String leaseName,
      String owner) {
    Request request = new Request("PUT", "/" + leaseName);
    request.setJsonEntity(
        String.format(
            "{\"settings\":{\"index.number_of_shards\":1,\"index.number_of_replicas\":0},"
                + "\"mappings\":{\"_meta\":{\"%s\":\"%s\"}}}",
            LEASE_OWNER_META, owner));
    // 400 when another run created it first, 403 when these credentials may not create it.
    request.addParameter("ignore", "400,403");
    try {
      return esComponents
          .getSearchClient()
          .performLowLevelRequest(opContext, request)
          .getStatusLine()
          .getStatusCode();
    } catch (IOException | RuntimeException e) {
      // The index may exist anyway; the caller reads its owner.
      log.warn("Creating {} did not answer; checking whether it exists", leaseName, e);
      return -1;
    }
  }

  @Nullable
  private static String leaseOwner(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String leaseName)
      throws IOException {
    Request request = new Request("GET", "/" + leaseName + "/_mapping");
    request.addParameter("ignore", "404");
    RawResponse response =
        esComponents.getSearchClient().performLowLevelRequest(opContext, request);
    if (response.getStatusLine().getStatusCode() == 404) {
      return null;
    }
    JsonNode owner =
        readJson(opContext, response)
            .path(leaseName)
            .path("mappings")
            .path("_meta")
            .path(LEASE_OWNER_META);
    return owner.isMissingNode() ? null : owner.asText();
  }

  /** Backups left by moving a legacy index aside, not counting the lease. */
  private static List<String> legacyBackups(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix)
      throws IOException {
    List<String> backups =
        resolveIndices(opContext, esComponents, prefix + LEGACY_BACKUP_INFIX + "*");
    backups.remove(leaseName(prefix));
    return backups;
  }

  private static void recordOutcome(OperationContext opContext, String outcome) {
    opContext
        .getMetricUtils()
        .ifPresent(m -> m.incrementMicrometer(MIGRATION_METRIC, 1, "outcome", outcome));
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
   * <p>The caller then creates the layout and calls {@link #startLegacyBackupCopies}, which copies
   * the backup back even when this method failed after removing the original.
   *
   * @param prefix the index prefix (e.g., "prod_")
   * @param useOpenSearch whether to build the OpenSearch layout instead of a data stream
   * @param lease held by this run for the whole call
   */
  public static void moveLegacyUsageEventIndexAside(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix,
      boolean useOpenSearch,
      LegacyMigrationLease lease)
      throws IOException, InterruptedException {
    String indexName = prefix + "datahub_usage_event";
    long waitUntil = System.currentTimeMillis() + blockedIndexMaxWait.toMillis();
    while (resolveIndices(opContext, esComponents, indexName).contains(indexName)) {
      if (dropStaleBackups(opContext, esComponents, prefix, indexName, lease)) {
        replaceLegacyIndex(
            opContext,
            esComponents,
            indexName,
            prefix + LEGACY_BACKUP_INFIX + System.currentTimeMillis(),
            useOpenSearch,
            lease);
        return;
      }
      // A recent clone blocks a new attempt. If its attempt died after blocking writes, nothing
      // else lifts the block, so wait for that clone to go stale or its migration to finish.
      if (!isWriteBlocked(opContext, esComponents, indexName)) {
        return;
      }
      if (System.currentTimeMillis() >= waitUntil) {
        // Clones keep appearing, or the index cannot be dated; rather than leave every usage event
        // rejected, lift the block and let a later run migrate.
        log.error(
            "{} stayed write-blocked for {} while a recent clone held off its migration; lifting"
                + " the block",
            indexName,
            blockedIndexMaxWait);
        liftWriteBlock(opContext, esComponents, indexName);
        return;
      }
      Thread.sleep(blockedIndexPollInterval.toMillis());
    }
  }

  /**
   * Starts copying every backup back into the managed layout, or confirms a copy already runs. Each
   * backup is copied back by a single reindex task that is recorded on the backup, because a second
   * copy would duplicate events once the layout has rolled over; a running copy whose task id was
   * not recorded is found and recorded instead of starting another one.
   *
   * <p>While the usage event index is still a plain index nothing is copied: it still holds every
   * event, and its backups are handled when it is moved aside. Otherwise a backup holds the only
   * copy of its events, so failing to start a copy throws, failing the step so its retries try
   * again. Cases retries cannot fix keep the backup, log how to copy it by hand, and count on
   * {@link #MIGRATION_METRIC}.
   *
   * @param lease held by this run for the whole call
   */
  public static void startLegacyBackupCopies(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix,
      boolean useOpenSearch,
      LegacyMigrationLease lease)
      throws IOException {
    String indexName = prefix + "datahub_usage_event";
    List<String> backups = legacyBackups(opContext, esComponents, prefix);
    if (backups.isEmpty()
        || resolveIndices(opContext, esComponents, indexName).contains(indexName)) {
      return;
    }
    String writeIndex = managedWriteIndex(opContext, esComponents, indexName, useOpenSearch);
    if (writeIndex == null) {
      throw new IOException(
          String.format(
              "%s hold usage events, but %s is not a %s to copy them into",
              backups,
              indexName,
              useOpenSearch ? "rollover alias with one write index" : "data stream"));
    }
    List<String> notStarted = new ArrayList<>();
    for (String backupName : backups) {
      try {
        startCopy(
            opContext, esComponents, backupName, indexName, writeIndex, !useOpenSearch, lease);
      } catch (ManualRecoveryException e) {
        log.error(e.getMessage());
        recordOutcome(opContext, OUTCOME_MANUAL_RECOVERY);
      } catch (IOException | RuntimeException e) {
        log.error(
            "Could not start copying usage events from {} back into {}", backupName, indexName, e);
        notStarted.add(backupName);
      }
    }
    if (!notStarted.isEmpty()) {
      throw new IOException(
          String.format(
              "Could not start copying %s back into %s, which hold the only copy of those usage"
                  + " events",
              notStarted, indexName));
    }
  }

  /**
   * Waits a few minutes for each recorded copy and deletes its backup once the copy accounts for
   * every event in it. A copy still running is checked again on the next run; a copy that finished
   * incomplete keeps its backup for a person to finish, since copying again could duplicate events.
   * Needs no lease: it starts nothing and only deletes a backup its own recorded copy accounted
   * for.
   */
  public static void finishLegacyBackupCopies(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String prefix)
      throws InterruptedException {
    String indexName = prefix + "datahub_usage_event";
    List<String> backups;
    try {
      backups = legacyBackups(opContext, esComponents, prefix);
    } catch (IOException | RuntimeException e) {
      log.error("Could not list the backups of {}; the next run checks them", indexName, e);
      return;
    }
    for (String backupName : backups) {
      try {
        finishCopy(opContext, esComponents, backupName, indexName);
      } catch (ManualRecoveryException e) {
        log.error(e.getMessage());
        recordOutcome(opContext, OUTCOME_MANUAL_RECOVERY);
      } catch (IOException | RuntimeException e) {
        log.error(
            "Could not check copying {} back into {}; the next run checks it",
            backupName,
            indexName,
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
      String indexName,
      LegacyMigrationLease lease)
      throws IOException {
    long indexCreated = creationDate(opContext, esComponents, indexName);
    if (indexCreated <= 0) {
      // No longer a plain index, so another run has migrated it meanwhile.
      return false;
    }
    boolean recentCloneExists = false;
    for (String backupName : legacyBackups(opContext, esComponents, prefix)) {
      long backupCreated = creationDate(opContext, esComponents, backupName);
      if (!backfillMeta(opContext, esComponents, backupName)
              .path(BACKFILL_TASK_META)
              .isMissingNode()
          || backupCreated <= indexCreated) {
        continue;
      }
      if (System.currentTimeMillis() - backupCreated <= STALE_BACKUP_AGE.toMillis()) {
        recentCloneExists = true;
      } else if (resolveIndices(opContext, esComponents, indexName).contains(indexName)) {
        lease.checkHeld();
        log.info("Deleting {}, left by an earlier attempt to migrate {}", backupName, indexName);
        deleteIndex(opContext, esComponents, backupName);
      }
    }
    if (recentCloneExists) {
      // Another attempt may still be using that clone; a later run migrates once it is stale.
      log.info(
          "A migration of {} started less than {} ago may still be running; not starting another",
          indexName,
          STALE_BACKUP_AGE);
      return false;
    }
    return true;
  }

  private static boolean isWriteBlocked(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName)
      throws IOException {
    return readJson(
            opContext,
            IndexUtils.performGetRequest(
                opContext,
                esComponents,
                "/" + indexName + "/_settings/" + IndexUtils.INDEX_BLOCKS_WRITE_SETTING))
        .path(indexName)
        .path("settings")
        .path("index")
        .path("blocks")
        .path("write")
        .asBoolean();
  }

  /**
   * Stops before the legacy index is destroyed once its write block is gone: another run lifts it
   * after waiting {@link #BLOCKED_INDEX_MAX_WAIT}, and events written since then are not in the
   * backup.
   */
  private static void checkStillWriteBlocked(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName)
      throws IOException {
    if (!isWriteBlocked(opContext, esComponents, indexName)) {
      throw new IOException(
          indexName
              + " is no longer write-blocked, so it may hold events its backup does not; leaving it"
              + " in place");
    }
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
      boolean useOpenSearch,
      LegacyMigrationLease lease)
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
      lease.checkHeld();
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
        replaceWithRolloverAlias(opContext, esComponents, indexName, lease);
      } else {
        // The lease is checked last: a run that took it over may have blocked writes again.
        checkStillWriteBlocked(opContext, esComponents, indexName);
        lease.checkHeld();
        deleteIndex(opContext, esComponents, indexName);
        originalDeleted = true;
        createDataStream(opContext, esComponents, indexName);
      }
    } catch (IOException | RuntimeException e) {
      if (!originalDeleted && !liftWriteBlock(opContext, esComponents, indexName)) {
        e.addSuppressed(new IOException("Could not lift the write block on " + indexName));
      }
      throw e;
    }
  }

  private static void replaceWithRolloverAlias(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String aliasName,
      LegacyMigrationLease lease)
      throws IOException {
    String firstIndex = aliasName + "-000001";
    boolean created = !resolveIndices(opContext, esComponents, firstIndex).contains(firstIndex);
    if (created) {
      IndexUtils.performPutRequest(opContext, esComponents, "/" + firstIndex, "{}");
    }
    try {
      checkStillWriteBlocked(opContext, esComponents, aliasName);
      lease.checkHeld();
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
      try {
        if (created
            && resolveIndices(opContext, esComponents, aliasName).contains(aliasName)
            && count(opContext, esComponents, firstIndex) == 0) {
          deleteIndex(opContext, esComponents, firstIndex);
        }
      } catch (IOException | RuntimeException cleanupFailure) {
        e.addSuppressed(cleanupFailure);
      }
      throw e;
    }
  }

  /**
   * Starts the single copy of one backup, or records a copy that is already running. Throws {@link
   * ManualRecoveryException} when copying again could duplicate events.
   */
  private static void startCopy(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String backupName,
      String indexName,
      String writeIndex,
      boolean dataStream,
      LegacyMigrationLease lease)
      throws IOException {
    JsonNode meta = backfillMeta(opContext, esComponents, backupName);
    String taskId = meta.path(BACKFILL_TASK_META).asText();
    String recordedWriteIndex = meta.path(BACKFILL_WRITE_INDEX_META).asText();
    if (!taskId.isEmpty() && getTask(opContext, esComponents, taskId) != null) {
      return;
    }
    // No copy recorded, or one whose task id was lost or that the cluster forgot (for example
    // after a restart): a copy may still be running under a task this run does not know.
    String runningTask = runningCopyTask(opContext, esComponents, backupName, indexName);
    if (runningTask != null) {
      log.info(
          "Copying {} back into {} already runs as task {}; recording it instead of starting"
              + " another copy",
          backupName,
          indexName,
          runningTask);
      recordCopy(
          opContext,
          esComponents,
          backupName,
          runningTask,
          recordedWriteIndex.isEmpty() ? writeIndex : recordedWriteIndex);
      return;
    }
    if (!recordedWriteIndex.isEmpty() && !recordedWriteIndex.equals(writeIndex)) {
      throw new ManualRecoveryException(
          String.format(
              "An earlier copy of %s into %s did not finish and %s has rolled over since, so"
                  + " copying again could duplicate events. %s is kept; copy the remaining events"
                  + " yourself, then delete it",
              backupName, recordedWriteIndex, indexName, backupName));
    }
    if (!recordedWriteIndex.isEmpty()) {
      log.warn(
          "An earlier copy of {} did not finish; copying it again into {}, which skips events"
              + " already there",
          backupName,
          writeIndex);
    }
    lease.checkHeld();
    recordCopy(opContext, esComponents, backupName, "", writeIndex);
    String reindex =
        String.format(
            "{\"conflicts\":\"proceed\",\"source\":{\"index\":\"%s\"},"
                + "\"dest\":{\"index\":\"%s\",\"op_type\":\"create\"},"
                + "\"script\":{\"lang\":\"painless\",\"source\":\"%s\","
                + "\"params\":{\"dataStream\":%s}}}",
            backupName, indexName, BACKFILL_SCRIPT, dataStream);
    String newTask =
        readJson(
                opContext,
                IndexUtils.performPostRequest(
                    opContext, esComponents, "/_reindex?wait_for_completion=false", reindex))
            .path("task")
            .asText();
    if (newTask.isEmpty()) {
      throw new IOException("Reindex from " + backupName + " did not return a task id");
    }
    recordCopy(opContext, esComponents, backupName, newTask, writeIndex);
  }

  /** Checks the recorded copy of one backup and deletes the backup once the copy is complete. */
  private static void finishCopy(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String backupName,
      String indexName)
      throws IOException, InterruptedException {
    String taskId =
        backfillMeta(opContext, esComponents, backupName).path(BACKFILL_TASK_META).asText();
    if (taskId.isEmpty()) {
      return;
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
      throw new ManualRecoveryException(
          String.format(
              "Copying usage events from %s back into %s (task %s) is incomplete: %d of %d copied,"
                  + " first error: %s. %s is kept and not copied again, because a second copy can"
                  + " duplicate events once the index has rolled over; copy the remaining events"
                  + " yourself, then delete it",
              backupName,
              indexName,
              taskId,
              created + present,
              expected,
              task.has("error") ? task.path("error") : failures.path(0),
              backupName));
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

  /** The id of a running reindex from the backup into the usage event index, if there is one. */
  @Nullable
  private static String runningCopyTask(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String backupName,
      String indexName)
      throws IOException {
    Request request = new Request("GET", "/_tasks");
    request.addParameter("actions", "*reindex");
    request.addParameter("detailed", "true");
    request.addParameter("ignore", "403");
    RawResponse response =
        esComponents.getSearchClient().performLowLevelRequest(opContext, request);
    if (response.getStatusLine().getStatusCode() == 403) {
      throw new TaskApiForbiddenException(
          "System update may not list tasks (403), so it cannot tell whether "
              + backupName
              + " is already being copied; copy it yourself as the upgrade notes describe, then"
              + " delete it");
    }
    JsonNode tasks = readJson(opContext, response);
    if (!tasks.path("node_failures").isEmpty() || !tasks.path("task_failures").isEmpty()) {
      // A node that did not answer may be running the copy, so do not conclude there is none.
      throw new IOException("Could not list the reindex tasks of every node: " + tasks);
    }
    // Both engines describe it as "reindex from [source] updated with Script{...} to [dest]".
    String from = "reindex from [" + backupName + "]";
    String to = " to [" + indexName + "]";
    for (JsonNode node : tasks.path("nodes")) {
      Iterator<Map.Entry<String, JsonNode>> it = node.path("tasks").fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> task = it.next();
        String description = task.getValue().path("description").asText();
        if (description.startsWith(from) && description.contains(to)) {
          return task.getKey();
        }
      }
    }
    return null;
  }

  /**
   * The index new usage events go to when the usage event index is the managed layout (a data
   * stream, or a rollover alias with exactly one write index), otherwise null.
   */
  @Nullable
  private static String managedWriteIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      boolean useOpenSearch)
      throws IOException {
    Request request = new Request("GET", "/_resolve/index/" + indexName);
    request.addParameter("ignore", "404");
    RawResponse response =
        esComponents.getSearchClient().performLowLevelRequest(opContext, request);
    if (response.getStatusLine().getStatusCode() == 404) {
      return null;
    }
    JsonNode resolved = readJson(opContext, response);
    List<String> managed = new ArrayList<>();
    resolved
        .path(useOpenSearch ? "aliases" : "data_streams")
        .forEach(entry -> managed.add(entry.path("name").asText()));
    if (!managed.contains(indexName)) {
      return null;
    }
    if (!useOpenSearch) {
      String writeIndex = writeIndex(opContext, esComponents, indexName, true);
      return writeIndex.isEmpty() ? null : writeIndex;
    }
    List<String> writeIndices = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> it =
        readJson(
                opContext,
                IndexUtils.performGetRequest(opContext, esComponents, "/_alias/" + indexName))
            .fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> index = it.next();
      if (index.getValue().path("aliases").path(indexName).path("is_write_index").asBoolean()) {
        writeIndices.add(index.getKey());
      }
    }
    return writeIndices.size() == 1 ? writeIndices.get(0) : null;
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
    request.addParameter("ignore", "403,404");
    RawResponse response =
        esComponents.getSearchClient().performLowLevelRequest(opContext, request);
    int status = response.getStatusLine().getStatusCode();
    if (status == 403) {
      throw new TaskApiForbiddenException(
          "System update may not read copy task "
              + taskId
              + " (403), so it cannot tell when the copy back finishes; copy the remaining events"
              + " yourself as the upgrade notes describe, then delete the backup");
    }
    return status == 404 ? null : readJson(opContext, response);
  }

  /**
   * A backup that no retry can copy back, which a person has to finish as the upgrade notes say.
   */
  private static class ManualRecoveryException extends IOException {
    ManualRecoveryException(String message) {
      super(message);
    }
  }

  /** The cluster forbids the tasks API to these credentials, so no run can finish the copy. */
  private static class TaskApiForbiddenException extends ManualRecoveryException {
    TaskApiForbiddenException(String message) {
      super(message);
    }
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

  /**
   * Lifts the write block on a legacy usage event index, retrying, since every usage event is
   * rejected while it stays. Returns false, after logging how to lift it by hand, when every
   * attempt failed.
   */
  private static boolean liftWriteBlock(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName) {
    if (IndexUtils.retryWithBackoff(
        5,
        2000,
        () -> {
          setWriteBlock(opContext, esComponents, indexName, false);
          return true;
        })) {
      return true;
    }
    log.error(
        "Could not lift the write block on {}, which rejects every usage event until it is lifted:"
            + " PUT /{}/_settings {\"index.blocks.write\":false}",
        indexName,
        indexName);
    recordOutcome(opContext, OUTCOME_MANUAL_RECOVERY);
    return false;
  }

  private static void deleteIndex(
      OperationContext opContext,
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName)
      throws IOException {
    log.info("DELETE => /{}", indexName);
    Request request = new Request("DELETE", "/" + indexName);
    // Another run may have deleted it first, which is the outcome this call wants.
    request.addParameter("ignore", "404");
    esComponents.getSearchClient().performLowLevelRequest(opContext, request);
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
