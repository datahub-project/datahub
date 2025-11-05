package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.OpenSearchStatusException;
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
                      IndexUtils.performPutRequest(esComponents, endpoint, policyJson);

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
                  RawResponse getResponse = IndexUtils.performGetRequest(esComponents, endpoint);
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
      return handleExistingPolicy(esComponents, policyName, prefix, operationContext);
    }

    if (getStatusCode == 404) {
      return createNewPolicy(esComponents, endpoint, policyJson, policyName);
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
      return handleExistingPolicy(esComponents, policyName, prefix, operationContext);
    }

    if (statusCode == 404) {
      log.info("ISM policy {} doesn't exist (from exception), creating it", policyName);
      return createNewPolicy(esComponents, endpoint, policyJson, policyName);
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
   * @param esComponents the Elasticsearch components factory
   * @param policyName the name of the ISM policy
   * @param prefix the prefix for index patterns
   * @param operationContext the operation context for JSON parsing
   * @return true if successful, false otherwise
   */
  private static boolean handleExistingPolicy(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix,
      OperationContext operationContext) {

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
   * @param esComponents the Elasticsearch components factory
   * @param endpoint the API endpoint
   * @param policyJson the policy JSON to create
   * @param policyName the name of the ISM policy
   * @return true if successful, false otherwise
   */
  private static boolean createNewPolicy(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String endpoint,
      String policyJson,
      String policyName) {

    log.info("ISM policy {} doesn't exist, creating it", policyName);

    try {
      RawResponse createResponse = IndexUtils.performPutRequest(esComponents, endpoint, policyJson);
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

      RawResponse response = IndexUtils.performPutRequest(esComponents, endpoint, templateJson);

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
      RawResponse response = IndexUtils.performPutRequest(esComponents, endpoint, templateJson);

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
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String dataStreamName)
      throws IOException {
    try {
      GetIndexRequest getRequest = new GetIndexRequest(dataStreamName);

      boolean exists =
          esComponents.getSearchClient().indexExists(getRequest, RequestOptions.DEFAULT);

      if (!exists) {
        // Create data stream by creating an index with the data stream name
        CreateIndexRequest createRequest = new CreateIndexRequest(dataStreamName);

        CreateIndexResponse response =
            esComponents.getSearchClient().createIndex(createRequest, RequestOptions.DEFAULT);

        if (response.isAcknowledged()) {
          log.info("Successfully created data stream: {}", dataStreamName);
        } else {
          log.warn("Data stream creation not acknowledged: {}", dataStreamName);
        }
      } else {
        log.info("Data stream {} already exists", dataStreamName);
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
   * @param esComponents the Elasticsearch/OpenSearch components factory
   * @param indexName the name of the index to create
   * @param aliasName the name of the alias to assign with is_write_index=true
   * @throws IOException if there's an error creating the index
   */
  private static void createIndexWithWriteAlias(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      String aliasName)
      throws IOException {
    String indexJson = String.format("{\"aliases\":{\"%s\":{\"is_write_index\":true}}}", aliasName);

    CreateIndexRequest request = new CreateIndexRequest(indexName);
    request.source(indexJson, XContentType.JSON);

    CreateIndexResponse response =
        esComponents.getSearchClient().createIndex(request, RequestOptions.DEFAULT);

    if (response.isAcknowledged()) {
      log.info("Successfully created index: {} with write alias: {}", indexName, aliasName);
    } else {
      log.warn("Index creation not acknowledged: {}", indexName);
    }
  }

  /**
   * Creates an initial index for AWS OpenSearch usage events.
   *
   * <p>This method creates the first index in a time-series setup for usage events in AWS
   * OpenSearch environments. Unlike Elasticsearch data streams, OpenSearch uses numbered indices
   * (e.g., "datahub_usage_event-000001") with aliases for rollover management.
   *
   * <p>The method creates an empty index configuration and applies the specified prefix. The alias
   * configuration is handled programmatically after index creation.
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
   * @param prefix the index prefix to apply to alias configurations (e.g., "prod_")
   * @throws IOException if there's an error reading the index configuration or making the request
   * @throws OpenSearchStatusException if the creation fails with a non-"already exists" error
   */
  public static void createOpenSearchIndex(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String indexName,
      String prefix)
      throws IOException {
    try {
      GetIndexRequest getRequest = new GetIndexRequest(indexName);

      boolean exists =
          esComponents.getSearchClient().indexExists(getRequest, RequestOptions.DEFAULT);

      if (!exists) {
        // Create index with alias in a single request (common syntax for both Elasticsearch and
        // OpenSearch)
        String aliasName = prefix + "datahub_usage_event";
        log.info("Creating new OpenSearch index: {} with alias: {}", indexName, aliasName);
        createIndexWithWriteAlias(esComponents, indexName, aliasName);
      } else {
        log.info("OpenSearch index {} already exists - skipping creation", indexName);
      }
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
      RawResponse getResponse = IndexUtils.performGetRequest(esComponents, endpoint);

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
          IndexUtils.performPutRequestWithParams(esComponents, endpoint, queryParams, policyJson);

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
