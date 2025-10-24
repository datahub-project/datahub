package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
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
   * /index/usage-event/policy.json} and applied to the specified policy name.
   *
   * <p>The method uses the low-level REST client to make a PUT request to the {@code
   * _ilm/policy/{policyName}} endpoint, which provides upsert behavior (creates if not exists,
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
      String policyJson = loadResourceAsString("/index/usage-event/policy.json");

      // Use the low-level client to make the PUT request to _ilm/policy endpoint
      String endpoint = "_ilm/policy/" + policyName;

      Request request = new Request("PUT", endpoint);
      request.setJsonEntity(policyJson);

      RawResponse response = esComponents.getSearchClient().performLowLevelRequest(request);

      if (response.getStatusLine().getStatusCode() == 200
          || response.getStatusLine().getStatusCode() == 201) {
        log.info("Successfully created ILM policy: {}", policyName);
      } else {
        log.error(
            "ILM policy creation returned status: {}", response.getStatusLine().getStatusCode());
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
   * Creates an Index State Management (ISM) policy for AWS OpenSearch usage events.
   *
   * <p>This method creates an ISM policy that manages the lifecycle of usage event indices in AWS
   * OpenSearch environments. The policy is loaded from the resource file {@code
   * /index/usage-event/aws_es_ism_policy.json} and applied to the specified policy name.
   *
   * <p>The method uses the low-level REST client to make a PUT request to the {@code
   * _plugins/_ism/policies/{policyName}} endpoint, which is specific to AWS OpenSearch. The policy
   * template includes placeholder replacement for the index prefix.
   *
   * <p>ISM policies in OpenSearch provide similar functionality to ILM policies in Elasticsearch,
   * including rollover conditions, state transitions, and retention policies.
   *
   * @param esComponents the Elasticsearch components factory providing search client access
   * @param policyName the name of the ISM policy to create (e.g., "datahub_usage_event_policy")
   * @param prefix the index prefix to apply to policy configurations (e.g., "prod_")
   * @throws IOException if there's an error reading the policy template or making the request
   * @throws ResponseException if the request fails with a non-409 status code
   */
  public static void createIsmPolicy(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      String policyName,
      String prefix)
      throws IOException {
    try {
      String policyJson = loadResourceAsString("/index/usage-event/aws_es_ism_policy.json");
      // Replace placeholders
      policyJson = policyJson.replace("PREFIX", prefix);

      // For AWS OpenSearch, we need to use the low-level REST client for ISM policies
      // since the high-level client doesn't support the _plugins/_ism/policies endpoint
      String endpoint = "_plugins/_ism/policies/" + policyName;

      // Use the low-level client to make the PUT request
      Request request = new Request("PUT", endpoint);
      request.setJsonEntity(policyJson);

      RawResponse response = esComponents.getSearchClient().performLowLevelRequest(request);

      if (response.getStatusLine().getStatusCode() == 200
          || response.getStatusLine().getStatusCode() == 201) {
        log.info("Successfully created ISM policy: {}", policyName);
      } else {
        log.warn(
            "ISM policy creation returned status: {}", response.getStatusLine().getStatusCode());
      }
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("ISM policy {} already exists", policyName);
      } else {
        throw e;
      }
    }
  }

  /**
   * Creates an index template for Elasticsearch usage events.
   *
   * <p>This method creates an index template that defines the structure and settings for usage
   * event indices in standard Elasticsearch environments. The template is loaded from the resource
   * file {@code /index/usage-event/index_template.json} and configured with the specified
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
   * _index_template/{templateName}} endpoint, which provides upsert behavior.
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
      String templateJson = loadResourceAsString("/index/usage-event/index_template.json");
      // Replace placeholders
      templateJson = templateJson.replace("PREFIX", prefix);
      templateJson = templateJson.replace("DUE_SHARDS", String.valueOf(numShards));
      templateJson = templateJson.replace("DUE_REPLICAS", String.valueOf(numReplicas));

      // Use the low-level client for index templates
      String endpoint = "_index_template/" + templateName;

      Request request = new Request("PUT", endpoint);
      request.setJsonEntity(templateJson);

      RawResponse response = esComponents.getSearchClient().performLowLevelRequest(request);

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
   * {@code /index/usage-event/aws_es_index_template.json} and configured with the specified
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
   * _template/{templateName}} endpoint, which is the legacy template API used by AWS OpenSearch.
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
      String templateJson = loadResourceAsString("/index/usage-event/aws_es_index_template.json");
      // Replace placeholders
      templateJson = templateJson.replace("PREFIX", prefix);
      templateJson = templateJson.replace("DUE_SHARDS", String.valueOf(numShards));
      templateJson = templateJson.replace("DUE_REPLICAS", String.valueOf(numReplicas));

      // For AWS OpenSearch, we need to use the _template endpoint instead of _index_template
      String endpoint = "_template/" + templateName;

      // Use the low-level client to make the PUT request
      Request request = new Request("PUT", endpoint);
      request.setJsonEntity(templateJson);

      RawResponse response = esComponents.getSearchClient().performLowLevelRequest(request);

      if (response.getStatusLine().getStatusCode() == 200
          || response.getStatusLine().getStatusCode() == 201) {
        log.info("Successfully created OpenSearch index template: {}", templateName);
      } else {
        log.warn(
            "OpenSearch index template creation returned status: {}",
            response.getStatusLine().getStatusCode());
      }
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 409) {
        log.info("OpenSearch index template {} already exists", templateName);
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
   * Creates an initial index for AWS OpenSearch usage events.
   *
   * <p>This method creates the first index in a time-series setup for usage events in AWS
   * OpenSearch environments. Unlike Elasticsearch data streams, OpenSearch uses numbered indices
   * (e.g., "datahub_usage_event-000001") with aliases for rollover management.
   *
   * <p>The method loads the index configuration from the resource file {@code
   * /index/usage-event/aws_es_index.json} and applies the specified prefix. The configuration
   * includes alias settings that enable ISM policy rollover.
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
        String indexJson = loadResourceAsString("/index/usage-event/aws_es_index.json");
        // Replace PREFIX placeholder with actual prefix
        indexJson = indexJson.replace("PREFIX", prefix);

        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.source(indexJson, XContentType.JSON);

        CreateIndexResponse response =
            esComponents.getSearchClient().createIndex(request, RequestOptions.DEFAULT);

        if (response.isAcknowledged()) {
          log.info("Successfully created OpenSearch index: {}", indexName);
        } else {
          log.warn("OpenSearch index creation not acknowledged: {}", indexName);
        }
      } else {
        log.info("OpenSearch index {} already exists", indexName);
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
   * Loads a resource file as a UTF-8 encoded string.
   *
   * <p>This utility method reads a resource file from the classpath and returns its contents as a
   * string. It's used internally by other methods to load JSON templates and configuration files.
   *
   * <p>The method uses the class loader to locate the resource and reads it using a buffered input
   * stream for efficient memory usage.
   *
   * @param resourcePath the path to the resource file (e.g., "/index/usage-event/policy.json")
   * @return the contents of the resource file as a UTF-8 encoded string
   * @throws IOException if the resource cannot be found or read
   */
  private static String loadResourceAsString(String resourcePath) throws IOException {
    try (InputStream inputStream = UsageEventIndexUtils.class.getResourceAsStream(resourcePath)) {
      if (inputStream == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
