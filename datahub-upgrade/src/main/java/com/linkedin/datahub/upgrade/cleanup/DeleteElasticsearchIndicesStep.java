package com.linkedin.datahub.upgrade.cleanup;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexDeletionUtils;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.client.indices.GetIndexRequest;

/**
 * Deletes all Elasticsearch/OpenSearch resources created by DataHub:
 *
 * <ul>
 *   <li>Entity search indices (V2/V3) resolved via {@link IndexConvention} patterns
 *   <li>Timeseries aspect indices resolved via {@link IndexConvention} pattern
 *   <li>System metadata index
 *   <li>Graph index
 *   <li>Usage event data stream / alias, index templates, ILM/ISM policies
 *   <li>Security roles and users created by the ES setup job
 * </ul>
 *
 * <p>Indices are enumerated by service-specific patterns from {@link IndexConvention} rather than a
 * blunt {@code prefix*} wildcard, which would be dangerous on shared clusters with no prefix
 * configured.
 */
@Slf4j
public class DeleteElasticsearchIndicesStep implements UpgradeStep {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;

  public DeleteElasticsearchIndicesStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents) {
    this.esComponents = esComponents;
  }

  @Override
  public String id() {
    return "DeleteElasticsearchIndicesStep";
  }

  @Override
  public int retryCount() {
    return 2;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        SearchClientShim<?> client = esComponents.getSearchClient();
        IndexConvention convention = esComponents.getIndexConvention();
        boolean isOpenSearch = client.getEngineType().isOpenSearch();

        // Entity search indices (V2/V3) — enumerate concrete names before deleting
        for (String pattern : convention.getAllEntityIndicesPatterns()) {
          deleteIndicesByPattern(client, pattern);
        }

        // Timeseries aspect indices
        deleteIndicesByPattern(client, convention.getAllTimeseriesAspectIndicesPattern());

        // Well-known named indices
        safeDeleteIndex(client, convention.getIndexName(ElasticSearchGraphService.INDEX_NAME));
        safeDeleteIndex(
            client, convention.getIndexName(ElasticSearchSystemMetadataService.INDEX_NAME));

        // Usage event resources (data stream/alias, template, ILM/ISM policy)
        deleteUsageEventResources(client, convention, isOpenSearch);

        // Security roles and users created by the ES setup job
        deleteSecurityResources(client, convention, isOpenSearch);

        log.info("Elasticsearch cleanup completed successfully");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("DeleteElasticsearchIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /**
   * Resolves a glob pattern to concrete index names and deletes each by exact name. This avoids
   * issuing a wildcard DELETE, which would be unsafe on shared clusters.
   */
  private void deleteIndicesByPattern(SearchClientShim<?> client, String pattern) {
    try {
      GetIndexResponse response =
          client.getIndex(new GetIndexRequest(pattern), RequestOptions.DEFAULT);
      for (String indexName : response.getIndices()) {
        safeDeleteIndex(client, indexName);
      }
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() == 404) {
        log.info("No indices matching pattern {} (already absent)", pattern);
      } else {
        log.warn(
            "Failed to enumerate indices for pattern {} (HTTP {}): {}",
            pattern,
            e.getResponse().getStatusLine().getStatusCode(),
            e.getMessage());
      }
    } catch (Exception e) {
      log.warn("Failed to enumerate indices for pattern {}: {}", pattern, e.getMessage());
    }
  }

  private void safeDeleteIndex(SearchClientShim<?> client, String indexName) {
    try {
      String tracked = IndexDeletionUtils.deleteIndex(client, indexName);
      if (tracked != null) {
        log.info("Deleted index/alias {}", indexName);
      } else {
        log.info("Index {} not found (already absent)", indexName);
      }
    } catch (Exception e) {
      log.warn("Failed to delete index {}: {}", indexName, e.getMessage());
    }
  }

  /** Deletes usage event data streams, index templates, and ILM/ISM policies. */
  private void deleteUsageEventResources(
      SearchClientShim<?> client, IndexConvention convention, boolean isOpenSearch) {
    String dataStreamName = convention.getIndexName(DATAHUB_USAGE_EVENT_INDEX);
    String templateName = convention.getIndexName("datahub_usage_event_index_template");
    String policyName = convention.getIndexName("datahub_usage_event_policy");

    if (isOpenSearch) {
      safeDeleteLowLevel(client, "/" + dataStreamName, "usage event alias");
      safeDeleteLowLevel(client, "/_index_template/" + templateName, "usage event index template");
      safeDeleteLowLevel(client, "/_plugins/_ism/policies/" + policyName, "ISM policy (plugins)");
      safeDeleteLowLevel(
          client, "/_opendistro/_ism/policies/" + policyName, "ISM policy (opendistro)");
    } else {
      safeDeleteLowLevel(client, "/_data_stream/" + dataStreamName, "usage event data stream");
      safeDeleteLowLevel(client, "/_index_template/" + templateName, "usage event index template");
      safeDeleteLowLevel(client, "/_ilm/policy/" + policyName, "ILM policy");
    }
  }

  /** Deletes the security role and user created by the ES setup job. */
  private void deleteSecurityResources(
      SearchClientShim<?> client, IndexConvention convention, boolean isOpenSearch) {
    String roleName = convention.getIndexName("access");
    String username = EnvironmentUtils.getString("CREATE_USER_ES_USERNAME");

    if (isOpenSearch) {
      safeDeleteLowLevel(
          client, "/_plugins/_security/api/rolesmapping/" + roleName, "OpenSearch role mapping");
      safeDeleteLowLevel(
          client,
          "/_opendistro/_security/api/rolesmapping/" + roleName,
          "OpenSearch role mapping (opendistro)");
      if (username != null && !username.isEmpty()) {
        safeDeleteLowLevel(
            client,
            "/_opendistro/_security/api/internalusers/" + username,
            "OpenSearch internal user");
      }
      safeDeleteLowLevel(client, "/_opendistro/_security/api/roles/" + roleName, "OpenSearch role");
    } else {
      if (username != null && !username.isEmpty()) {
        safeDeleteLowLevel(client, "/_security/user/" + username, "Elasticsearch user");
      }
      safeDeleteLowLevel(client, "/_security/role/" + roleName, "Elasticsearch role");
    }
  }

  /** Performs a low-level DELETE request, logging but not throwing on 404. */
  private void safeDeleteLowLevel(SearchClientShim<?> client, String endpoint, String description) {
    try {
      performDelete(client, endpoint);
      log.info("Deleted {}: {}", description, endpoint);
    } catch (ResponseException e) {
      int status = e.getResponse().getStatusLine().getStatusCode();
      if (status == 404) {
        log.info("{} not found (already absent): {}", description, endpoint);
      } else {
        log.warn("Failed to delete {} (HTTP {}): {}", description, status, e.getMessage());
      }
    } catch (Exception e) {
      log.warn("Failed to delete {}: {}", description, e.getMessage());
    }
  }

  private void performDelete(SearchClientShim<?> client, String endpoint) throws Exception {
    log.info("DELETE => {}", endpoint);
    Request request = new Request("DELETE", endpoint);
    RawResponse response = client.performLowLevelRequest(request);
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode >= 400) {
      throw new RuntimeException("DELETE " + endpoint + " returned HTTP " + statusCode);
    }
  }
}
