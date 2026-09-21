package com.linkedin.datahub.upgrade.cleanup;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexDeletionUtils;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
  @Nullable private final SearchClusterRegistry searchClusterRegistry;

  public DeleteElasticsearchIndicesStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents) {
    this(esComponents, null);
  }

  public DeleteElasticsearchIndicesStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      @Nullable SearchClusterRegistry searchClusterRegistry) {
    this.esComponents = esComponents;
    this.searchClusterRegistry = searchClusterRegistry;
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
      OperationContext opContext = context.opContext();
      try {
        IndexConvention convention = esComponents.getIndexConvention();
        SearchClusterAccess access = opContext.getSearchContext().requireSearchClusterAccess();

        deleteOwnedIndices(opContext, access, convention);
        deleteSecurityOnClusters(opContext, access);

        log.info("Elasticsearch cleanup completed successfully");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("DeleteElasticsearchIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void deleteOwnedIndices(
      OperationContext opContext, SearchClusterAccess access, IndexConvention convention) {
    for (String pattern : convention.getAllEntityIndicesPatterns(opContext)) {
      SearchComponent component =
          SearchClusterAccess.tryComponentForEntityIndex(convention, pattern);
      if (component == null) {
        throw new IllegalArgumentException(
            "Unrecognized entity cleanup pattern cannot be routed safely: " + pattern);
      }
      deleteIndicesByPattern(opContext, access.clientFor(component), pattern);
    }

    deleteIndicesByPattern(
        opContext,
        access.clientFor(SearchComponent.TIMESERIES),
        convention.getAllTimeseriesAspectIndicesPattern(opContext));

    deleteIndicesByPattern(
        opContext,
        access.clientFor(SearchComponent.SEMANTIC),
        convention.getAllSemanticEntityIndicesPattern(opContext));

    safeDeleteIndex(
        opContext,
        access.clientFor(SearchComponent.GRAPH),
        convention.getIndexName(
            opContext, SearchComponent.GRAPH, ElasticSearchGraphService.INDEX_NAME));
    safeDeleteIndex(
        opContext,
        access.clientFor(SearchComponent.SYSTEM_METADATA),
        convention.getIndexName(
            opContext,
            SearchComponent.SYSTEM_METADATA,
            ElasticSearchSystemMetadataService.INDEX_NAME));

    SearchClientShim<?> usageClient = access.clientFor(SearchComponent.USAGE);
    deleteUsageEventResources(
        opContext, usageClient, convention, usageClient.getEngineType().isOpenSearch());
  }

  private void deleteSecurityOnClusters(
      @Nonnull OperationContext opContext, @Nonnull SearchClusterAccess access) {
    if (searchClusterRegistry != null) {
      for (SearchClusterRegistry.ClusterConnection connection :
          searchClusterRegistry.uniqueConnections()) {
        SearchClientShim<?> clusterClient = connection.getClient();
        String roleName = connection.getConfig().getIndex().getFinalPrefix() + "access";
        deleteSecurityResources(
            opContext, clusterClient, roleName, clusterClient.getEngineType().isOpenSearch());
      }
      return;
    }
    String roleName = esComponents.getConfig().getIndex().getFinalPrefix() + "access";
    for (SearchClientShim<?> clusterClient : uniqueClients(access)) {
      deleteSecurityResources(
          opContext, clusterClient, roleName, clusterClient.getEngineType().isOpenSearch());
    }
  }

  @Nonnull
  private static List<SearchClientShim<?>> uniqueClients(@Nonnull SearchClusterAccess access) {
    IdentityHashMap<SearchClientShim<?>, Boolean> seen = new IdentityHashMap<>();
    List<SearchClientShim<?>> unique = new ArrayList<>();
    for (SearchComponent component : SearchComponent.values()) {
      SearchClientShim<?> client = access.clientFor(component);
      if (seen.putIfAbsent(client, Boolean.TRUE) == null) {
        unique.add(client);
      }
    }
    return unique;
  }

  /**
   * Resolves a glob pattern to concrete index names and deletes each by exact name. This avoids
   * issuing a wildcard DELETE, which would be unsafe on shared clusters.
   */
  private void deleteIndicesByPattern(
      OperationContext opContext, SearchClientShim<?> client, String pattern) {
    try {
      GetIndexResponse response =
          client.getIndex(opContext, new GetIndexRequest(pattern), RequestOptions.DEFAULT);
      for (String indexName : response.getIndices()) {
        safeDeleteIndex(opContext, client, indexName);
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

  private void safeDeleteIndex(
      OperationContext opContext, SearchClientShim<?> client, String indexName) {
    try {
      String tracked = IndexDeletionUtils.deleteIndex(client, opContext, indexName);
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
      OperationContext opContext,
      SearchClientShim<?> client,
      IndexConvention convention,
      boolean isOpenSearch) {
    String dataStreamName =
        convention.getIndexName(opContext, SearchComponent.USAGE, DATAHUB_USAGE_EVENT_INDEX);
    String templateName =
        convention.getIndexName(
            opContext, SearchComponent.USAGE, "datahub_usage_event_index_template");
    String policyName =
        convention.getIndexName(opContext, SearchComponent.USAGE, "datahub_usage_event_policy");

    if (isOpenSearch) {
      safeDeleteLowLevel(opContext, client, "/" + dataStreamName, "usage event alias");
      safeDeleteLowLevel(
          opContext, client, "/_index_template/" + templateName, "usage event index template");
      safeDeleteLowLevel(
          opContext, client, "/_plugins/_ism/policies/" + policyName, "ISM policy (plugins)");
      safeDeleteLowLevel(
          opContext, client, "/_opendistro/_ism/policies/" + policyName, "ISM policy (opendistro)");
    } else {
      safeDeleteLowLevel(
          opContext, client, "/_data_stream/" + dataStreamName, "usage event data stream");
      safeDeleteLowLevel(
          opContext, client, "/_index_template/" + templateName, "usage event index template");
      safeDeleteLowLevel(opContext, client, "/_ilm/policy/" + policyName, "ILM policy");
    }
  }

  /** Deletes the security role and user created by the ES setup job. */
  private void deleteSecurityResources(
      OperationContext opContext,
      SearchClientShim<?> client,
      String roleName,
      boolean isOpenSearch) {
    String username = EnvironmentUtils.getString("CREATE_USER_ES_USERNAME");

    if (isOpenSearch) {
      safeDeleteLowLevel(
          opContext,
          client,
          "/_plugins/_security/api/rolesmapping/" + roleName,
          "OpenSearch role mapping");
      safeDeleteLowLevel(
          opContext,
          client,
          "/_opendistro/_security/api/rolesmapping/" + roleName,
          "OpenSearch role mapping (opendistro)");
      if (username != null && !username.isEmpty()) {
        safeDeleteLowLevel(
            opContext,
            client,
            "/_opendistro/_security/api/internalusers/" + username,
            "OpenSearch internal user");
      }
      safeDeleteLowLevel(
          opContext, client, "/_opendistro/_security/api/roles/" + roleName, "OpenSearch role");
    } else {
      if (username != null && !username.isEmpty()) {
        safeDeleteLowLevel(opContext, client, "/_security/user/" + username, "Elasticsearch user");
      }
      safeDeleteLowLevel(opContext, client, "/_security/role/" + roleName, "Elasticsearch role");
    }
  }

  /** Performs a low-level DELETE request, logging but not throwing on 404. */
  private void safeDeleteLowLevel(
      OperationContext opContext, SearchClientShim<?> client, String endpoint, String description) {
    try {
      performDelete(opContext, client, endpoint);
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

  private void performDelete(
      OperationContext opContext, SearchClientShim<?> client, String endpoint) throws Exception {
    log.info("DELETE => {}", endpoint);
    Request request = new Request("DELETE", endpoint);
    RawResponse response = client.performLowLevelRequest(opContext, request);
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode >= 400) {
      throw new RuntimeException("DELETE " + endpoint + " returned HTTP " + statusCode);
    }
  }
}
