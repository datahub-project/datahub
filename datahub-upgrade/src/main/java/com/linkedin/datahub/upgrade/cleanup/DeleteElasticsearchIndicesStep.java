package com.linkedin.datahub.upgrade.cleanup;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.upgrade.DataHubUpgradeState;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

/**
 * Deletes all Elasticsearch/OpenSearch resources created by DataHub: indices matching the
 * configured prefix, usage event data streams / aliases, index templates, ILM/ISM policies, and
 * security roles/users.
 */
@Slf4j
public class DeleteElasticsearchIndicesStep implements UpgradeStep {

  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  private final ConfigurationProvider configurationProvider;

  public DeleteElasticsearchIndicesStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      ConfigurationProvider configurationProvider) {
    this.esComponents = esComponents;
    this.configurationProvider = configurationProvider;
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
        String indexPrefix = configurationProvider.getElasticSearch().getIndex().getFinalPrefix();
        boolean isOpenSearch = esComponents.getSearchClient().getEngineType().isOpenSearch();

        deleteUsageEventResources(indexPrefix, isOpenSearch);
        deleteAllIndices(indexPrefix);
        deleteSecurityResources(indexPrefix, isOpenSearch);

        log.info("Elasticsearch cleanup completed successfully");
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("DeleteElasticsearchIndicesStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  /** Delete all indices matching {prefix}* */
  private void deleteAllIndices(String prefix) {
    String pattern = prefix + "*";
    try {
      log.info("Deleting all indices matching pattern: {}", pattern);
      performDelete("/" + pattern + "?ignore_unavailable=true");
      log.info("Successfully deleted indices matching {}", pattern);
    } catch (Exception e) {
      log.warn("Failed to delete indices matching {}: {}", pattern, e.getMessage());
    }
  }

  /** Delete usage event data streams, index templates, and ILM/ISM policies. */
  private void deleteUsageEventResources(String prefix, boolean isOpenSearch) {
    String prefixedDataStream = prefix + "datahub_usage_event";
    String prefixedTemplate = prefix + "datahub_usage_event_index_template";
    String prefixedPolicy = prefix + "datahub_usage_event_policy";

    if (isOpenSearch) {
      // OpenSearch: delete alias, then ISM policy
      safeDelete("/" + prefixedDataStream, "usage event alias");
      safeDelete("/_index_template/" + prefixedTemplate, "usage event index template");
      // Try both ISM API paths
      safeDelete("/_plugins/_ism/policies/" + prefixedPolicy, "ISM policy (plugins)");
      safeDelete("/_opendistro/_ism/policies/" + prefixedPolicy, "ISM policy (opendistro)");
    } else {
      // Elasticsearch: delete data stream, index template, ILM policy
      safeDelete("/_data_stream/" + prefixedDataStream, "usage event data stream");
      safeDelete("/_index_template/" + prefixedTemplate, "usage event index template");
      safeDelete("/_ilm/policy/" + prefixedPolicy, "ILM policy");
    }
  }

  /** Delete the security role and user created by CreateUserStep. */
  private void deleteSecurityResources(String prefix, boolean isOpenSearch) {
    String roleName = prefix + "access";
    String username = EnvironmentUtils.getString("CREATE_USER_ES_USERNAME");

    if (isOpenSearch) {
      // Role mapping
      safeDelete("/_plugins/_security/api/rolesmapping/" + roleName, "OpenSearch role mapping");
      safeDelete(
          "/_opendistro/_security/api/rolesmapping/" + roleName,
          "OpenSearch role mapping (opendistro)");
      // User
      if (username != null && !username.isEmpty()) {
        safeDelete(
            "/_opendistro/_security/api/internalusers/" + username, "OpenSearch internal user");
      }
      // Role
      safeDelete("/_opendistro/_security/api/roles/" + roleName, "OpenSearch role");
    } else {
      // Elasticsearch Cloud
      if (username != null && !username.isEmpty()) {
        safeDelete("/_security/user/" + username, "Elasticsearch user");
      }
      safeDelete("/_security/role/" + roleName, "Elasticsearch role");
    }
  }

  /** Perform a DELETE request, logging but not throwing on 404. */
  private void safeDelete(String endpoint, String description) {
    try {
      performDelete(endpoint);
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

  private void performDelete(String endpoint) throws Exception {
    log.info("DELETE => {}", endpoint);
    Request request = new Request("DELETE", endpoint);
    RawResponse response = esComponents.getSearchClient().performLowLevelRequest(request);
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode >= 400) {
      throw new RuntimeException("DELETE " + endpoint + " returned HTTP " + statusCode);
    }
  }
}
