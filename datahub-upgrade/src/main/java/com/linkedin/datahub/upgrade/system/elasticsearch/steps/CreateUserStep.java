package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexRoleUtils;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CreateUserStep implements UpgradeStep {
  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  private final ConfigurationProvider configurationProvider;

  @Override
  public String id() {
    return "CreateElasticsearchUserStep";
  }

  @Override
  public int retryCount() {
    return 3;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    boolean createUser = EnvironmentUtils.getBoolean("CREATE_USER_ES", false);
    if (!createUser) {
      log.info("Elasticsearch user creation is disabled, skipping user setup");
    }
    return !createUser;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      try {
        final String indexPrefix =
            configurationProvider.getElasticSearch().getIndex().getFinalPrefix();

        // Check for CREATE_USER_ES_USERNAME and CREATE_USER_ES_PASSWORD environment variables first
        String username = EnvironmentUtils.getString("CREATE_USER_ES_USERNAME");
        String password = EnvironmentUtils.getString("CREATE_USER_ES_PASSWORD");
        String iamRoleArn = EnvironmentUtils.getString("CREATE_USER_ES_IAM_ROLE_ARN");

        // Determine the authentication mode
        boolean usingIam = iamRoleArn != null && !iamRoleArn.isEmpty();
        boolean usingUserPassword =
            username != null && !username.isEmpty() && password != null && !password.isEmpty();

        // Validate that at least one authentication method is configured
        if (!usingIam && !usingUserPassword) {
          log.warn(
              "Either CREATE_USER_ES_IAM_ROLE_ARN or CREATE_USER_ES_USERNAME/CREATE_USER_ES_PASSWORD must be configured");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        String roleName = indexPrefix + "access";

        if (esComponents.getSearchClient().getEngineType().isOpenSearch()) {
          setupOpenSearchUser(
              indexPrefix,
              roleName,
              username,
              password,
              iamRoleArn,
              usingIam,
              usingUserPassword,
              context.opContext());
        } else {
          // Elasticsearch Cloud doesn't support IAM authentication
          if (usingIam) {
            log.warn("IAM authentication is only supported for AWS OpenSearch Service");
            return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
          }
          setupElasticsearchCloudUser(indexPrefix, roleName, username, password);
        }

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("CreateElasticsearchUserStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void setupElasticsearchCloudUser(
      String prefix, String roleName, String username, String password) throws Exception {
    log.info("Creating Elasticsearch Cloud user and role");
    IndexRoleUtils.createElasticsearchCloudUser(esComponents, roleName, username, password, prefix);
  }

  private void setupOpenSearchUser(
      String prefix,
      String roleName,
      String username,
      String password,
      String iamRoleArn,
      boolean usingIam,
      boolean usingUserPassword,
      OperationContext operationContext)
      throws Exception {
    // Check if this is AWS OpenSearch Service
    boolean isAwsOpenSearch = IndexUtils.isAwsOpenSearchService(esComponents);

    if (isAwsOpenSearch) {
      log.info("Detected AWS OpenSearch Service. Creating AWS-specific role.");

      // Create the role first (required for both modes)
      IndexRoleUtils.createAwsOpenSearchRole(esComponents, roleName, prefix);

      if (usingIam) {
        // IAM authentication: create role mapping to IAM role
        log.info("IAM mode: Creating role mapping for IAM role: {}", iamRoleArn);
        IndexRoleUtils.createAwsOpenSearchRoleMapping(esComponents, roleName, iamRoleArn);
      }

      if (usingUserPassword) {
        // Internal user authentication: create internal user
        log.info("Internal user mode: Creating internal user: {}", username);
        IndexRoleUtils.createAwsOpenSearchUser(
            esComponents, username, password, roleName, null, operationContext);
      }
    } else {
      log.warn("Detected self-hosted OpenSearch. Creating user and role not supported.");
    }
  }
}
