package com.linkedin.datahub.upgrade.system.elasticsearch.steps;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexRoleUtils;
import com.linkedin.datahub.upgrade.system.elasticsearch.util.IndexUtils;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.utils.EnvironmentUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CreateUserStep implements UpgradeStep {
  private final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;
  private final ConfigurationProvider configurationProvider;
  @Nullable private final SearchClusterRegistry searchClusterRegistry;

  public CreateUserStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      ConfigurationProvider configurationProvider) {
    this(esComponents, configurationProvider, null);
  }

  public CreateUserStep(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents,
      ConfigurationProvider configurationProvider,
      @Nullable SearchClusterRegistry searchClusterRegistry) {
    this.esComponents = esComponents;
    this.configurationProvider = configurationProvider;
    this.searchClusterRegistry = searchClusterRegistry;
  }

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

        String username = EnvironmentUtils.getString("CREATE_USER_ES_USERNAME");
        String password = EnvironmentUtils.getString("CREATE_USER_ES_PASSWORD");
        String iamRoleArn = EnvironmentUtils.getString("CREATE_USER_ES_IAM_ROLE_ARN");

        boolean usingIam = iamRoleArn != null && !iamRoleArn.isEmpty();
        boolean usingUserPassword =
            username != null && !username.isEmpty() && password != null && !password.isEmpty();

        if (!usingIam && !usingUserPassword) {
          log.warn(
              "Either CREATE_USER_ES_IAM_ROLE_ARN or CREATE_USER_ES_USERNAME/CREATE_USER_ES_PASSWORD must be configured");
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }

        String roleName = indexPrefix + "access";
        for (BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster :
            clustersToConfigure()) {
          log.info(
              "Creating search user/role on cluster engine {}",
              cluster.getSearchClient().getEngineType());
          setupUserOnCluster(
              cluster,
              context.opContext(),
              indexPrefix,
              roleName,
              username,
              password,
              iamRoleArn,
              usingIam,
              usingUserPassword);
        }

        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("CreateElasticsearchUserStep failed.", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  @Nonnull
  private List<BaseElasticSearchComponentsFactory.BaseElasticSearchComponents>
      clustersToConfigure() {
    if (searchClusterRegistry == null) {
      return List.of(esComponents);
    }
    List<BaseElasticSearchComponentsFactory.BaseElasticSearchComponents> clusters =
        new ArrayList<>();
    for (SearchClusterRegistry.ClusterConnection connection :
        searchClusterRegistry.uniqueConnections()) {
      clusters.add(connection.asComponents(esComponents.getIndexConvention()));
    }
    return clusters;
  }

  private void setupUserOnCluster(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      OperationContext operationContext,
      String prefix,
      String roleName,
      String username,
      String password,
      String iamRoleArn,
      boolean usingIam,
      boolean usingUserPassword)
      throws Exception {
    if (cluster.getSearchClient().getEngineType().isOpenSearch()) {
      setupOpenSearchUser(
          cluster,
          prefix,
          roleName,
          username,
          password,
          iamRoleArn,
          usingIam,
          usingUserPassword,
          operationContext);
      return;
    }
    if (usingIam) {
      log.warn("IAM authentication is only supported for AWS OpenSearch Service");
      throw new IllegalStateException(
          "IAM authentication is only supported for AWS OpenSearch Service");
    }
    log.info("Creating Elasticsearch Cloud user and role");
    IndexRoleUtils.createElasticsearchCloudUser(
        operationContext, cluster, roleName, username, password, prefix);
  }

  private void setupOpenSearchUser(
      BaseElasticSearchComponentsFactory.BaseElasticSearchComponents cluster,
      String prefix,
      String roleName,
      String username,
      String password,
      String iamRoleArn,
      boolean usingIam,
      boolean usingUserPassword,
      OperationContext operationContext)
      throws Exception {
    boolean isAwsOpenSearch = IndexUtils.isAwsOpenSearchService(cluster);

    if (isAwsOpenSearch) {
      log.info("Detected AWS OpenSearch Service. Creating AWS-specific role.");

      IndexRoleUtils.createAwsOpenSearchRole(operationContext, cluster, roleName, prefix);

      if (usingIam) {
        log.info("IAM mode: Creating role mapping for IAM role: {}", iamRoleArn);
        IndexRoleUtils.createAwsOpenSearchRoleMapping(
            operationContext, cluster, roleName, iamRoleArn);
      }

      if (usingUserPassword) {
        log.info("Internal user mode: Creating internal user: {}", username);
        IndexRoleUtils.createAwsOpenSearchUser(
            operationContext, cluster, username, password, roleName, null);
      }
    } else {
      log.warn("Detected self-hosted OpenSearch. Creating user and role not supported.");
    }
  }
}
