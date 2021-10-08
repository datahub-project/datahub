package com.linkedin.datahub.graphql.resolvers.config;

import com.datahub.metadata.authorization.Authorizer;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AnalyticsConfig;
import com.linkedin.datahub.graphql.generated.AppConfig;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.IdentityManagementConfig;
import com.linkedin.datahub.graphql.generated.PoliciesConfig;
import com.linkedin.datahub.graphql.generated.Privilege;
import com.linkedin.datahub.graphql.generated.ResourcePrivileges;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * Resolver responsible for serving app configurations to the React UI.
 */
public class AppConfigResolver implements DataFetcher<CompletableFuture<AppConfig>> {

  private final Boolean _isAnalyticsEnabled;

  public AppConfigResolver(final Boolean isAnalyticsEnabled) {
    _isAnalyticsEnabled = isAnalyticsEnabled;
  }

  @Override
  public CompletableFuture<AppConfig> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    final AppConfig appConfig = new AppConfig();

    final AnalyticsConfig analyticsConfig = new AnalyticsConfig();
    analyticsConfig.setEnabled(_isAnalyticsEnabled);

    final PoliciesConfig policiesConfig = new PoliciesConfig();

    boolean policiesEnabled = Authorizer.AuthorizationMode.DEFAULT.equals(context.getAuthorizer().mode());
    policiesConfig.setEnabled(policiesEnabled);

    policiesConfig.setPlatformPrivileges(com.linkedin.metadata.authorization.PoliciesConfig.PLATFORM_PRIVILEGES
        .stream()
        .map(this::mapPrivilege)
        .collect(Collectors.toList()));

    policiesConfig.setResourcePrivileges(com.linkedin.metadata.authorization.PoliciesConfig.RESOURCE_PRIVILEGES
        .stream()
        .map(this::mapResourcePrivileges)
        .collect(Collectors.toList())
    );

    final IdentityManagementConfig identityManagementConfig = new IdentityManagementConfig();
    identityManagementConfig.setEnabled(true); // Identity Management always enabled. TODO: Understand if there's a case where this should change.

    appConfig.setAnalyticsConfig(analyticsConfig);
    appConfig.setPoliciesConfig(policiesConfig);
    appConfig.setIdentityManagementConfig(identityManagementConfig);

    return CompletableFuture.completedFuture(appConfig);
  }

  private ResourcePrivileges mapResourcePrivileges(
      com.linkedin.metadata.authorization.PoliciesConfig.ResourcePrivileges resourcePrivileges) {
    final ResourcePrivileges graphQLPrivileges = new ResourcePrivileges();
    graphQLPrivileges.setResourceType(resourcePrivileges.getResourceType());
    graphQLPrivileges.setResourceTypeDisplayName(resourcePrivileges.getResourceTypeDisplayName());
    graphQLPrivileges.setEntityType(mapResourceTypeToEntityType(resourcePrivileges.getResourceType()));
    graphQLPrivileges.setPrivileges(
        resourcePrivileges.getPrivileges().stream().map(this::mapPrivilege).collect(Collectors.toList())
    );
    return graphQLPrivileges;
  }

  private Privilege mapPrivilege(com.linkedin.metadata.authorization.PoliciesConfig.Privilege privilege) {
    final Privilege graphQLPrivilege = new Privilege();
    graphQLPrivilege.setType(privilege.getType());
    graphQLPrivilege.setDisplayName(privilege.getDisplayName());
    graphQLPrivilege.setDescription(privilege.getDescription());
    return graphQLPrivilege;
  }

  private EntityType mapResourceTypeToEntityType(final String resourceType) {
    // TODO: Is there a better way to instruct the UI to present a searchable resource?
    if (com.linkedin.metadata.authorization.PoliciesConfig.DATASET_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.DATASET;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DASHBOARD_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.DASHBOARD;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CHART_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.CHART;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DATA_FLOW_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.DATA_FLOW;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DATA_JOB_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.DATA_JOB;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.TAG_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.TAG;
    } else {
      return null;
    }
  }
}