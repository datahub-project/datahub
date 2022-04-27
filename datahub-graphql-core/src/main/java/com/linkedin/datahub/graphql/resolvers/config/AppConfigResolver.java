package com.linkedin.datahub.graphql.resolvers.config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AnalyticsConfig;
import com.linkedin.datahub.graphql.generated.AppConfig;
import com.linkedin.datahub.graphql.generated.AuthConfig;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.IdentityManagementConfig;
import com.linkedin.datahub.graphql.generated.LineageConfig;
import com.linkedin.datahub.graphql.generated.ManagedIngestionConfig;
import com.linkedin.datahub.graphql.generated.PoliciesConfig;
import com.linkedin.datahub.graphql.generated.Privilege;
import com.linkedin.datahub.graphql.generated.ResourcePrivileges;
import com.linkedin.datahub.graphql.generated.VisualConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.version.GitVersion;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


/**
 * Resolver responsible for serving app configurations to the React UI.
 */
public class AppConfigResolver implements DataFetcher<CompletableFuture<AppConfig>> {

  private final GitVersion _gitVersion;
  private final boolean _isAnalyticsEnabled;
  private final IngestionConfiguration _ingestionConfiguration;
  private final AuthenticationConfiguration _authenticationConfiguration;
  private final AuthorizationConfiguration _authorizationConfiguration;
  private final boolean _supportsImpactAnalysis;
  private final VisualConfiguration _visualConfiguration;

  public AppConfigResolver(
      final GitVersion gitVersion,
      final boolean isAnalyticsEnabled,
      final IngestionConfiguration ingestionConfiguration,
      final AuthenticationConfiguration authenticationConfiguration,
      final AuthorizationConfiguration authorizationConfiguration,
      final boolean supportsImpactAnalysis,
      final VisualConfiguration visualConfiguration) {
    _gitVersion = gitVersion;
    _isAnalyticsEnabled = isAnalyticsEnabled;
    _ingestionConfiguration = ingestionConfiguration;
    _authenticationConfiguration = authenticationConfiguration;
    _authorizationConfiguration = authorizationConfiguration;
    _supportsImpactAnalysis = supportsImpactAnalysis;
    _visualConfiguration = visualConfiguration;
  }

  @Override
  public CompletableFuture<AppConfig> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    final AppConfig appConfig = new AppConfig();

    appConfig.setAppVersion(_gitVersion.getVersion());

    final LineageConfig lineageConfig = new LineageConfig();
    lineageConfig.setSupportsImpactAnalysis(_supportsImpactAnalysis);
    appConfig.setLineageConfig(lineageConfig);

    final AnalyticsConfig analyticsConfig = new AnalyticsConfig();
    analyticsConfig.setEnabled(_isAnalyticsEnabled);

    final AuthConfig authConfig = new AuthConfig();
    authConfig.setTokenAuthEnabled(_authenticationConfiguration.isEnabled());

    final PoliciesConfig policiesConfig = new PoliciesConfig();
    policiesConfig.setEnabled(_authorizationConfiguration.getDefaultAuthorizer().isEnabled());

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

    final ManagedIngestionConfig ingestionConfig = new ManagedIngestionConfig();
    ingestionConfig.setEnabled(_ingestionConfiguration.isEnabled());
    appConfig.setAuthConfig(authConfig);
    appConfig.setAnalyticsConfig(analyticsConfig);
    appConfig.setPoliciesConfig(policiesConfig);
    appConfig.setIdentityManagementConfig(identityManagementConfig);
    appConfig.setManagedIngestionConfig(ingestionConfig);
    appConfig.setAuthConfig(authConfig);
    appConfig.setVisualConfig(_visualConfiguration);

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
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.GLOSSARY_TERM_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.GLOSSARY_TERM;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DOMAIN_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.DOMAIN;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CONTAINER_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.CONTAINER;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CORP_GROUP_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.CORP_GROUP;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CORP_USER_PRIVILEGES.getResourceType().equals(resourceType)) {
      return EntityType.CORP_USER;
    } else {
      return null;
    }
  }
}
