package com.linkedin.datahub.graphql.resolvers.config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AnalyticsConfig;
import com.linkedin.datahub.graphql.generated.AppConfig;
import com.linkedin.datahub.graphql.generated.AuthConfig;
import com.linkedin.datahub.graphql.generated.ChromeExtensionConfig;
import com.linkedin.datahub.graphql.generated.EntityProfileConfig;
import com.linkedin.datahub.graphql.generated.EntityProfilesConfig;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FeatureFlagsConfig;
import com.linkedin.datahub.graphql.generated.IdentityManagementConfig;
import com.linkedin.datahub.graphql.generated.LineageConfig;
import com.linkedin.datahub.graphql.generated.ManagedIngestionConfig;
import com.linkedin.datahub.graphql.generated.PoliciesConfig;
import com.linkedin.datahub.graphql.generated.Privilege;
import com.linkedin.datahub.graphql.generated.QueriesTabConfig;
import com.linkedin.datahub.graphql.generated.ResourcePrivileges;
import com.linkedin.datahub.graphql.generated.SearchResultsVisualConfig;
import com.linkedin.datahub.graphql.generated.TelemetryConfig;
import com.linkedin.datahub.graphql.generated.TestsConfig;
import com.linkedin.datahub.graphql.generated.ViewsConfig;
import com.linkedin.datahub.graphql.generated.VisualConfig;
import com.linkedin.metadata.config.ChromeExtensionConfiguration;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.config.TestsConfiguration;
import com.linkedin.metadata.config.ViewsConfiguration;
import com.linkedin.metadata.config.VisualConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.version.GitVersion;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Resolver responsible for serving app configurations to the React UI. */
public class AppConfigResolver implements DataFetcher<CompletableFuture<AppConfig>> {

  private final GitVersion _gitVersion;
  private final boolean _isAnalyticsEnabled;
  private final IngestionConfiguration _ingestionConfiguration;
  private final AuthenticationConfiguration _authenticationConfiguration;
  private final AuthorizationConfiguration _authorizationConfiguration;
  private final boolean _supportsImpactAnalysis;
  private final VisualConfiguration _visualConfiguration;
  private final TelemetryConfiguration _telemetryConfiguration;
  private final TestsConfiguration _testsConfiguration;
  private final DataHubConfiguration _datahubConfiguration;
  private final ViewsConfiguration _viewsConfiguration;
  private final FeatureFlags _featureFlags;
  private final ChromeExtensionConfiguration _chromeExtensionConfiguration;

  public AppConfigResolver(
      final GitVersion gitVersion,
      final boolean isAnalyticsEnabled,
      final IngestionConfiguration ingestionConfiguration,
      final AuthenticationConfiguration authenticationConfiguration,
      final AuthorizationConfiguration authorizationConfiguration,
      final boolean supportsImpactAnalysis,
      final VisualConfiguration visualConfiguration,
      final TelemetryConfiguration telemetryConfiguration,
      final TestsConfiguration testsConfiguration,
      final DataHubConfiguration datahubConfiguration,
      final ViewsConfiguration viewsConfiguration,
      final FeatureFlags featureFlags,
      final ChromeExtensionConfiguration chromeExtensionConfiguration) {
    _gitVersion = gitVersion;
    _isAnalyticsEnabled = isAnalyticsEnabled;
    _ingestionConfiguration = ingestionConfiguration;
    _authenticationConfiguration = authenticationConfiguration;
    _authorizationConfiguration = authorizationConfiguration;
    _supportsImpactAnalysis = supportsImpactAnalysis;
    _visualConfiguration = visualConfiguration;
    _telemetryConfiguration = telemetryConfiguration;
    _testsConfiguration = testsConfiguration;
    _datahubConfiguration = datahubConfiguration;
    _viewsConfiguration = viewsConfiguration;
    _featureFlags = featureFlags;
    _chromeExtensionConfiguration = chromeExtensionConfiguration;
  }

  @Override
  public CompletableFuture<AppConfig> get(final DataFetchingEnvironment environment)
      throws Exception {

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

    policiesConfig.setPlatformPrivileges(
        com.linkedin.metadata.authorization.PoliciesConfig.PLATFORM_PRIVILEGES.stream()
            .map(this::mapPrivilege)
            .collect(Collectors.toList()));

    policiesConfig.setResourcePrivileges(
        com.linkedin.metadata.authorization.PoliciesConfig.RESOURCE_PRIVILEGES.stream()
            .map(this::mapResourcePrivileges)
            .collect(Collectors.toList()));

    final IdentityManagementConfig identityManagementConfig = new IdentityManagementConfig();
    identityManagementConfig.setEnabled(
        true); // Identity Management always enabled. TODO: Understand if there's a case where this
    // should change.

    final ManagedIngestionConfig ingestionConfig = new ManagedIngestionConfig();
    ingestionConfig.setEnabled(_ingestionConfiguration.isEnabled());

    appConfig.setAnalyticsConfig(analyticsConfig);
    appConfig.setPoliciesConfig(policiesConfig);
    appConfig.setIdentityManagementConfig(identityManagementConfig);
    appConfig.setManagedIngestionConfig(ingestionConfig);
    appConfig.setAuthConfig(authConfig);

    final VisualConfig visualConfig = new VisualConfig();
    if (_visualConfiguration != null) {
      if (_visualConfiguration.getAssets() != null) {
        visualConfig.setLogoUrl(_visualConfiguration.getAssets().getLogoUrl());
        visualConfig.setFaviconUrl(_visualConfiguration.getAssets().getFaviconUrl());
      }
      if (_visualConfiguration.getAppTitle() != null) {
        visualConfig.setAppTitle(_visualConfiguration.getAppTitle());
      }
      visualConfig.setHideGlossary(_visualConfiguration.isHideGlossary());
    }
    if (_visualConfiguration != null && _visualConfiguration.getQueriesTab() != null) {
      QueriesTabConfig queriesTabConfig = new QueriesTabConfig();
      queriesTabConfig.setQueriesTabResultSize(
          _visualConfiguration.getQueriesTab().getQueriesTabResultSize());
      visualConfig.setQueriesTab(queriesTabConfig);
    }
    if (_visualConfiguration != null && _visualConfiguration.getEntityProfile() != null) {
      EntityProfilesConfig entityProfilesConfig = new EntityProfilesConfig();
      if (_visualConfiguration.getEntityProfile().getDomainDefaultTab() != null) {
        EntityProfileConfig profileConfig = new EntityProfileConfig();
        profileConfig.setDefaultTab(_visualConfiguration.getEntityProfile().getDomainDefaultTab());
        entityProfilesConfig.setDomain(profileConfig);
      }
      visualConfig.setEntityProfiles(entityProfilesConfig);
    }
    if (_visualConfiguration != null && _visualConfiguration.getSearchResult() != null) {
      SearchResultsVisualConfig searchResultsVisualConfig = new SearchResultsVisualConfig();
      if (_visualConfiguration.getSearchResult().getEnableNameHighlight() != null) {
        searchResultsVisualConfig.setEnableNameHighlight(
            _visualConfiguration.getSearchResult().getEnableNameHighlight());
      }
      visualConfig.setSearchResult(searchResultsVisualConfig);
    }
    appConfig.setVisualConfig(visualConfig);

    final TelemetryConfig telemetryConfig = new TelemetryConfig();
    telemetryConfig.setEnableThirdPartyLogging(_telemetryConfiguration.isEnableThirdPartyLogging());
    appConfig.setTelemetryConfig(telemetryConfig);

    final TestsConfig testsConfig = new TestsConfig();
    testsConfig.setEnabled(_testsConfiguration.isEnabled());
    appConfig.setTestsConfig(testsConfig);

    final ViewsConfig viewsConfig = new ViewsConfig();
    viewsConfig.setEnabled(_viewsConfiguration.isEnabled());
    appConfig.setViewsConfig(viewsConfig);

    final FeatureFlagsConfig featureFlagsConfig =
        FeatureFlagsConfig.builder()
            .setShowSearchFiltersV2(_featureFlags.isShowSearchFiltersV2())
            .setBusinessAttributeEntityEnabled(_featureFlags.isBusinessAttributeEntityEnabled())
            .setReadOnlyModeEnabled(_featureFlags.isReadOnlyModeEnabled())
            .setShowBrowseV2(_featureFlags.isShowBrowseV2())
            .setShowAcrylInfo(_featureFlags.isShowAcrylInfo())
            .setErModelRelationshipFeatureEnabled(
                _featureFlags.isErModelRelationshipFeatureEnabled())
            .setShowAccessManagement(_featureFlags.isShowAccessManagement())
            .setNestedDomainsEnabled(_featureFlags.isNestedDomainsEnabled())
            .setPlatformBrowseV2(_featureFlags.isPlatformBrowseV2())
            .setDataContractsEnabled(_featureFlags.isDataContractsEnabled())
            .setEditableDatasetNameEnabled(_featureFlags.isEditableDatasetNameEnabled())
            .setThemeV2Enabled(_featureFlags.isThemeV2Enabled())
            .setThemeV2Default(_featureFlags.isThemeV2Default())
            .setThemeV2Toggleable(_featureFlags.isThemeV2Toggleable())
            .setLineageGraphV2(_featureFlags.isLineageGraphV2())
            .setShowSeparateSiblings(_featureFlags.isShowSeparateSiblings())
            .setShowManageStructuredProperties(_featureFlags.isShowManageStructuredProperties())
            .setSchemaFieldCLLEnabled(_featureFlags.isSchemaFieldCLLEnabled())
            .setHideDbtSourceInLineage(_featureFlags.isHideDbtSourceInLineage())
            .setSchemaFieldLineageIgnoreStatus(_featureFlags.isSchemaFieldLineageIgnoreStatus())
            .setShowNavBarRedesign(_featureFlags.isShowNavBarRedesign())
            .setShowAutoCompleteResults(_featureFlags.isShowAutoCompleteResults())
            .setEntityVersioningEnabled(_featureFlags.isEntityVersioning())
            .build();

    appConfig.setFeatureFlags(featureFlagsConfig);

    final ChromeExtensionConfig chromeExtensionConfig = new ChromeExtensionConfig();
    chromeExtensionConfig.setEnabled(_chromeExtensionConfiguration.isEnabled());
    chromeExtensionConfig.setLineageEnabled(_chromeExtensionConfiguration.isLineageEnabled());
    appConfig.setChromeExtensionConfig(chromeExtensionConfig);

    return CompletableFuture.completedFuture(appConfig);
  }

  private ResourcePrivileges mapResourcePrivileges(
      com.linkedin.metadata.authorization.PoliciesConfig.ResourcePrivileges resourcePrivileges) {
    final ResourcePrivileges graphQLPrivileges = new ResourcePrivileges();
    graphQLPrivileges.setResourceType(resourcePrivileges.getResourceType());
    graphQLPrivileges.setResourceTypeDisplayName(resourcePrivileges.getResourceTypeDisplayName());
    graphQLPrivileges.setEntityType(
        mapResourceTypeToEntityType(resourcePrivileges.getResourceType()));
    graphQLPrivileges.setPrivileges(
        resourcePrivileges.getPrivileges().stream()
            .map(this::mapPrivilege)
            .collect(Collectors.toList()));
    return graphQLPrivileges;
  }

  private Privilege mapPrivilege(
      com.linkedin.metadata.authorization.PoliciesConfig.Privilege privilege) {
    final Privilege graphQLPrivilege = new Privilege();
    graphQLPrivilege.setType(privilege.getType());
    graphQLPrivilege.setDisplayName(privilege.getDisplayName());
    graphQLPrivilege.setDescription(privilege.getDescription());
    return graphQLPrivilege;
  }

  private EntityType mapResourceTypeToEntityType(final String resourceType) {
    // TODO: Is there a better way to instruct the UI to present a searchable resource?
    if (com.linkedin.metadata.authorization.PoliciesConfig.DATASET_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.DATASET;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DASHBOARD_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.DASHBOARD;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CHART_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.CHART;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DATA_FLOW_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.DATA_FLOW;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DATA_JOB_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.DATA_JOB;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.TAG_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.TAG;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.GLOSSARY_TERM_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.GLOSSARY_TERM;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.GLOSSARY_NODE_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.GLOSSARY_NODE;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.DOMAIN_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.DOMAIN;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CONTAINER_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.CONTAINER;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CORP_GROUP_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.CORP_GROUP;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.CORP_USER_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.CORP_USER;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.ER_MODEL_RELATIONSHIP_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.ER_MODEL_RELATIONSHIP;
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.BUSINESS_ATTRIBUTE_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.BUSINESS_ATTRIBUTE;
    } else {
      return null;
    }
  }
}
