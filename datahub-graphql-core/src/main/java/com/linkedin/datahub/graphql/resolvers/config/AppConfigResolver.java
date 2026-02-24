package com.linkedin.datahub.graphql.resolvers.config;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.generated.ApplicationConfig;
import com.linkedin.datahub.graphql.generated.EntityProfileConfig;
import com.linkedin.datahub.graphql.generated.QueriesTabConfig;
import com.linkedin.metadata.config.*;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for serving app configurations to the React UI. */
@Slf4j
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
  private final SearchBarConfiguration _searchBarConfig;
  private final SearchCardConfiguration _searchCardConfig;
  private final SearchFlagsConfiguration _searchFlagsConfig;
  private final HomePageConfiguration _homePageConfig;
  private final FeatureFlags _featureFlags;
  private final ChromeExtensionConfiguration _chromeExtensionConfiguration;
  private final SettingsService _settingsService;
  private final boolean _isS3Enabled;
  private final SemanticSearchConfiguration _semanticSearchConfiguration;

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
      final SearchBarConfiguration searchBarConfig,
      final SearchCardConfiguration searchCardConfig,
      final SearchFlagsConfiguration searchFlagsConfig,
      final HomePageConfiguration homePageConfig,
      final FeatureFlags featureFlags,
      final ChromeExtensionConfiguration chromeExtensionConfiguration,
      final SettingsService settingsService,
      final boolean isS3Enabled,
      final SemanticSearchConfiguration semanticSearchConfiguration) {
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
    _searchBarConfig = searchBarConfig;
    _searchCardConfig = searchCardConfig;
    _searchFlagsConfig = searchFlagsConfig;
    _homePageConfig = homePageConfig;
    _featureFlags = featureFlags;
    _chromeExtensionConfiguration = chromeExtensionConfiguration;
    _settingsService = settingsService;
    _isS3Enabled = isS3Enabled;
    _semanticSearchConfiguration = semanticSearchConfiguration;
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
      visualConfig.setShowFullTitleInLineage(_visualConfiguration.isShowFullTitleInLineage());
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
    if (_visualConfiguration != null && _visualConfiguration.getTheme() != null) {
      ThemeConfig themeConfig = new ThemeConfig();
      if (_visualConfiguration.getTheme().getThemeId() != null) {
        themeConfig.setThemeId(_visualConfiguration.getTheme().getThemeId());
      }
      visualConfig.setTheme(themeConfig);
    }
    if (_settingsService != null) {
      ApplicationConfig applicationConfig = new ApplicationConfig();
      final GlobalSettingsInfo globalSettings =
          _settingsService.getGlobalSettings(context.getOperationContext());
      if (globalSettings != null
          && globalSettings.hasApplications()
          && globalSettings.getApplications().hasEnabled()) {
        applicationConfig.setShowApplicationInNavigation(
            globalSettings.getApplications().isEnabled());
        applicationConfig.setShowSidebarSectionWhenEmpty(
            globalSettings.getApplications().isEnabled());
      } else {
        applicationConfig.setShowApplicationInNavigation(false);
        applicationConfig.setShowSidebarSectionWhenEmpty(false);
      }
      visualConfig.setApplication(applicationConfig);
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

    final SearchBarConfig searchBarConfig = new SearchBarConfig();
    try {
      searchBarConfig.setApiVariant(SearchBarAPI.valueOf(_searchBarConfig.getApiVariant()));
    } catch (IllegalArgumentException e) {
      searchBarConfig.setApiVariant(SearchBarAPI.AUTOCOMPLETE_FOR_MULTIPLE);
    }
    appConfig.setSearchBarConfig(searchBarConfig);

    final SearchCardConfig searchCardConfig = new SearchCardConfig();
    searchCardConfig.setShowDescription(_searchCardConfig.getShowDescription());
    appConfig.setSearchCardConfig(searchCardConfig);

    final SearchFlagsConfig searchFlagsConfig = new SearchFlagsConfig();
    searchFlagsConfig.setDefaultSkipHighlighting(_searchFlagsConfig.getDefaultSkipHighlighting());
    appConfig.setSearchFlagsConfig(searchFlagsConfig);

    final HomePageConfig homePageConfig = new HomePageConfig();
    try {
      homePageConfig.setFirstInPersonalSidebar(
          PersonalSidebarSection.valueOf(_homePageConfig.getFirstInPersonalSidebar()));
    } catch (Exception e) {
      log.warn(
          String.format(
              "Unexpected value set for firstInPersonalSidebar: %s",
              _homePageConfig.getFirstInPersonalSidebar()),
          e);
      homePageConfig.setFirstInPersonalSidebar(PersonalSidebarSection.YOUR_ASSETS);
    }
    appConfig.setHomePageConfig(homePageConfig);

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
            .setShowHasSiblingsFilter(_featureFlags.isShowHasSiblingsFilter())
            .setShowSearchBarAutocompleteRedesign(
                _featureFlags.isShowSearchBarAutocompleteRedesign())
            .setShowManageTags(_featureFlags.isShowManageTags())
            .setShowIntroducePage(_featureFlags.isShowIntroducePage())
            .setShowIngestionPageRedesign(_featureFlags.isShowIngestionPageRedesign())
            .setShowLineageExpandMore(_featureFlags.isShowLineageExpandMore())
            .setShowStatsTabRedesign(_featureFlags.isShowStatsTabRedesign())
            .setShowDefaultExternalLinks(_featureFlags.isShowDefaultExternalLinks())
            .setShowHomePageRedesign(_featureFlags.isShowHomePageRedesign())
            .setShowProductUpdates(_featureFlags.isShowProductUpdates())
            .setLineageGraphV3(_featureFlags.isLineageGraphV3())
            .setLogicalModelsEnabled(_featureFlags.isLogicalModelsEnabled())
            .setShowHomepageUserRole(_featureFlags.isShowHomepageUserRole())
            .setAssetSummaryPageV1(_featureFlags.isAssetSummaryPageV1())
            .setDatasetSummaryPageV1(_featureFlags.isDatasetSummaryPageV1())
            .setDocumentationFileUploadV1(isDocumentationFileUploadV1Enabled())
            .setContextDocumentsEnabled(_featureFlags.isContextDocumentsEnabled())
            .setIngestionOnboardingRedesignV1(_featureFlags.isIngestionOnboardingRedesignV1())
            .setHideLineageInSearchCards(_featureFlags.isHideLineageInSearchCards())
            .build();

    appConfig.setFeatureFlags(featureFlagsConfig);

    final ChromeExtensionConfig chromeExtensionConfig = new ChromeExtensionConfig();
    chromeExtensionConfig.setEnabled(_chromeExtensionConfiguration.isEnabled());
    chromeExtensionConfig.setLineageEnabled(_chromeExtensionConfiguration.isLineageEnabled());
    appConfig.setChromeExtensionConfig(chromeExtensionConfig);

    // Populate semantic search configuration
    if (_semanticSearchConfiguration != null) {
      final SemanticSearchConfig semanticSearchConfig = new SemanticSearchConfig();
      semanticSearchConfig.setEnabled(_semanticSearchConfiguration.isEnabled());
      semanticSearchConfig.setEnabledEntities(
          new java.util.ArrayList<>(_semanticSearchConfiguration.getEnabledEntities()));

      // Build EmbeddingConfig from server's embedding provider configuration
      if (_semanticSearchConfiguration.getEmbeddingProvider() != null) {
        final com.linkedin.metadata.config.search.EmbeddingProviderConfiguration providerConfig =
            _semanticSearchConfiguration.getEmbeddingProvider();

        final EmbeddingConfig embeddingConfig = new EmbeddingConfig();
        embeddingConfig.setProvider(providerConfig.getType());
        embeddingConfig.setModelId(providerConfig.getModelId());

        // Derive and set canonical model embedding key
        final String modelEmbeddingKey = deriveModelEmbeddingKey(providerConfig.getModelId());
        embeddingConfig.setModelEmbeddingKey(modelEmbeddingKey);

        // Populate provider-specific configuration
        if ("aws-bedrock".equalsIgnoreCase(providerConfig.getType())
            && providerConfig.getBedrock() != null
            && providerConfig.getBedrock().getAwsRegion() != null) {
          final AwsProviderConfig awsProviderConfig = new AwsProviderConfig();
          awsProviderConfig.setRegion(providerConfig.getBedrock().getAwsRegion());
          embeddingConfig.setAwsProviderConfig(awsProviderConfig);
        }

        semanticSearchConfig.setEmbeddingConfig(embeddingConfig);
      }

      appConfig.setSemanticSearchConfig(semanticSearchConfig);
    }

    return CompletableFuture.completedFuture(appConfig);
  }

  /**
   * Derive canonical model embedding key from model ID for use in SemanticContent aspects.
   *
   * <p>This is the single source of truth for modelEmbeddingKey derivation. Clients must use the
   * modelEmbeddingKey provided by this method to ensure consistency between client (writing
   * embeddings) and server (querying embeddings).
   *
   * <p>The modelEmbeddingKey is used as the key in the SemanticContent embeddings map and as the
   * field name in Elasticsearch indices.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>cohere.embed-english-v3 → cohere_embed_v3
   *   <li>cohere.embed-multilingual-v3 → cohere_embed_multilingual_v3
   *   <li>amazon.titan-embed-text-v1 → amazon_titan_v1
   *   <li>text-embedding-3-small → text_embedding_3_small
   *   <li>text-embedding-3-large → text_embedding_3_large
   *   <li>embed-english-v3.0 → embed_english_v3_0
   * </ul>
   */
  private static String deriveModelEmbeddingKey(final String modelId) {
    // Cohere native models (embed-english-v3.0, embed-multilingual-v3.0)
    // Check these FIRST because they also match the "embed-english-v3" pattern
    if (modelId.contains("embed-english-v3.0")) return "embed_english_v3_0";
    if (modelId.contains("embed-multilingual-v3.0")) return "embed_multilingual_v3_0";
    if (modelId.contains("embed-english-light-v3.0")) return "embed_english_light_v3_0";
    // AWS Bedrock Cohere models (without the .0 suffix)
    if (modelId.contains("embed-english-v3")) return "cohere_embed_v3";
    if (modelId.contains("embed-multilingual-v3")) return "cohere_embed_multilingual_v3";
    // AWS Bedrock Titan models
    if (modelId.contains("titan-embed-text-v1")) return "amazon_titan_v1";
    if (modelId.contains("titan-embed-text-v2")) return "amazon_titan_v2";
    // Fallback: replace special chars with underscores
    // This handles OpenAI models like text-embedding-3-small → text_embedding_3_small
    return modelId.replace("-", "_").replace(".", "_").replace(":", "_");
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
    } else if (com.linkedin.metadata.authorization.PoliciesConfig.PLATFORM_INSTANCE_PRIVILEGES
        .getResourceType()
        .equals(resourceType)) {
      return EntityType.DATA_PLATFORM_INSTANCE;
    } else {
      return null;
    }
  }

  private boolean isDocumentationFileUploadV1Enabled() {
    boolean isEnabledInConfig = _featureFlags.isDocumentationFileUploadV1();
    return isEnabledInConfig && _isS3Enabled;
  }
}
