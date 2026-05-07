package com.linkedin.datahub.graphql.resolvers.config;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.AuthenticationConfiguration;
import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.DefaultAuthorizerConfiguration;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AppConfig;
import com.linkedin.datahub.graphql.generated.PersonalSidebarSection;
import com.linkedin.datahub.graphql.generated.SearchBarAPI;
import com.linkedin.metadata.config.*;
import com.linkedin.metadata.config.search.EmbeddingProviderConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.settings.global.ApplicationsSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AppConfigResolverTest {

  @Mock private GitVersion mockGitVersion;
  @Mock private IngestionConfiguration mockIngestionConfiguration;
  @Mock private AuthenticationConfiguration mockAuthenticationConfiguration;
  @Mock private AuthorizationConfiguration mockAuthorizationConfiguration;
  @Mock private DefaultAuthorizerConfiguration mockDefaultAuthorizer;
  @Mock private VisualConfiguration mockVisualConfiguration;
  @Mock private TelemetryConfiguration mockTelemetryConfiguration;
  @Mock private TestsConfiguration mockTestsConfiguration;
  @Mock private DataHubConfiguration mockDatahubConfiguration;
  @Mock private S3Configuration mockS3Configuration;
  @Mock private ViewsConfiguration mockViewsConfiguration;
  @Mock private SearchBarConfiguration mockSearchBarConfiguration;
  @Mock private SearchCardConfiguration mockSearchCardConfiguration;
  @Mock private SearchFlagsConfiguration mockSearchFlagsConfiguration;
  @Mock private HomePageConfiguration mockHomePageConfiguration;
  @Mock private FeatureFlags mockFeatureFlags;
  @Mock private ChromeExtensionConfiguration mockChromeExtensionConfiguration;
  @Mock private SettingsService mockSettingsService;
  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;
  @Mock private GlobalSettingsInfo mockGlobalSettingsInfo;
  @Mock private SemanticSearchConfiguration mockSemanticSearchConfiguration;

  private AppConfigResolver resolver;
  private QueryContext mockContext;

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);

    mockContext = getMockAllowContext();
    when(mockDataFetchingEnvironment.getContext()).thenReturn(mockContext);

    // Setup basic mock responses
    when(mockGitVersion.getVersion()).thenReturn("1.0.0");
    when(mockIngestionConfiguration.isEnabled()).thenReturn(true);
    when(mockAuthenticationConfiguration.isEnabled()).thenReturn(true);
    when(mockAuthorizationConfiguration.getDefaultAuthorizer()).thenReturn(mockDefaultAuthorizer);
    when(mockDefaultAuthorizer.isEnabled()).thenReturn(true);
    when(mockTelemetryConfiguration.isEnableThirdPartyLogging()).thenReturn(false);
    when(mockTestsConfiguration.isEnabled()).thenReturn(true);
    when(mockViewsConfiguration.isEnabled()).thenReturn(true);
    when(mockSearchBarConfiguration.getApiVariant()).thenReturn("AUTOCOMPLETE_FOR_MULTIPLE");
    when(mockSearchCardConfiguration.getShowDescription()).thenReturn(true);
    when(mockSearchFlagsConfiguration.getDefaultSkipHighlighting()).thenReturn(false);
    when(mockHomePageConfiguration.getFirstInPersonalSidebar()).thenReturn("YOUR_ASSETS");
    when(mockChromeExtensionConfiguration.isEnabled()).thenReturn(false);
    when(mockChromeExtensionConfiguration.isLineageEnabled()).thenReturn(false);

    // Setup S3 configuration
    when(mockDatahubConfiguration.getS3()).thenReturn(mockS3Configuration);
    when(mockS3Configuration.getBucketName()).thenReturn("test-bucket");

    // Setup feature flags
    setupFeatureFlags();

    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true, // isAnalyticsEnabled
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true, // supportsImpactAnalysis
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false, // isS3Enabled
            mockSemanticSearchConfiguration);
  }

  private void setupFeatureFlags() {
    when(mockFeatureFlags.isShowSearchFiltersV2()).thenReturn(false);
    when(mockFeatureFlags.isBusinessAttributeEntityEnabled()).thenReturn(false);
    when(mockFeatureFlags.isReadOnlyModeEnabled()).thenReturn(false);
    when(mockFeatureFlags.isShowBrowseV2()).thenReturn(false);
    when(mockFeatureFlags.isShowAcrylInfo()).thenReturn(false);
    when(mockFeatureFlags.isErModelRelationshipFeatureEnabled()).thenReturn(false);
    when(mockFeatureFlags.isShowAccessManagement()).thenReturn(false);
    when(mockFeatureFlags.isNestedDomainsEnabled()).thenReturn(false);
    when(mockFeatureFlags.isPlatformBrowseV2()).thenReturn(false);
    when(mockFeatureFlags.isDataContractsEnabled()).thenReturn(false);
    when(mockFeatureFlags.isEditableDatasetNameEnabled()).thenReturn(false);
    when(mockFeatureFlags.isThemeV2Enabled()).thenReturn(false);
    when(mockFeatureFlags.isThemeV2Default()).thenReturn(false);
    when(mockFeatureFlags.isThemeV2Toggleable()).thenReturn(false);
    when(mockFeatureFlags.isLineageGraphV2()).thenReturn(false);
    when(mockFeatureFlags.isShowSeparateSiblings()).thenReturn(false);
    when(mockFeatureFlags.isShowManageStructuredProperties()).thenReturn(false);
    when(mockFeatureFlags.isSchemaFieldCLLEnabled()).thenReturn(false);
    when(mockFeatureFlags.isHideDbtSourceInLineage()).thenReturn(false);
    when(mockFeatureFlags.isSchemaFieldLineageIgnoreStatus()).thenReturn(false);
    when(mockFeatureFlags.isShowNavBarRedesign()).thenReturn(false);
    when(mockFeatureFlags.isShowAutoCompleteResults()).thenReturn(false);
    when(mockFeatureFlags.isEntityVersioning()).thenReturn(false);
    when(mockFeatureFlags.isShowHasSiblingsFilter()).thenReturn(false);
    when(mockFeatureFlags.isShowSearchBarAutocompleteRedesign()).thenReturn(false);
    when(mockFeatureFlags.isShowManageTags()).thenReturn(false);
    when(mockFeatureFlags.isShowIntroducePage()).thenReturn(false);
    when(mockFeatureFlags.isShowIngestionPageRedesign()).thenReturn(false);
    when(mockFeatureFlags.isShowLineageExpandMore()).thenReturn(false);
    when(mockFeatureFlags.isShowStatsTabRedesign()).thenReturn(false);
    when(mockFeatureFlags.isShowHomePageRedesign()).thenReturn(false);
    when(mockFeatureFlags.isShowProductUpdates()).thenReturn(false);
    when(mockFeatureFlags.isLineageGraphV3()).thenReturn(false);
    when(mockFeatureFlags.isLogicalModelsEnabled()).thenReturn(false);
    when(mockFeatureFlags.isShowHomepageUserRole()).thenReturn(false);
    when(mockFeatureFlags.isAssetSummaryPageV1()).thenReturn(false);
    when(mockFeatureFlags.isDatasetSummaryPageV1()).thenReturn(false);
    when(mockFeatureFlags.isDocumentationFileUploadV1()).thenReturn(false);
  }

  @Test
  public void testGetBasicConfig() throws Exception {
    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result);
    assertEquals(result.getAppVersion(), "1.0.0");
    assertNotNull(result.getLineageConfig());
    assertTrue(result.getLineageConfig().getSupportsImpactAnalysis());
    assertNotNull(result.getAnalyticsConfig());
    assertTrue(result.getAnalyticsConfig().getEnabled());
    assertNotNull(result.getAuthConfig());
    assertTrue(result.getAuthConfig().getTokenAuthEnabled());
    assertNotNull(result.getPoliciesConfig());
    assertTrue(result.getPoliciesConfig().getEnabled());
    assertNotNull(result.getIdentityManagementConfig());
    assertTrue(result.getIdentityManagementConfig().getEnabled());
    assertNotNull(result.getManagedIngestionConfig());
    assertTrue(result.getManagedIngestionConfig().getEnabled());
    assertNotNull(result.getTelemetryConfig());
    assertFalse(result.getTelemetryConfig().getEnableThirdPartyLogging());
    assertNotNull(result.getTestsConfig());
    assertTrue(result.getTestsConfig().getEnabled());
    assertNotNull(result.getViewsConfig());
    assertTrue(result.getViewsConfig().getEnabled());
    assertNotNull(result.getSearchBarConfig());
    assertEquals(
        result.getSearchBarConfig().getApiVariant(), SearchBarAPI.AUTOCOMPLETE_FOR_MULTIPLE);
    assertNotNull(result.getSearchCardConfig());
    assertTrue(result.getSearchCardConfig().getShowDescription());
    assertNotNull(result.getHomePageConfig());
    assertEquals(
        result.getHomePageConfig().getFirstInPersonalSidebar(), PersonalSidebarSection.YOUR_ASSETS);
    assertNotNull(result.getFeatureFlags());
    assertNotNull(result.getChromeExtensionConfig());
    assertFalse(result.getChromeExtensionConfig().getEnabled());
  }

  @Test
  public void testGetConfigWithAnalyticsDisabled() throws Exception {
    resolver =
        new AppConfigResolver(
            mockGitVersion,
            false, // isAnalyticsEnabled
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false, // isS3Enabled
            mockSemanticSearchConfiguration);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getAnalyticsConfig());
    assertFalse(result.getAnalyticsConfig().getEnabled());
  }

  @Test
  public void testGetConfigWithVisualConfiguration() throws Exception {
    AssetsConfiguration mockAssets = mock(AssetsConfiguration.class);
    QueriesTabConfig mockQueriesTab = mock(QueriesTabConfig.class);
    EntityProfileConfig mockEntityProfile = mock(EntityProfileConfig.class);
    SearchResultVisualConfig mockSearchResult = mock(SearchResultVisualConfig.class);
    ThemeConfiguration mockTheme = mock(ThemeConfiguration.class);

    when(mockVisualConfiguration.getAssets()).thenReturn(mockAssets);
    when(mockAssets.getLogoUrl()).thenReturn("https://example.com/logo.png");
    when(mockAssets.getFaviconUrl()).thenReturn("https://example.com/favicon.ico");
    when(mockVisualConfiguration.getAppTitle()).thenReturn("Custom DataHub");
    when(mockVisualConfiguration.isHideGlossary()).thenReturn(true);
    when(mockVisualConfiguration.isShowFullTitleInLineage()).thenReturn(true);
    when(mockVisualConfiguration.getQueriesTab()).thenReturn(mockQueriesTab);
    when(mockQueriesTab.getQueriesTabResultSize()).thenReturn(20);
    when(mockVisualConfiguration.getEntityProfile()).thenReturn(mockEntityProfile);
    when(mockEntityProfile.getDomainDefaultTab()).thenReturn("DOCUMENTATION");
    when(mockVisualConfiguration.getSearchResult()).thenReturn(mockSearchResult);
    when(mockSearchResult.getEnableNameHighlight()).thenReturn(true);
    when(mockVisualConfiguration.getTheme()).thenReturn(mockTheme);
    when(mockTheme.getThemeId()).thenReturn("dark");

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getVisualConfig());
    assertEquals(result.getVisualConfig().getLogoUrl(), "https://example.com/logo.png");
    assertEquals(result.getVisualConfig().getFaviconUrl(), "https://example.com/favicon.ico");
    assertEquals(result.getVisualConfig().getAppTitle(), "Custom DataHub");
    assertTrue(result.getVisualConfig().getHideGlossary());
    assertTrue(result.getVisualConfig().getShowFullTitleInLineage());
    assertNotNull(result.getVisualConfig().getQueriesTab());
    assertEquals(
        result.getVisualConfig().getQueriesTab().getQueriesTabResultSize(), Integer.valueOf(20));
    assertNotNull(result.getVisualConfig().getEntityProfiles());
    assertNotNull(result.getVisualConfig().getEntityProfiles().getDomain());
    assertEquals(
        result.getVisualConfig().getEntityProfiles().getDomain().getDefaultTab(), "DOCUMENTATION");
    assertNotNull(result.getVisualConfig().getSearchResult());
    assertTrue(result.getVisualConfig().getSearchResult().getEnableNameHighlight());
    assertNotNull(result.getVisualConfig().getTheme());
    assertEquals(result.getVisualConfig().getTheme().getThemeId(), "dark");
  }

  @Test
  public void testGetConfigWithApplicationsEnabled() throws Exception {
    ApplicationsSettings mockApplications = mock(ApplicationsSettings.class);
    when(mockGlobalSettingsInfo.hasApplications()).thenReturn(true);
    when(mockGlobalSettingsInfo.getApplications()).thenReturn(mockApplications);
    when(mockApplications.hasEnabled()).thenReturn(true);
    when(mockApplications.isEnabled()).thenReturn(true);
    when(mockSettingsService.getGlobalSettings(any())).thenReturn(mockGlobalSettingsInfo);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getVisualConfig());
    assertNotNull(result.getVisualConfig().getApplication());
    assertTrue(result.getVisualConfig().getApplication().getShowApplicationInNavigation());
    assertTrue(result.getVisualConfig().getApplication().getShowSidebarSectionWhenEmpty());
  }

  @Test
  public void testGetConfigWithApplicationsDisabled() throws Exception {
    ApplicationsSettings mockApplications = mock(ApplicationsSettings.class);
    when(mockGlobalSettingsInfo.hasApplications()).thenReturn(true);
    when(mockGlobalSettingsInfo.getApplications()).thenReturn(mockApplications);
    when(mockApplications.hasEnabled()).thenReturn(true);
    when(mockApplications.isEnabled()).thenReturn(false);
    when(mockSettingsService.getGlobalSettings(any())).thenReturn(mockGlobalSettingsInfo);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getVisualConfig());
    assertNotNull(result.getVisualConfig().getApplication());
    assertFalse(result.getVisualConfig().getApplication().getShowApplicationInNavigation());
    assertFalse(result.getVisualConfig().getApplication().getShowSidebarSectionWhenEmpty());
  }

  @Test
  public void testGetConfigWithNullGlobalSettings() throws Exception {
    when(mockSettingsService.getGlobalSettings(any())).thenReturn(null);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getVisualConfig());
    assertNotNull(result.getVisualConfig().getApplication());
    assertFalse(result.getVisualConfig().getApplication().getShowApplicationInNavigation());
    assertFalse(result.getVisualConfig().getApplication().getShowSidebarSectionWhenEmpty());
  }

  @Test
  public void testGetConfigWithSearchCardDescriptionDisabled() throws Exception {
    when(mockSearchCardConfiguration.getShowDescription()).thenReturn(false);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSearchCardConfig());
    assertFalse(result.getSearchCardConfig().getShowDescription());
  }

  @Test
  public void testGetConfigWithFeatureFlagsEnabled() throws Exception {
    // Enable some feature flags
    when(mockFeatureFlags.isShowSearchFiltersV2()).thenReturn(true);
    when(mockFeatureFlags.isBusinessAttributeEntityEnabled()).thenReturn(true);
    when(mockFeatureFlags.isReadOnlyModeEnabled()).thenReturn(true);
    when(mockFeatureFlags.isThemeV2Enabled()).thenReturn(true);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getFeatureFlags());
    assertTrue(result.getFeatureFlags().getShowSearchFiltersV2());
    assertTrue(result.getFeatureFlags().getBusinessAttributeEntityEnabled());
    assertTrue(result.getFeatureFlags().getReadOnlyModeEnabled());
    assertTrue(result.getFeatureFlags().getThemeV2Enabled());
  }

  @Test
  public void testGetConfigWithInvalidSearchBarAPI() throws Exception {
    when(mockSearchBarConfiguration.getApiVariant()).thenReturn("INVALID_API");

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSearchBarConfig());
    assertEquals(
        result.getSearchBarConfig().getApiVariant(), SearchBarAPI.AUTOCOMPLETE_FOR_MULTIPLE);
  }

  @Test
  public void testGetConfigWithInvalidPersonalSidebarSection() throws Exception {
    when(mockHomePageConfiguration.getFirstInPersonalSidebar()).thenReturn("INVALID_SECTION");

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getHomePageConfig());
    assertEquals(
        result.getHomePageConfig().getFirstInPersonalSidebar(), PersonalSidebarSection.YOUR_ASSETS);
  }

  @Test
  public void testGetConfigWithNullSettingsService() throws Exception {
    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true,
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            null, // null settings service
            false,
            mockSemanticSearchConfiguration);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result);
    assertNotNull(result.getVisualConfig());
    // Should not crash and should handle null settings service gracefully
  }

  @Test
  public void testGetConfigWithNullVisualConfiguration() throws Exception {
    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true,
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            null, // null visual configuration
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false,
            mockSemanticSearchConfiguration);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result);
    assertNotNull(result.getVisualConfig());
    // Should handle null visual configuration gracefully
  }

  @Test
  public void testPoliciesConfigMapping() throws Exception {
    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getPoliciesConfig());
    assertNotNull(result.getPoliciesConfig().getPlatformPrivileges());
    assertNotNull(result.getPoliciesConfig().getResourcePrivileges());
    // Verify that the privileges lists are populated (they come from static config)
    assertFalse(result.getPoliciesConfig().getPlatformPrivileges().isEmpty());
    assertFalse(result.getPoliciesConfig().getResourcePrivileges().isEmpty());
  }

  @Test
  public void testDocumentationFileUploadV1EnabledWhenFeatureFlagAndS3Enabled() throws Exception {
    when(mockFeatureFlags.isDocumentationFileUploadV1()).thenReturn(true);
    when(mockS3Configuration.getBucketName()).thenReturn("my-bucket");

    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true, // isAnalyticsEnabled
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true, // supportsImpactAnalysis
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            true, // isS3Enabled
            mockSemanticSearchConfiguration);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getFeatureFlags());
    assertTrue(result.getFeatureFlags().getDocumentationFileUploadV1());
  }

  @Test
  public void testDocumentationFileUploadV1DisabledWhenFeatureFlagDisabledAndS3Enabled()
      throws Exception {
    when(mockFeatureFlags.isDocumentationFileUploadV1()).thenReturn(false);

    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true, // isAnalyticsEnabled
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true, // supportsImpactAnalysis
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            true, // isS3Enabled
            mockSemanticSearchConfiguration);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getFeatureFlags());
    assertFalse(result.getFeatureFlags().getDocumentationFileUploadV1());
  }

  @Test
  public void testDocumentationFileUploadV1DisabledWhenFeatureFlagEnabledAndS3Disabled()
      throws Exception {
    when(mockFeatureFlags.isDocumentationFileUploadV1()).thenReturn(true);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getFeatureFlags());
    assertFalse(result.getFeatureFlags().getDocumentationFileUploadV1());
  }

  @Test
  public void testDocumentationFileUploadV1DisabledWhenFeatureFlagAndS3Disabled() throws Exception {
    when(mockFeatureFlags.isDocumentationFileUploadV1()).thenReturn(false);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getFeatureFlags());
    assertFalse(result.getFeatureFlags().getDocumentationFileUploadV1());
  }

  @Test
  public void testSemanticSearchConfigPopulated() throws Exception {
    // Setup semantic search configuration
    Set<String> enabledEntities = new HashSet<>();
    enabledEntities.add("document");
    enabledEntities.add("chart");

    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("aws-bedrock");
    EmbeddingProviderConfiguration.BedrockConfig bedrockConfig =
        new EmbeddingProviderConfiguration.BedrockConfig();
    bedrockConfig.setModel("cohere.embed-english-v3");
    bedrockConfig.setAwsRegion("us-west-2");
    embeddingProvider.setBedrock(bedrockConfig);

    ModelEmbeddingConfig modelConfig = new ModelEmbeddingConfig();
    modelConfig.setVectorDimension(1024);

    Map<String, ModelEmbeddingConfig> models = new HashMap<>();
    models.put("cohere_embed_v3", modelConfig);

    SemanticSearchConfiguration semanticSearchConfig = new SemanticSearchConfiguration();
    semanticSearchConfig.setEnabled(true);
    semanticSearchConfig.setEnabledEntities(enabledEntities);
    semanticSearchConfig.setModels(models);
    semanticSearchConfig.setEmbeddingProvider(embeddingProvider);

    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true,
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false,
            semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertTrue(result.getSemanticSearchConfig().getEnabled());
    assertNotNull(result.getSemanticSearchConfig().getEnabledEntities());
    assertEquals(result.getSemanticSearchConfig().getEnabledEntities().size(), 2);
    assertTrue(result.getSemanticSearchConfig().getEnabledEntities().contains("document"));
    assertTrue(result.getSemanticSearchConfig().getEnabledEntities().contains("chart"));

    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getProvider(), "aws-bedrock");
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getModelId(),
        "cohere.embed-english-v3");
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getModelEmbeddingKey(),
        "cohere_embed_v3");
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig().getAwsProviderConfig());
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getAwsProviderConfig().getRegion(),
        "us-west-2");
  }

  @Test
  public void testSemanticSearchConfigPopulatedWithVertexAi() throws Exception {
    // Setup semantic search configuration with vertex_ai provider
    Set<String> enabledEntities = new HashSet<>();
    enabledEntities.add("document");
    enabledEntities.add("chart");

    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("vertex_ai");
    EmbeddingProviderConfiguration.VertexAiConfig vertexConfig =
        new EmbeddingProviderConfiguration.VertexAiConfig();
    vertexConfig.setProjectId("test-project");
    vertexConfig.setLocation("us-east1");
    vertexConfig.setModel("gemini-embedding-001");
    embeddingProvider.setVertexai(vertexConfig);

    ModelEmbeddingConfig modelConfig = new ModelEmbeddingConfig();
    modelConfig.setVectorDimension(768);

    Map<String, ModelEmbeddingConfig> models = new HashMap<>();
    models.put("gemini_embedding_001", modelConfig);

    SemanticSearchConfiguration semanticSearchConfig = new SemanticSearchConfiguration();
    semanticSearchConfig.setEnabled(true);
    semanticSearchConfig.setEnabledEntities(enabledEntities);
    semanticSearchConfig.setModels(models);
    semanticSearchConfig.setEmbeddingProvider(embeddingProvider);

    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true,
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false,
            semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    assertEquals(result.getSemanticSearchConfig().getEmbeddingConfig().getProvider(), "vertex_ai");
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getModelId(), "gemini-embedding-001");
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getModelEmbeddingKey(),
        "gemini_embedding_001");
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig().getVertexProviderConfig());
    assertEquals(
        result
            .getSemanticSearchConfig()
            .getEmbeddingConfig()
            .getVertexProviderConfig()
            .getProjectId(),
        "test-project");
    assertEquals(
        result
            .getSemanticSearchConfig()
            .getEmbeddingConfig()
            .getVertexProviderConfig()
            .getLocation(),
        "us-east1");
    assertNull(result.getSemanticSearchConfig().getEmbeddingConfig().getAwsProviderConfig());
  }

  @Test
  public void testSemanticSearchConfigOmitsVertexProviderConfigWhenLocationBlank()
      throws Exception {
    // When the server has vertex_ai configured but VERTEX_AI_LOCATION is blank
    // (Spring binds an empty default ${VERTEX_AI_LOCATION:} as ""), the resolver
    // must not emit a half-populated vertexProviderConfig to the client.
    Set<String> enabledEntities = new HashSet<>();
    enabledEntities.add("document");

    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("vertex_ai");
    EmbeddingProviderConfiguration.VertexAiConfig vertexConfig =
        new EmbeddingProviderConfiguration.VertexAiConfig();
    vertexConfig.setProjectId("test-project");
    vertexConfig.setLocation(""); // blank location — exercises the isBlank() guard
    vertexConfig.setModel("gemini-embedding-001");
    embeddingProvider.setVertexai(vertexConfig);

    SemanticSearchConfiguration semanticSearchConfig = new SemanticSearchConfiguration();
    semanticSearchConfig.setEnabled(true);
    semanticSearchConfig.setEnabledEntities(enabledEntities);
    semanticSearchConfig.setEmbeddingProvider(embeddingProvider);

    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true,
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false,
            semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    // The provider type is still echoed back, but no half-populated vertex config is emitted.
    assertEquals(result.getSemanticSearchConfig().getEmbeddingConfig().getProvider(), "vertex_ai");
    assertNull(result.getSemanticSearchConfig().getEmbeddingConfig().getVertexProviderConfig());
  }

  @Test
  public void testSemanticSearchConfigNull() throws Exception {
    resolver =
        new AppConfigResolver(
            mockGitVersion,
            true,
            mockIngestionConfiguration,
            mockAuthenticationConfiguration,
            mockAuthorizationConfiguration,
            true,
            mockVisualConfiguration,
            mockTelemetryConfiguration,
            mockTestsConfiguration,
            mockDatahubConfiguration,
            mockViewsConfiguration,
            mockSearchBarConfiguration,
            mockSearchCardConfiguration,
            mockSearchFlagsConfiguration,
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService,
            false,
            null);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result);
  }

  /**
   * Builds an {@link AppConfigResolver} with the given semantic-search config and otherwise the
   * mocks set up in {@link #setupTest()}. Mirrors the inline resolver-rebuild pattern used in the
   * existing tests but in helper form so {@code deriveModelEmbeddingKey} branches can be exercised
   * without repeating the 20-arg constructor each time.
   */
  private AppConfigResolver resolverWithSemanticSearchConfig(
      SemanticSearchConfiguration semanticSearchConfig) {
    return new AppConfigResolver(
        mockGitVersion,
        true,
        mockIngestionConfiguration,
        mockAuthenticationConfiguration,
        mockAuthorizationConfiguration,
        true,
        mockVisualConfiguration,
        mockTelemetryConfiguration,
        mockTestsConfiguration,
        mockDatahubConfiguration,
        mockViewsConfiguration,
        mockSearchBarConfiguration,
        mockSearchCardConfiguration,
        mockSearchFlagsConfiguration,
        mockHomePageConfiguration,
        mockFeatureFlags,
        mockChromeExtensionConfiguration,
        mockSettingsService,
        false,
        semanticSearchConfig);
  }

  /**
   * Builds a semantic-search config wrapping the given embedding-provider config, with a single
   * "document" enabled entity. Used by the model-embedding-key tests to keep them concise.
   */
  private static SemanticSearchConfiguration semanticSearchConfigWith(
      EmbeddingProviderConfiguration embeddingProvider) {
    Set<String> enabledEntities = new HashSet<>();
    enabledEntities.add("document");

    SemanticSearchConfiguration semanticSearchConfig = new SemanticSearchConfiguration();
    semanticSearchConfig.setEnabled(true);
    semanticSearchConfig.setEnabledEntities(enabledEntities);
    semanticSearchConfig.setEmbeddingProvider(embeddingProvider);
    return semanticSearchConfig;
  }

  private static EmbeddingProviderConfiguration bedrockEmbeddingProvider(String model) {
    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("aws-bedrock");
    EmbeddingProviderConfiguration.BedrockConfig bedrockConfig =
        new EmbeddingProviderConfiguration.BedrockConfig();
    bedrockConfig.setModel(model);
    bedrockConfig.setAwsRegion("us-west-2");
    embeddingProvider.setBedrock(bedrockConfig);
    return embeddingProvider;
  }

  private static EmbeddingProviderConfiguration vertexAiEmbeddingProvider(
      String projectId, String location, String model) {
    EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
    embeddingProvider.setType("vertex_ai");
    EmbeddingProviderConfiguration.VertexAiConfig vertexConfig =
        new EmbeddingProviderConfiguration.VertexAiConfig();
    vertexConfig.setProjectId(projectId);
    vertexConfig.setLocation(location);
    vertexConfig.setModel(model);
    embeddingProvider.setVertexai(vertexConfig);
    return embeddingProvider;
  }

  /**
   * Helper that drives the resolver with a single-model semantic-search config and asserts the
   * derived modelEmbeddingKey.
   */
  private void assertDerivedModelEmbeddingKey(
      EmbeddingProviderConfiguration embeddingProvider, String expectedKey) throws Exception {
    SemanticSearchConfiguration semanticSearchConfig = semanticSearchConfigWith(embeddingProvider);
    resolver = resolverWithSemanticSearchConfig(semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    assertEquals(
        result.getSemanticSearchConfig().getEmbeddingConfig().getModelEmbeddingKey(), expectedKey);
  }

  @Test
  public void testDeriveModelEmbeddingKeyForCohereEmbedEnglishV3() throws Exception {
    assertDerivedModelEmbeddingKey(
        bedrockEmbeddingProvider("cohere.embed-english-v3"), "cohere_embed_v3");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForCohereEmbedEnglishV3_0() throws Exception {
    // Cohere native model IDs (with .0 suffix) must take precedence over the bedrock-style match.
    EmbeddingProviderConfiguration cohere = new EmbeddingProviderConfiguration();
    cohere.setType("cohere");
    EmbeddingProviderConfiguration.CohereConfig cc =
        new EmbeddingProviderConfiguration.CohereConfig();
    cc.setModel("embed-english-v3.0");
    cohere.setCohere(cc);
    assertDerivedModelEmbeddingKey(cohere, "embed_english_v3_0");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForCohereMultilingualV3_0() throws Exception {
    EmbeddingProviderConfiguration cohere = new EmbeddingProviderConfiguration();
    cohere.setType("cohere");
    EmbeddingProviderConfiguration.CohereConfig cc =
        new EmbeddingProviderConfiguration.CohereConfig();
    cc.setModel("embed-multilingual-v3.0");
    cohere.setCohere(cc);
    assertDerivedModelEmbeddingKey(cohere, "embed_multilingual_v3_0");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForCohereEnglishLightV3_0() throws Exception {
    EmbeddingProviderConfiguration cohere = new EmbeddingProviderConfiguration();
    cohere.setType("cohere");
    EmbeddingProviderConfiguration.CohereConfig cc =
        new EmbeddingProviderConfiguration.CohereConfig();
    cc.setModel("embed-english-light-v3.0");
    cohere.setCohere(cc);
    assertDerivedModelEmbeddingKey(cohere, "embed_english_light_v3_0");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForCohereMultilingualV3() throws Exception {
    assertDerivedModelEmbeddingKey(
        bedrockEmbeddingProvider("cohere.embed-multilingual-v3"), "cohere_embed_multilingual_v3");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForTitanV1() throws Exception {
    assertDerivedModelEmbeddingKey(
        bedrockEmbeddingProvider("amazon.titan-embed-text-v1"), "amazon_titan_v1");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForTitanV2() throws Exception {
    assertDerivedModelEmbeddingKey(
        bedrockEmbeddingProvider("amazon.titan-embed-text-v2:0"), "amazon_titan_v2");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForGeminiEmbedding001() throws Exception {
    assertDerivedModelEmbeddingKey(
        vertexAiEmbeddingProvider("p", "us-east1", "gemini-embedding-001"), "gemini_embedding_001");
  }

  @Test
  public void testDeriveModelEmbeddingKeyForTextEmbedding005() throws Exception {
    assertDerivedModelEmbeddingKey(
        vertexAiEmbeddingProvider("p", "us-east1", "text-embedding-005"), "text_embedding_005");
  }

  /**
   * Fallback path: model IDs that don't match any of the canonical-key shortcuts get their special
   * characters replaced with underscores.
   */
  @Test
  public void testDeriveModelEmbeddingKeyFallback() throws Exception {
    EmbeddingProviderConfiguration openai = new EmbeddingProviderConfiguration();
    openai.setType("openai");
    EmbeddingProviderConfiguration.OpenAIConfig oc =
        new EmbeddingProviderConfiguration.OpenAIConfig();
    oc.setModel("some-unknown-model.v2:0");
    openai.setOpenai(oc);
    assertDerivedModelEmbeddingKey(openai, "some_unknown_model_v2_0");
  }

  // ------- Vertex AI guard branches in get() -------

  /**
   * Mirrors {@link #testSemanticSearchConfigOmitsVertexProviderConfigWhenLocationBlank} for null.
   */
  @Test
  public void testSemanticSearchConfigOmitsVertexProviderConfigWhenLocationNull() throws Exception {
    SemanticSearchConfiguration semanticSearchConfig =
        semanticSearchConfigWith(
            vertexAiEmbeddingProvider("test-project", null, "gemini-embedding-001"));

    resolver = resolverWithSemanticSearchConfig(semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    assertEquals(result.getSemanticSearchConfig().getEmbeddingConfig().getProvider(), "vertex_ai");
    assertNull(result.getSemanticSearchConfig().getEmbeddingConfig().getVertexProviderConfig());
  }

  @Test
  public void testSemanticSearchConfigOmitsVertexProviderConfigWhenProjectIdBlank()
      throws Exception {
    SemanticSearchConfiguration semanticSearchConfig =
        semanticSearchConfigWith(vertexAiEmbeddingProvider("", "us-east1", "gemini-embedding-001"));

    resolver = resolverWithSemanticSearchConfig(semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    assertEquals(result.getSemanticSearchConfig().getEmbeddingConfig().getProvider(), "vertex_ai");
    assertNull(result.getSemanticSearchConfig().getEmbeddingConfig().getVertexProviderConfig());
  }

  @Test
  public void testSemanticSearchConfigOmitsVertexProviderConfigWhenProjectIdNull()
      throws Exception {
    SemanticSearchConfiguration semanticSearchConfig =
        semanticSearchConfigWith(
            vertexAiEmbeddingProvider(null, "us-east1", "gemini-embedding-001"));

    resolver = resolverWithSemanticSearchConfig(semanticSearchConfig);

    AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

    assertNotNull(result.getSemanticSearchConfig());
    assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
    assertEquals(result.getSemanticSearchConfig().getEmbeddingConfig().getProvider(), "vertex_ai");
    assertNull(result.getSemanticSearchConfig().getEmbeddingConfig().getVertexProviderConfig());
  }

  @Test
  public void testSemanticSearchConfigModelEmbeddingKeyDerivation() throws Exception {
    // Test a few key model ID to model embedding key mappings
    String[][] testCases = {
      {"cohere.embed-english-v3", "cohere_embed_v3"},
      {"cohere.embed-multilingual-v3", "cohere_embed_multilingual_v3"}
    };

    for (String[] testCase : testCases) {
      String modelId = testCase[0];
      String expectedModelEmbeddingKey = testCase[1];

      Set<String> enabledEntities = new HashSet<>();
      enabledEntities.add("document");

      EmbeddingProviderConfiguration embeddingProvider = new EmbeddingProviderConfiguration();
      embeddingProvider.setType("aws-bedrock");
      EmbeddingProviderConfiguration.BedrockConfig bedrockConfig =
          new EmbeddingProviderConfiguration.BedrockConfig();
      bedrockConfig.setModel(modelId);
      bedrockConfig.setAwsRegion("us-west-2");
      embeddingProvider.setBedrock(bedrockConfig);

      ModelEmbeddingConfig modelConfig = new ModelEmbeddingConfig();
      modelConfig.setVectorDimension(1024);

      Map<String, ModelEmbeddingConfig> models = new HashMap<>();
      models.put(expectedModelEmbeddingKey, modelConfig);

      SemanticSearchConfiguration semanticSearchConfig = new SemanticSearchConfiguration();
      semanticSearchConfig.setEnabled(true);
      semanticSearchConfig.setEnabledEntities(enabledEntities);
      semanticSearchConfig.setModels(models);
      semanticSearchConfig.setEmbeddingProvider(embeddingProvider);

      resolver =
          new AppConfigResolver(
              mockGitVersion,
              true,
              mockIngestionConfiguration,
              mockAuthenticationConfiguration,
              mockAuthorizationConfiguration,
              true,
              mockVisualConfiguration,
              mockTelemetryConfiguration,
              mockTestsConfiguration,
              mockDatahubConfiguration,
              mockViewsConfiguration,
              mockSearchBarConfiguration,
              mockSearchCardConfiguration,
              mockSearchFlagsConfiguration,
              mockHomePageConfiguration,
              mockFeatureFlags,
              mockChromeExtensionConfiguration,
              mockSettingsService,
              false,
              semanticSearchConfig);

      AppConfig result = resolver.get(mockDataFetchingEnvironment).get();

      assertNotNull(result.getSemanticSearchConfig());
      assertNotNull(result.getSemanticSearchConfig().getEmbeddingConfig());
      assertEquals(
          result.getSemanticSearchConfig().getEmbeddingConfig().getModelEmbeddingKey(),
          expectedModelEmbeddingKey,
          "Model embedding key derivation failed for model ID: " + modelId);
    }
  }
}
