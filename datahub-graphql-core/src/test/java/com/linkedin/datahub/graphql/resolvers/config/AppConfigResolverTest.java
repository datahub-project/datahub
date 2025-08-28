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
import com.linkedin.metadata.config.telemetry.TelemetryConfiguration;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.settings.global.ApplicationsSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
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
  @Mock private ViewsConfiguration mockViewsConfiguration;
  @Mock private SearchBarConfiguration mockSearchBarConfiguration;
  @Mock private SearchCardConfiguration mockSearchCardConfiguration;
  @Mock private HomePageConfiguration mockHomePageConfiguration;
  @Mock private FeatureFlags mockFeatureFlags;
  @Mock private ChromeExtensionConfiguration mockChromeExtensionConfiguration;
  @Mock private SettingsService mockSettingsService;
  @Mock private DataFetchingEnvironment mockDataFetchingEnvironment;
  @Mock private GlobalSettingsInfo mockGlobalSettingsInfo;

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
    when(mockHomePageConfiguration.getFirstInPersonalSidebar()).thenReturn("YOUR_ASSETS");
    when(mockChromeExtensionConfiguration.isEnabled()).thenReturn(false);
    when(mockChromeExtensionConfiguration.isLineageEnabled()).thenReturn(false);

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
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService);
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
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService);

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
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            null // null settings service
            );

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
            mockHomePageConfiguration,
            mockFeatureFlags,
            mockChromeExtensionConfiguration,
            mockSettingsService);

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
}
