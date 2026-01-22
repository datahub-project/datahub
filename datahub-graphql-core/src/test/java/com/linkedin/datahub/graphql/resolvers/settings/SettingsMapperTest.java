package com.linkedin.datahub.graphql.resolvers.settings;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AiAssistantSettings;
import com.linkedin.datahub.graphql.generated.AiInstruction;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.AiPluginAuthType;
import com.linkedin.datahub.graphql.generated.AiPluginConfig;
import com.linkedin.datahub.graphql.generated.AiPluginType;
import com.linkedin.datahub.graphql.generated.DocumentationAiSettings;
import com.linkedin.datahub.graphql.generated.GlobalSettings;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiInstructionType;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.OAuthAiPluginConfig;
import io.datahubproject.metadata.services.SecretService;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SettingsMapperTest {

  private SettingsMapper settingsMapper;
  private FeatureFlags featureFlags;
  private SecretService secretService;
  private QueryContext queryContext;

  @BeforeMethod
  public void setup() {
    featureFlags = mock(FeatureFlags.class);
    secretService = mock(SecretService.class);
    queryContext = mock(QueryContext.class);
    settingsMapper = new SettingsMapper(secretService, featureFlags);
  }

  @Test
  public void testMapGlobalSettingsWithAiFeatures() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);
    when(featureFlags.isDocumentationAiDefaultEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Assistant Settings
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    com.linkedin.settings.global.AiInstruction instruction =
        new com.linkedin.settings.global.AiInstruction();
    instruction.setId("test-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(com.linkedin.settings.global.AiInstructionState.ACTIVE);
    instruction.setInstruction("Test AI instruction");

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1695456789000L);
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    instruction.setCreated(auditStamp);
    instruction.setLastModified(auditStamp);

    instructions.add(instruction);
    aiAssistantSettings.setInstructions(instructions);
    globalSettingsInfo.setAiAssistant(aiAssistantSettings);

    // Create Documentation AI Settings
    com.linkedin.settings.global.DocumentationAiSettings docAiSettings =
        new com.linkedin.settings.global.DocumentationAiSettings();
    docAiSettings.setEnabled(true);
    docAiSettings.setInstructions(instructions);
    globalSettingsInfo.setDocumentationAi(docAiSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 1);

    AiInstruction mappedInstruction = mappedAiAssistant.getInstructions().get(0);
    assertEquals(mappedInstruction.getId(), "test-instruction-id");
    assertEquals(
        mappedInstruction.getType(),
        com.linkedin.datahub.graphql.generated.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(mappedInstruction.getState(), AiInstructionState.ACTIVE);
    assertEquals(mappedInstruction.getInstruction(), "Test AI instruction");
    assertNotNull(mappedInstruction.getCreated());
    assertEquals(mappedInstruction.getCreated().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getCreated().getActor(), "urn:li:corpuser:testuser");
    assertNotNull(mappedInstruction.getLastModified());
    assertEquals(mappedInstruction.getLastModified().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getLastModified().getActor(), "urn:li:corpuser:testuser");

    // Verify Documentation AI Settings
    assertNotNull(result.getDocumentationAi());
    DocumentationAiSettings mappedDocAi = result.getDocumentationAi();
    assertTrue(mappedDocAi.getEnabled());
    assertNotNull(mappedDocAi.getInstructions());
    assertEquals(mappedDocAi.getInstructions().size(), 1);

    AiInstruction mappedDocInstruction = mappedDocAi.getInstructions().get(0);
    assertEquals(mappedDocInstruction.getId(), "test-instruction-id");
    assertEquals(mappedDocInstruction.getState(), AiInstructionState.ACTIVE);
    assertEquals(mappedDocInstruction.getInstruction(), "Test AI instruction");
    assertNotNull(mappedDocInstruction.getCreated());
    assertNotNull(mappedDocInstruction.getLastModified());
  }

  @Test
  public void testMapGlobalSettingsWithAiFeaturesDisabled() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(false);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings (should have empty instructions)
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 0);

    // Verify Documentation AI Settings (should be disabled)
    assertNotNull(result.getDocumentationAi());
    DocumentationAiSettings mappedDocAi = result.getDocumentationAi();
    assertFalse(mappedDocAi.getEnabled());
    assertNotNull(mappedDocAi.getInstructions());
    assertEquals(mappedDocAi.getInstructions().size(), 0);
  }

  @Test
  public void testMapGlobalSettingsWithEmptyAiSettings() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);
    when(featureFlags.isDocumentationAiDefaultEnabled()).thenReturn(false);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings (should have empty instructions)
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 0);

    // Verify Documentation AI Settings (should use default enabled flag)
    assertNotNull(result.getDocumentationAi());
    DocumentationAiSettings mappedDocAi = result.getDocumentationAi();
    assertFalse(
        mappedDocAi.getEnabled()); // Should match featureFlags.isDocumentationAiDefaultEnabled()
    assertNotNull(mappedDocAi.getInstructions());
    assertEquals(mappedDocAi.getInstructions().size(), 0);
  }

  @Test
  public void testMapGlobalSettingsWithInactiveInstructions() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);
    when(featureFlags.isDocumentationAiDefaultEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Assistant Settings with INACTIVE instruction
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    com.linkedin.settings.global.AiInstruction instruction =
        new com.linkedin.settings.global.AiInstruction();
    instruction.setId("inactive-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(com.linkedin.settings.global.AiInstructionState.INACTIVE);
    instruction.setInstruction("Inactive AI instruction");

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1695456789000L);
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:testuser"));
    instruction.setCreated(auditStamp);
    instruction.setLastModified(auditStamp);

    instructions.add(instruction);
    aiAssistantSettings.setInstructions(instructions);
    globalSettingsInfo.setAiAssistant(aiAssistantSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Assistant Settings with INACTIVE state
    assertNotNull(result.getAiAssistant());
    AiAssistantSettings mappedAiAssistant = result.getAiAssistant();
    assertNotNull(mappedAiAssistant.getInstructions());
    assertEquals(mappedAiAssistant.getInstructions().size(), 1);

    AiInstruction mappedInstruction = mappedAiAssistant.getInstructions().get(0);
    assertEquals(mappedInstruction.getId(), "inactive-instruction-id");
    assertEquals(
        mappedInstruction.getType(),
        com.linkedin.datahub.graphql.generated.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(mappedInstruction.getState(), AiInstructionState.INACTIVE);
    assertEquals(mappedInstruction.getInstruction(), "Inactive AI instruction");
    assertNotNull(mappedInstruction.getCreated());
    assertNotNull(mappedInstruction.getLastModified());
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // AI Plugins Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testMapGlobalSettingsWithAiPlugins() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add an MCP Server plugin with OAuth auth
    com.linkedin.settings.global.AiPluginConfig plugin1 =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin1.setId("urn:li:service:glean-search");
    plugin1.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin1.setServiceUrn(UrnUtils.getUrn("urn:li:service:glean-search"));
    plugin1.setEnabled(true);
    plugin1.setInstructions("Use Glean for searching company documentation.");
    plugin1.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_OAUTH);

    // Add OAuth config
    OAuthAiPluginConfig oauthConfig = new OAuthAiPluginConfig();
    oauthConfig.setServerUrn(UrnUtils.getUrn("urn:li:oauthAuthorizationServer:glean"));
    plugin1.setOauthConfig(oauthConfig);

    pluginsArray.add(plugin1);

    // Add a second plugin with no auth
    com.linkedin.settings.global.AiPluginConfig plugin2 =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin2.setId("urn:li:service:internal-tools");
    plugin2.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin2.setServiceUrn(UrnUtils.getUrn("urn:li:service:internal-tools"));
    plugin2.setEnabled(false);
    plugin2.setAuthType(com.linkedin.settings.global.AiPluginAuthType.NONE);

    pluginsArray.add(plugin2);

    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins
    assertNotNull(result.getAiPlugins());
    List<AiPluginConfig> mappedPlugins = result.getAiPlugins();
    assertEquals(mappedPlugins.size(), 2);

    // Verify first plugin
    AiPluginConfig mappedPlugin1 = mappedPlugins.get(0);
    assertEquals(mappedPlugin1.getId(), "urn:li:service:glean-search");
    assertEquals(mappedPlugin1.getType(), AiPluginType.MCP_SERVER);
    assertEquals(mappedPlugin1.getServiceUrn(), "urn:li:service:glean-search");
    assertTrue(mappedPlugin1.getEnabled());
    assertEquals(mappedPlugin1.getInstructions(), "Use Glean for searching company documentation.");
    assertEquals(mappedPlugin1.getAuthType(), AiPluginAuthType.USER_OAUTH);
    assertNotNull(mappedPlugin1.getOauthConfig());
    assertEquals(
        mappedPlugin1.getOauthConfig().getServerUrn(), "urn:li:oauthAuthorizationServer:glean");

    // Verify second plugin
    AiPluginConfig mappedPlugin2 = mappedPlugins.get(1);
    assertEquals(mappedPlugin2.getId(), "urn:li:service:internal-tools");
    assertEquals(mappedPlugin2.getType(), AiPluginType.MCP_SERVER);
    assertFalse(mappedPlugin2.getEnabled());
    assertEquals(mappedPlugin2.getAuthType(), AiPluginAuthType.NONE);
    assertNull(mappedPlugin2.getOauthConfig());
  }

  @Test
  public void testMapGlobalSettingsWithEmptyAiPlugins() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create empty AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    pluginSettings.setPlugins(new AiPluginConfigArray());
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins - should be empty list
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 0);
  }

  @Test
  public void testMapGlobalSettingsWithNoAiPluginSettings() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();
    // No aiPluginSettings set

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins - should be empty list
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 0);
  }

  @Test
  public void testMapGlobalSettingsWithSharedApiKeyPlugin() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with shared API key auth
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:weather-api");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:weather-api"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);

    // Add shared API key config with embedded auth injection settings
    com.linkedin.settings.global.SharedApiKeyAiPluginConfig sharedConfig =
        new com.linkedin.settings.global.SharedApiKeyAiPluginConfig();
    sharedConfig.setCredentialUrn(
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_weather-api__apiKey"));
    sharedConfig.setAuthLocation(com.linkedin.settings.global.AuthInjectionLocation.HEADER);
    sharedConfig.setAuthHeaderName("X-API-Key");
    // authScheme is optional - not setting it to test that case
    plugin.setSharedApiKeyConfig(sharedConfig);

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);

    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertEquals(mappedPlugin.getAuthType(), AiPluginAuthType.SHARED_API_KEY);
    assertNotNull(mappedPlugin.getSharedApiKeyConfig());
    assertEquals(
        mappedPlugin.getSharedApiKeyConfig().getCredentialUrn(),
        "urn:li:dataHubConnection:urn_li_service_weather-api__apiKey");
    assertEquals(
        mappedPlugin.getSharedApiKeyConfig().getAuthLocation(),
        com.linkedin.datahub.graphql.generated.AuthInjectionLocation.HEADER);
    assertEquals(mappedPlugin.getSharedApiKeyConfig().getAuthHeaderName(), "X-API-Key");
    assertNull(mappedPlugin.getSharedApiKeyConfig().getAuthScheme());
  }

  @Test
  public void testMapGlobalSettingsWithUserApiKeyPlugin() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with user API key auth
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:user-key-api");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:user-key-api"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_API_KEY);

    // Add user API key config with embedded auth injection settings
    com.linkedin.settings.global.UserApiKeyAiPluginConfig userConfig =
        new com.linkedin.settings.global.UserApiKeyAiPluginConfig();
    userConfig.setAuthLocation(com.linkedin.settings.global.AuthInjectionLocation.QUERY_PARAM);
    userConfig.setAuthQueryParam("api_key");
    userConfig.setAuthHeaderName("Authorization"); // Still set even though location is QUERY_PARAM
    plugin.setUserApiKeyConfig(userConfig);

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);

    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertEquals(mappedPlugin.getAuthType(), AiPluginAuthType.USER_API_KEY);
    assertNotNull(mappedPlugin.getUserApiKeyConfig());
    assertEquals(
        mappedPlugin.getUserApiKeyConfig().getAuthLocation(),
        com.linkedin.datahub.graphql.generated.AuthInjectionLocation.QUERY_PARAM);
    assertEquals(mappedPlugin.getUserApiKeyConfig().getAuthQueryParam(), "api_key");
    assertEquals(mappedPlugin.getUserApiKeyConfig().getAuthHeaderName(), "Authorization");
    assertNull(mappedPlugin.getUserApiKeyConfig().getAuthScheme());
  }

  @Test
  public void testMapGlobalSettingsWithSharedApiKeyDefaultAuthInjection() {
    // Setup - test that default values are used when auth injection settings are not set
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with shared API key auth - minimal config (only credentialUrn)
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:minimal-api");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:minimal-api"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);

    // Add minimal shared API key config - only credential URN
    com.linkedin.settings.global.SharedApiKeyAiPluginConfig sharedConfig =
        new com.linkedin.settings.global.SharedApiKeyAiPluginConfig();
    sharedConfig.setCredentialUrn(
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_minimal-api__apiKey"));
    // Not setting authLocation, authHeaderName - should use defaults
    plugin.setSharedApiKeyConfig(sharedConfig);

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins - should use default values
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);

    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertNotNull(mappedPlugin.getSharedApiKeyConfig());
    // Defaults should be applied
    assertEquals(
        mappedPlugin.getSharedApiKeyConfig().getAuthLocation(),
        com.linkedin.datahub.graphql.generated.AuthInjectionLocation.HEADER);
    assertEquals(mappedPlugin.getSharedApiKeyConfig().getAuthHeaderName(), "Authorization");
  }

  @Test
  public void testMapGlobalSettingsWithOAuthRequiredScopes() {
    // Setup
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with OAuth and required scopes
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:oauth-with-scopes");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:oauth-with-scopes"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_OAUTH);

    // Add OAuth config with required scopes
    OAuthAiPluginConfig oauthConfig = new OAuthAiPluginConfig();
    oauthConfig.setServerUrn(UrnUtils.getUrn("urn:li:oauthAuthorizationServer:scoped"));
    oauthConfig.setRequiredScopes(
        new com.linkedin.data.template.StringArray("read", "write", "admin"));
    plugin.setOauthConfig(oauthConfig);

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);

    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertEquals(mappedPlugin.getAuthType(), AiPluginAuthType.USER_OAUTH);
    assertNotNull(mappedPlugin.getOauthConfig());
    assertNotNull(mappedPlugin.getOauthConfig().getRequiredScopes());
    assertEquals(mappedPlugin.getOauthConfig().getRequiredScopes().size(), 3);
    assertTrue(mappedPlugin.getOauthConfig().getRequiredScopes().contains("read"));
    assertTrue(mappedPlugin.getOauthConfig().getRequiredScopes().contains("write"));
    assertTrue(mappedPlugin.getOauthConfig().getRequiredScopes().contains("admin"));
  }

  @Test
  public void testMapGlobalSettingsWithNullSharedApiKeyConfig() {
    // Setup - plugin has SHARED_API_KEY auth type but sharedApiKeyConfig is null
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with shared API key auth but no config
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:missing-config");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:missing-config"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);
    // Not setting sharedApiKeyConfig - it's null

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute - should not throw
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);
    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertEquals(mappedPlugin.getAuthType(), AiPluginAuthType.SHARED_API_KEY);
    assertNull(mappedPlugin.getSharedApiKeyConfig()); // Should be null, not cause NPE
  }

  @Test
  public void testMapGlobalSettingsWithNullUserApiKeyConfig() {
    // Setup - plugin has USER_API_KEY auth type but userApiKeyConfig is null
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with user API key auth but no config
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:missing-user-config");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:missing-user-config"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_API_KEY);
    // Not setting userApiKeyConfig - it's null

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute - should not throw
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);
    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertEquals(mappedPlugin.getAuthType(), AiPluginAuthType.USER_API_KEY);
    assertNull(mappedPlugin.getUserApiKeyConfig()); // Should be null, not cause NPE
  }

  @Test
  public void testMapGlobalSettingsWithSharedApiKeyScheme() {
    // Setup - test shared API key with auth scheme (e.g., "Bearer")
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with shared API key auth and scheme
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:bearer-api");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:bearer-api"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);

    // Add shared API key config with scheme
    com.linkedin.settings.global.SharedApiKeyAiPluginConfig sharedConfig =
        new com.linkedin.settings.global.SharedApiKeyAiPluginConfig();
    sharedConfig.setCredentialUrn(
        UrnUtils.getUrn("urn:li:dataHubConnection:urn_li_service_bearer-api__apiKey"));
    sharedConfig.setAuthLocation(com.linkedin.settings.global.AuthInjectionLocation.HEADER);
    sharedConfig.setAuthHeaderName("Authorization");
    sharedConfig.setAuthScheme("Bearer");
    plugin.setSharedApiKeyConfig(sharedConfig);

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);

    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertNotNull(mappedPlugin.getSharedApiKeyConfig());
    assertEquals(mappedPlugin.getSharedApiKeyConfig().getAuthScheme(), "Bearer");
  }

  @Test
  public void testMapGlobalSettingsWithUserApiKeyScheme() {
    // Setup - test user API key with auth scheme
    when(featureFlags.isAiFeaturesEnabled()).thenReturn(true);

    GlobalSettingsInfo globalSettingsInfo = new GlobalSettingsInfo();

    // Create AI Plugin Settings
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Add a plugin with user API key auth and scheme
    com.linkedin.settings.global.AiPluginConfig plugin =
        new com.linkedin.settings.global.AiPluginConfig();
    plugin.setId("urn:li:service:user-bearer-api");
    plugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:user-bearer-api"));
    plugin.setEnabled(true);
    plugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.USER_API_KEY);

    // Add user API key config with scheme
    com.linkedin.settings.global.UserApiKeyAiPluginConfig userConfig =
        new com.linkedin.settings.global.UserApiKeyAiPluginConfig();
    userConfig.setAuthLocation(com.linkedin.settings.global.AuthInjectionLocation.HEADER);
    userConfig.setAuthHeaderName("Authorization");
    userConfig.setAuthScheme("ApiKey");
    plugin.setUserApiKeyConfig(userConfig);

    pluginsArray.add(plugin);
    pluginSettings.setPlugins(pluginsArray);
    globalSettingsInfo.setAiPluginSettings(pluginSettings);

    // Mock integrations to prevent NPE
    globalSettingsInfo.setIntegrations(
        new com.linkedin.settings.global.GlobalIntegrationSettings());
    globalSettingsInfo.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    // Execute
    GlobalSettings result = settingsMapper.mapGlobalSettings(queryContext, globalSettingsInfo);

    // Verify AI Plugins
    assertNotNull(result.getAiPlugins());
    assertEquals(result.getAiPlugins().size(), 1);

    AiPluginConfig mappedPlugin = result.getAiPlugins().get(0);
    assertNotNull(mappedPlugin.getUserApiKeyConfig());
    assertEquals(mappedPlugin.getUserApiKeyConfig().getAuthScheme(), "ApiKey");
    assertEquals(
        mappedPlugin.getUserApiKeyConfig().getAuthLocation(),
        com.linkedin.datahub.graphql.generated.AuthInjectionLocation.HEADER);
  }
}
