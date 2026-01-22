package com.linkedin.datahub.graphql.types.corpuser.mappers;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.generated.AiAssistantSettings;
import com.linkedin.datahub.graphql.generated.AiInstruction;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.UserAiPluginConfig;
import com.linkedin.datahub.graphql.generated.UserAiPluginSettings;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.UserAiPluginConfigArray;
import com.linkedin.identity.UserApiKeyConnectionConfig;
import com.linkedin.identity.UserOAuthConnectionConfig;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.CorpUserKey;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiInstructionType;
import java.util.List;
import org.testng.annotations.Test;

public class CorpUserMapperTest {

  @Test
  public void testMapCorpUserWithAiAssistantSettings() {
    // Create entity response with corp user settings containing AI assistant settings
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with AI assistant settings
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Create AI assistant settings
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    com.linkedin.settings.global.AiInstruction instruction =
        new com.linkedin.settings.global.AiInstruction();
    instruction.setId("user-instruction-id");
    instruction.setType(AiInstructionType.GENERAL_CONTEXT);
    instruction.setState(com.linkedin.settings.global.AiInstructionState.ACTIVE);
    instruction.setInstruction("Test AI instruction for user");

    // Set audit stamps
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(1695456789000L);
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:testUser"));
    instruction.setCreated(auditStamp);
    instruction.setLastModified(auditStamp);

    instructions.add(instruction);
    aiAssistantSettings.setInstructions(instructions);
    corpUserSettings.setAiAssistant(aiAssistantSettings);

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify AI assistant settings are mapped correctly
    assertNotNull(result.getSettings());
    assertNotNull(result.getSettings().getAiAssistant());

    AiAssistantSettings mappedAiSettings = result.getSettings().getAiAssistant();
    assertNotNull(mappedAiSettings.getInstructions());
    assertEquals(mappedAiSettings.getInstructions().size(), 1);

    AiInstruction mappedInstruction = mappedAiSettings.getInstructions().get(0);
    assertEquals(mappedInstruction.getId(), "user-instruction-id");
    assertEquals(
        mappedInstruction.getType(),
        com.linkedin.datahub.graphql.generated.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(mappedInstruction.getState(), AiInstructionState.ACTIVE);
    assertEquals(mappedInstruction.getInstruction(), "Test AI instruction for user");
    assertNotNull(mappedInstruction.getCreated());
    assertEquals(mappedInstruction.getCreated().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getCreated().getActor(), "urn:li:corpuser:testUser");
    assertNotNull(mappedInstruction.getLastModified());
    assertEquals(mappedInstruction.getLastModified().getTime(), Long.valueOf(1695456789000L));
    assertEquals(mappedInstruction.getLastModified().getActor(), "urn:li:corpuser:testUser");
  }

  @Test
  public void testMapCorpUserWithoutAiAssistantSettings() {
    // Create entity response with corp user settings but no AI assistant settings
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings without AI assistant settings
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify settings exist but AI assistant settings are null (not set)
    assertNotNull(result.getSettings());
    assertNull(result.getSettings().getAiAssistant());
  }

  @Test
  public void testMapCorpUserWithEmptyAiInstructions() {
    // Create entity response with corp user settings containing AI assistant settings with empty
    // instructions
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with AI assistant settings but empty instructions
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Create AI assistant settings with empty instructions
    com.linkedin.settings.global.AiAssistantSettings aiAssistantSettings =
        new com.linkedin.settings.global.AiAssistantSettings();
    AiInstructionArray instructions = new AiInstructionArray();
    aiAssistantSettings.setInstructions(instructions);
    corpUserSettings.setAiAssistant(aiAssistantSettings);

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify AI assistant settings are mapped with empty instructions
    assertNotNull(result.getSettings());
    assertNotNull(result.getSettings().getAiAssistant());

    AiAssistantSettings mappedAiSettings = result.getSettings().getAiAssistant();
    assertNotNull(mappedAiSettings.getInstructions());
    assertEquals(mappedAiSettings.getInstructions().size(), 0);
  }

  @Test
  public void testMapCorpUserWithAiPluginSettings() {
    // Create entity response with corp user settings containing AI plugin settings
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with AI plugin settings
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    // Create AI plugin settings with one OAuth-connected plugin and one API key plugin
    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    UserAiPluginConfigArray pluginConfigs = new UserAiPluginConfigArray();

    // Add OAuth-connected plugin
    com.linkedin.identity.UserAiPluginConfig oauthPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    oauthPlugin.setId("urn:li:service:github-plugin");
    oauthPlugin.setEnabled(true);
    oauthPlugin.setOauthConfig(
        new UserOAuthConnectionConfig()
            .setConnectionUrn(UrnUtils.getUrn("urn:li:dataHubConnection:github-conn")));
    pluginConfigs.add(oauthPlugin);

    // Add API key plugin
    com.linkedin.identity.UserAiPluginConfig apiKeyPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    apiKeyPlugin.setId("urn:li:service:openai-plugin");
    apiKeyPlugin.setEnabled(false);
    apiKeyPlugin.setApiKeyConfig(
        new UserApiKeyConnectionConfig()
            .setConnectionUrn(UrnUtils.getUrn("urn:li:dataHubConnection:openai-conn")));
    pluginConfigs.add(apiKeyPlugin);

    aiPluginSettings.setPlugins(pluginConfigs);
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);
    assertEquals(result.getUsername(), "testUser");

    // Verify AI plugin settings are mapped correctly
    assertNotNull(result.getSettings());
    assertNotNull(result.getSettings().getAiPluginSettings());

    UserAiPluginSettings mappedPluginSettings = result.getSettings().getAiPluginSettings();
    List<UserAiPluginConfig> plugins = mappedPluginSettings.getPlugins();
    assertNotNull(plugins);
    assertEquals(plugins.size(), 2);

    // Verify OAuth-connected plugin
    UserAiPluginConfig mappedOauthPlugin = plugins.get(0);
    assertEquals(mappedOauthPlugin.getId(), "urn:li:service:github-plugin");
    assertTrue(mappedOauthPlugin.getEnabled());
    assertNotNull(mappedOauthPlugin.getOauthConfig());
    assertEquals(
        mappedOauthPlugin.getOauthConfig().getConnectionUrn(),
        "urn:li:dataHubConnection:github-conn");

    // Verify API key plugin
    UserAiPluginConfig mappedApiKeyPlugin = plugins.get(1);
    assertEquals(mappedApiKeyPlugin.getId(), "urn:li:service:openai-plugin");
    assertFalse(mappedApiKeyPlugin.getEnabled());
    assertNotNull(mappedApiKeyPlugin.getApiKeyConfig());
    assertEquals(
        mappedApiKeyPlugin.getApiKeyConfig().getConnectionUrn(),
        "urn:li:dataHubConnection:openai-conn");
  }

  @Test
  public void testMapCorpUserWithEmptyAiPluginSettings() {
    // Create entity response with corp user settings containing AI plugin settings but no plugins
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with AI plugin settings but empty plugins
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    aiPluginSettings.setPlugins(new UserAiPluginConfigArray());
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    // Set appearance settings to avoid NPE
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    // Add corp user settings to aspects
    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify the mapped corp user
    assertNotNull(result);

    // Verify AI plugin settings are mapped with empty plugins list
    assertNotNull(result.getSettings());
    assertNotNull(result.getSettings().getAiPluginSettings());
    assertNotNull(result.getSettings().getAiPluginSettings().getPlugins());
    assertEquals(result.getSettings().getAiPluginSettings().getPlugins().size(), 0);
  }

  @Test
  public void testMapCorpUserWithAiPluginSettingsAllFields() {
    // Create entity response with corp user settings containing AI plugin with all fields
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    // Create aspects map
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    // Add corp user key
    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    // Create corp user settings with a fully configured plugin
    CorpUserSettings corpUserSettings = new CorpUserSettings();

    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    UserAiPluginConfigArray pluginConfigs = new UserAiPluginConfigArray();

    // Add plugin with all fields: enabled, allowedTools, OAuth config
    com.linkedin.identity.UserAiPluginConfig fullPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    fullPlugin.setId("urn:li:service:full-plugin");
    fullPlugin.setEnabled(true);
    fullPlugin.setAllowedTools(
        new com.linkedin.data.template.StringArray("tool1", "tool2", "tool3"));
    fullPlugin.setOauthConfig(
        new UserOAuthConnectionConfig()
            .setConnectionUrn(UrnUtils.getUrn("urn:li:dataHubConnection:full-oauth-conn")));
    pluginConfigs.add(fullPlugin);

    aiPluginSettings.setPlugins(pluginConfigs);
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    // Set appearance settings
    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    // Map the entity response
    CorpUser result = CorpUserMapper.map(null, entityResponse);

    // Verify all fields are mapped
    UserAiPluginConfig mappedPlugin =
        result.getSettings().getAiPluginSettings().getPlugins().get(0);
    assertEquals(mappedPlugin.getId(), "urn:li:service:full-plugin");
    assertTrue(mappedPlugin.getEnabled());
    assertNotNull(mappedPlugin.getAllowedTools());
    assertEquals(mappedPlugin.getAllowedTools().size(), 3);
    assertTrue(mappedPlugin.getAllowedTools().contains("tool1"));
    assertTrue(mappedPlugin.getAllowedTools().contains("tool2"));
    assertTrue(mappedPlugin.getAllowedTools().contains("tool3"));
    assertNotNull(mappedPlugin.getOauthConfig());
    assertTrue(mappedPlugin.getOauthConfig().getIsConnected());
    assertEquals(
        mappedPlugin.getOauthConfig().getConnectionUrn(),
        "urn:li:dataHubConnection:full-oauth-conn");
  }

  @Test
  public void testMapCorpUserWithDisabledPlugin() {
    // Test a disabled plugin
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    CorpUserSettings corpUserSettings = new CorpUserSettings();

    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    UserAiPluginConfigArray pluginConfigs = new UserAiPluginConfigArray();

    // Add disabled plugin
    com.linkedin.identity.UserAiPluginConfig disabledPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    disabledPlugin.setId("urn:li:service:disabled-plugin");
    disabledPlugin.setEnabled(false);
    pluginConfigs.add(disabledPlugin);

    aiPluginSettings.setPlugins(pluginConfigs);
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    CorpUser result = CorpUserMapper.map(null, entityResponse);

    UserAiPluginConfig mappedPlugin =
        result.getSettings().getAiPluginSettings().getPlugins().get(0);
    assertFalse(mappedPlugin.getEnabled());
  }

  @Test
  public void testMapCorpUserWithPluginWithoutConnectionUrn() {
    // Test plugin configs where connection URN is not set (isConnected should be false)
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    CorpUserSettings corpUserSettings = new CorpUserSettings();

    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    UserAiPluginConfigArray pluginConfigs = new UserAiPluginConfigArray();

    // Plugin with OAuth config but no connection URN set
    com.linkedin.identity.UserAiPluginConfig pluginNoConn =
        new com.linkedin.identity.UserAiPluginConfig();
    pluginNoConn.setId("urn:li:service:no-conn-plugin");
    pluginNoConn.setEnabled(true);
    // Create OAuth config without setting connectionUrn
    pluginNoConn.setOauthConfig(new UserOAuthConnectionConfig());
    pluginConfigs.add(pluginNoConn);

    aiPluginSettings.setPlugins(pluginConfigs);
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    CorpUser result = CorpUserMapper.map(null, entityResponse);

    UserAiPluginConfig mappedPlugin =
        result.getSettings().getAiPluginSettings().getPlugins().get(0);
    assertNotNull(mappedPlugin.getOauthConfig());
    assertFalse(mappedPlugin.getOauthConfig().getIsConnected());
    assertNull(mappedPlugin.getOauthConfig().getConnectionUrn());
  }

  @Test
  public void testMapCorpUserWithMultiplePlugins() {
    // Test multiple plugins with different auth types
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    CorpUserSettings corpUserSettings = new CorpUserSettings();

    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    UserAiPluginConfigArray pluginConfigs = new UserAiPluginConfigArray();

    // Plugin 1: OAuth connected
    com.linkedin.identity.UserAiPluginConfig oauthPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    oauthPlugin.setId("urn:li:service:oauth-plugin");
    oauthPlugin.setEnabled(true);
    oauthPlugin.setOauthConfig(
        new UserOAuthConnectionConfig()
            .setConnectionUrn(UrnUtils.getUrn("urn:li:dataHubConnection:oauth-conn")));
    pluginConfigs.add(oauthPlugin);

    // Plugin 2: API key connected
    com.linkedin.identity.UserAiPluginConfig apiKeyPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    apiKeyPlugin.setId("urn:li:service:apikey-plugin");
    apiKeyPlugin.setEnabled(true);
    apiKeyPlugin.setApiKeyConfig(
        new UserApiKeyConnectionConfig()
            .setConnectionUrn(UrnUtils.getUrn("urn:li:dataHubConnection:apikey-conn")));
    pluginConfigs.add(apiKeyPlugin);

    // Plugin 3: Disabled, no connection
    com.linkedin.identity.UserAiPluginConfig disabledPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    disabledPlugin.setId("urn:li:service:disabled-plugin");
    disabledPlugin.setEnabled(false);
    pluginConfigs.add(disabledPlugin);

    aiPluginSettings.setPlugins(pluginConfigs);
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    CorpUser result = CorpUserMapper.map(null, entityResponse);

    List<UserAiPluginConfig> plugins = result.getSettings().getAiPluginSettings().getPlugins();
    assertEquals(plugins.size(), 3);

    // Verify OAuth plugin
    UserAiPluginConfig mappedOauth = plugins.get(0);
    assertEquals(mappedOauth.getId(), "urn:li:service:oauth-plugin");
    assertTrue(mappedOauth.getEnabled());
    assertNotNull(mappedOauth.getOauthConfig());
    assertTrue(mappedOauth.getOauthConfig().getIsConnected());
    assertNull(mappedOauth.getApiKeyConfig());

    // Verify API key plugin
    UserAiPluginConfig mappedApiKey = plugins.get(1);
    assertEquals(mappedApiKey.getId(), "urn:li:service:apikey-plugin");
    assertTrue(mappedApiKey.getEnabled());
    assertNotNull(mappedApiKey.getApiKeyConfig());
    assertTrue(mappedApiKey.getApiKeyConfig().getIsConnected());
    assertNull(mappedApiKey.getOauthConfig());

    // Verify disabled plugin
    UserAiPluginConfig mappedDisabled = plugins.get(2);
    assertEquals(mappedDisabled.getId(), "urn:li:service:disabled-plugin");
    assertFalse(mappedDisabled.getEnabled());
    assertNull(mappedDisabled.getOauthConfig());
    assertNull(mappedDisabled.getApiKeyConfig());
  }

  @Test
  public void testMapCorpUserWithPluginMinimalFields() {
    // Test plugin with only required fields (id)
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(UrnUtils.getUrn("urn:li:corpuser:testUser"));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();

    CorpUserKey corpUserKey = new CorpUserKey();
    corpUserKey.setUsername("testUser");
    EnvelopedAspect keyAspect = new EnvelopedAspect();
    keyAspect.setValue(new Aspect(corpUserKey.data()));
    aspectMap.put(Constants.CORP_USER_KEY_ASPECT_NAME, keyAspect);

    CorpUserSettings corpUserSettings = new CorpUserSettings();

    com.linkedin.identity.UserAiPluginSettings aiPluginSettings =
        new com.linkedin.identity.UserAiPluginSettings();
    UserAiPluginConfigArray pluginConfigs = new UserAiPluginConfigArray();

    // Minimal plugin - only ID set
    com.linkedin.identity.UserAiPluginConfig minimalPlugin =
        new com.linkedin.identity.UserAiPluginConfig();
    minimalPlugin.setId("urn:li:service:minimal-plugin");
    // Don't set enabled, allowedTools, apiKeyConfig, or oauthConfig
    pluginConfigs.add(minimalPlugin);

    aiPluginSettings.setPlugins(pluginConfigs);
    corpUserSettings.setAiPluginSettings(aiPluginSettings);

    com.linkedin.identity.CorpUserAppearanceSettings appearanceSettings =
        new com.linkedin.identity.CorpUserAppearanceSettings();
    appearanceSettings.setShowSimplifiedHomepage(false);
    corpUserSettings.setAppearance(appearanceSettings);

    EnvelopedAspect settingsAspect = new EnvelopedAspect();
    settingsAspect.setValue(new Aspect(corpUserSettings.data()));
    aspectMap.put(Constants.CORP_USER_SETTINGS_ASPECT_NAME, settingsAspect);

    entityResponse.setAspects(aspectMap);

    CorpUser result = CorpUserMapper.map(null, entityResponse);

    UserAiPluginConfig mappedPlugin =
        result.getSettings().getAiPluginSettings().getPlugins().get(0);
    assertEquals(mappedPlugin.getId(), "urn:li:service:minimal-plugin");
    // Optional fields should be null or not set
    assertNull(mappedPlugin.getEnabled());
    assertNull(mappedPlugin.getAllowedTools());
    assertNull(mappedPlugin.getOauthConfig());
    assertNull(mappedPlugin.getApiKeyConfig());
  }
}
