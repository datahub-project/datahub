package com.linkedin.datahub.graphql.resolvers.settings;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AiInstructionInput;
import com.linkedin.datahub.graphql.generated.AiInstructionState;
import com.linkedin.datahub.graphql.generated.AiInstructionType;
import com.linkedin.datahub.graphql.generated.AiPluginAuthType;
import com.linkedin.datahub.graphql.generated.AiPluginConfigInput;
import com.linkedin.datahub.graphql.generated.AiPluginType;
import com.linkedin.datahub.graphql.generated.UpdateAiAssistantSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateDocumentationAiSettingsInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.integration.IntegrationsService;
import com.linkedin.settings.global.AiAssistantSettings;
import com.linkedin.settings.global.AiInstruction;
import com.linkedin.settings.global.AiInstructionArray;
import com.linkedin.settings.global.AiPluginConfig;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.DocumentationAiSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.SecretService;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateGlobalSettingsResolverTest {

  private UpdateGlobalSettingsResolver resolver;
  private EntityClient entityClient;
  private SecretService secretService;
  private IntegrationsService integrationsService;
  private QueryContext queryContext;
  private OperationContext operationContext;
  private DataFetchingEnvironment environment;

  @BeforeMethod
  public void setup() {
    entityClient = mock(EntityClient.class);
    secretService = mock(SecretService.class);
    integrationsService = mock(IntegrationsService.class);
    queryContext = mock(QueryContext.class);
    operationContext = mock(OperationContext.class);
    environment = mock(DataFetchingEnvironment.class);

    resolver = new UpdateGlobalSettingsResolver(entityClient, secretService, integrationsService);
  }

  @Test
  public void testConstructorNullChecks() {
    // Test null entityClient
    assertThrows(
        NullPointerException.class,
        () -> new UpdateGlobalSettingsResolver(null, secretService, integrationsService));

    // Test null secretService
    assertThrows(
        NullPointerException.class,
        () -> new UpdateGlobalSettingsResolver(entityClient, null, integrationsService));

    // Test null integrationsService
    assertThrows(
        NullPointerException.class,
        () -> new UpdateGlobalSettingsResolver(entityClient, secretService, null));
  }

  // Note: Skipping integration test due to static method dependencies
  // Authorization tests are covered by other test classes

  // Note: Skipping integration test due to static method dependencies
  // Exception handling tests are covered by private method tests below

  @Test
  public void testUpdateDocumentationAiSettings() throws Exception {
    // Setup
    DocumentationAiSettings existingSettings = new DocumentationAiSettings();
    existingSettings.setEnabled(false);

    UpdateDocumentationAiSettingsInput input = new UpdateDocumentationAiSettingsInput();
    input.setEnabled(true);

    AiInstructionInput instructionInput = new AiInstructionInput();
    instructionInput.setId("custom-id");
    instructionInput.setType(AiInstructionType.GENERAL_CONTEXT);
    instructionInput.setState(AiInstructionState.ACTIVE);
    instructionInput.setInstruction("Custom instruction");
    input.setInstructions(Arrays.asList(instructionInput));

    String actorUrn = "urn:li:corpuser:admin";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method updateDocAiMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateDocumentationAiSettings",
            DocumentationAiSettings.class,
            UpdateDocumentationAiSettingsInput.class,
            QueryContext.class);
    updateDocAiMethod.setAccessible(true);

    // Execute
    updateDocAiMethod.invoke(resolver, existingSettings, input, queryContext);

    // Verify
    assertTrue(existingSettings.isEnabled());
    assertEquals(existingSettings.getInstructions().size(), 1);
    AiInstruction instruction = existingSettings.getInstructions().get(0);
    assertEquals(instruction.getId(), "custom-id");
    assertEquals(
        instruction.getType(), com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(instruction.getState(), com.linkedin.settings.global.AiInstructionState.ACTIVE);
    assertEquals(instruction.getInstruction(), "Custom instruction");
    assertNotNull(instruction.getCreated());
    assertNotNull(instruction.getLastModified());
  }

  @Test
  public void testUpdateAiAssistantSettings() throws Exception {
    // Setup
    AiAssistantSettings existingSettings = new AiAssistantSettings();

    UpdateAiAssistantSettingsInput input = new UpdateAiAssistantSettingsInput();

    AiInstructionInput instructionInput = new AiInstructionInput();
    instructionInput.setInstruction("Assistant instruction");
    instructionInput.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstructions(Arrays.asList(instructionInput));

    String actorUrn = "urn:li:corpuser:admin";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method updateAiAssistantMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateAiAssistantSettings",
            AiAssistantSettings.class,
            UpdateAiAssistantSettingsInput.class,
            QueryContext.class);
    updateAiAssistantMethod.setAccessible(true);

    // Execute
    updateAiAssistantMethod.invoke(resolver, existingSettings, input, queryContext);

    // Verify
    assertEquals(existingSettings.getInstructions().size(), 1);
    AiInstruction instruction = existingSettings.getInstructions().get(0);
    assertNotNull(instruction.getId()); // Should be auto-generated UUID
    assertNotNull(instruction.getType());
    assertEquals(instruction.getState(), com.linkedin.settings.global.AiInstructionState.ACTIVE);
    assertEquals(instruction.getInstruction(), "Assistant instruction");
    assertNotNull(instruction.getCreated());
    assertNotNull(instruction.getLastModified());
  }

  @Test
  public void testMapAiInstructionInputsWithProvidedId() throws Exception {
    // Setup
    AiInstructionInput input = new AiInstructionInput();
    input.setId("custom-instruction-id");
    input.setState(AiInstructionState.INACTIVE);
    input.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstruction("Test instruction");

    String actorUrn = "urn:li:corpuser:testuser";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiInstructionInputs", List.class, QueryContext.class);
    mapMethod.setAccessible(true);

    // Execute
    AiInstructionArray result =
        (AiInstructionArray) mapMethod.invoke(resolver, Arrays.asList(input), queryContext);

    // Verify
    assertEquals(result.size(), 1);
    AiInstruction instruction = result.get(0);
    assertEquals(instruction.getId(), "custom-instruction-id");
    assertEquals(
        instruction.getType(), com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(instruction.getState(), com.linkedin.settings.global.AiInstructionState.INACTIVE);
    assertEquals(instruction.getInstruction(), "Test instruction");
    assertNotNull(instruction.getCreated());
    assertEquals(instruction.getCreated().getActor(), UrnUtils.getUrn(actorUrn));
    assertNotNull(instruction.getLastModified());
    assertEquals(instruction.getLastModified().getActor(), UrnUtils.getUrn(actorUrn));
  }

  @Test
  public void testMapAiInstructionInputsWithGeneratedId() throws Exception {
    // Setup
    AiInstructionInput input = new AiInstructionInput();
    // No ID provided - should generate UUID
    input.setInstruction("Test instruction without ID");
    input.setType(AiInstructionType.GENERAL_CONTEXT);

    String actorUrn = "urn:li:corpuser:testuser";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiInstructionInputs", List.class, QueryContext.class);
    mapMethod.setAccessible(true);

    // Execute
    AiInstructionArray result =
        (AiInstructionArray) mapMethod.invoke(resolver, Arrays.asList(input), queryContext);

    // Verify
    assertEquals(result.size(), 1);
    AiInstruction instruction = result.get(0);
    assertNotNull(instruction.getId()); // Should be auto-generated UUID
    assertTrue(instruction.getId().length() > 0);
    assertEquals(
        instruction.getState(),
        com.linkedin.settings.global.AiInstructionState.ACTIVE); // Default state
    assertEquals(instruction.getInstruction(), "Test instruction without ID");
  }

  @Test
  public void testMapGraphqlStateToPdlStateActive() throws Exception {
    // Use reflection to test the private method
    Method mapStateMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapGraphqlStateToPdlState",
            com.linkedin.datahub.graphql.generated.AiInstructionState.class);
    mapStateMethod.setAccessible(true);

    // Execute
    com.linkedin.settings.global.AiInstructionState result =
        (com.linkedin.settings.global.AiInstructionState)
            mapStateMethod.invoke(resolver, AiInstructionState.ACTIVE);

    // Verify
    assertEquals(result, com.linkedin.settings.global.AiInstructionState.ACTIVE);
  }

  @Test
  public void testMapGraphqlStateToPdlStateInactive() throws Exception {
    // Use reflection to test the private method
    Method mapStateMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapGraphqlStateToPdlState",
            com.linkedin.datahub.graphql.generated.AiInstructionState.class);
    mapStateMethod.setAccessible(true);

    // Execute
    com.linkedin.settings.global.AiInstructionState result =
        (com.linkedin.settings.global.AiInstructionState)
            mapStateMethod.invoke(resolver, AiInstructionState.INACTIVE);

    // Verify
    assertEquals(result, com.linkedin.settings.global.AiInstructionState.INACTIVE);
  }

  @Test
  public void testInstructionsReplaceNotAppend() throws Exception {
    // Setup existing settings with instructions
    DocumentationAiSettings existingSettings = new DocumentationAiSettings();
    AiInstructionArray existingInstructions = new AiInstructionArray();

    AiInstruction existingInstruction = new AiInstruction();
    existingInstruction.setId("existing-id");
    existingInstruction.setType(com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    existingInstruction.setState(com.linkedin.settings.global.AiInstructionState.ACTIVE);
    existingInstruction.setInstruction("Existing instruction");
    existingInstructions.add(existingInstruction);

    existingSettings.setInstructions(existingInstructions);

    // Create new input with different instructions
    UpdateDocumentationAiSettingsInput input = new UpdateDocumentationAiSettingsInput();

    AiInstructionInput newInstructionInput = new AiInstructionInput();
    newInstructionInput.setId("new-id");
    newInstructionInput.setInstruction("New instruction");
    newInstructionInput.setType(AiInstructionType.GENERAL_CONTEXT);
    input.setInstructions(Arrays.asList(newInstructionInput));

    String actorUrn = "urn:li:corpuser:admin";
    when(queryContext.getActorUrn()).thenReturn(actorUrn);

    // Use reflection to test the private method
    Method updateDocAiMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateDocumentationAiSettings",
            DocumentationAiSettings.class,
            UpdateDocumentationAiSettingsInput.class,
            QueryContext.class);
    updateDocAiMethod.setAccessible(true);

    // Execute
    updateDocAiMethod.invoke(resolver, existingSettings, input, queryContext);

    // Verify instructions were replaced, not appended
    assertEquals(existingSettings.getInstructions().size(), 1);
    assertEquals(existingSettings.getInstructions().get(0).getId(), "new-id");
    assertEquals(
        existingSettings.getInstructions().get(0).getType(),
        com.linkedin.settings.global.AiInstructionType.GENERAL_CONTEXT);
    assertEquals(existingSettings.getInstructions().get(0).getInstruction(), "New instruction");
    // Old instruction should be gone
    assertFalse(
        existingSettings.getInstructions().stream()
            .anyMatch(inst -> "existing-id".equals(inst.getId())));
  }

  /**
   * Test that updateAiPlugins merges updates with existing plugins by ID, rather than replacing the
   * entire list. Per GraphQL schema: "Plugins not included will remain unchanged."
   */
  @Test
  public void testUpdateAiPluginsMergesByIdNotReplace() throws Exception {
    // Setup existing settings with two plugins
    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray existingPlugins = new AiPluginConfigArray();

    // Plugin A - will be updated
    AiPluginConfig pluginA = new AiPluginConfig();
    pluginA.setId("plugin-a");
    pluginA.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    pluginA.setServiceUrn(UrnUtils.getUrn("urn:li:service:service-a"));
    pluginA.setEnabled(true);
    pluginA.setInstructions("Original instructions for A");
    pluginA.setAuthType(com.linkedin.settings.global.AiPluginAuthType.NONE);
    existingPlugins.add(pluginA);

    // Plugin B - should remain unchanged (not in update)
    AiPluginConfig pluginB = new AiPluginConfig();
    pluginB.setId("plugin-b");
    pluginB.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    pluginB.setServiceUrn(UrnUtils.getUrn("urn:li:service:service-b"));
    pluginB.setEnabled(true);
    pluginB.setInstructions("Instructions for B");
    pluginB.setAuthType(com.linkedin.settings.global.AiPluginAuthType.NONE);
    existingPlugins.add(pluginB);

    pluginSettings.setPlugins(existingPlugins);
    existingSettings.setAiPluginSettings(pluginSettings);

    // Create update input - only updating plugin A and adding plugin C
    AiPluginConfigInput updateA = new AiPluginConfigInput();
    updateA.setId("plugin-a");
    updateA.setType(AiPluginType.MCP_SERVER);
    updateA.setServiceUrn("urn:li:service:service-a");
    updateA.setEnabled(false); // Changed from true to false
    updateA.setInstructions("Updated instructions for A"); // Changed
    updateA.setAuthType(AiPluginAuthType.NONE);

    AiPluginConfigInput newPluginC = new AiPluginConfigInput();
    newPluginC.setId("plugin-c");
    newPluginC.setType(AiPluginType.MCP_SERVER);
    newPluginC.setServiceUrn("urn:li:service:service-c");
    newPluginC.setEnabled(true);
    newPluginC.setInstructions("New plugin C");
    newPluginC.setAuthType(AiPluginAuthType.NONE);

    List<AiPluginConfigInput> updates = Arrays.asList(updateA, newPluginC);

    // Use reflection to test the private method
    Method updateAiPluginsMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateAiPlugins", GlobalSettingsInfo.class, List.class);
    updateAiPluginsMethod.setAccessible(true);

    // Execute
    updateAiPluginsMethod.invoke(resolver, existingSettings, updates);

    // Verify - should have 3 plugins (A updated, B preserved, C added)
    AiPluginConfigArray resultPlugins = existingSettings.getAiPluginSettings().getPlugins();
    assertEquals(resultPlugins.size(), 3, "Should have 3 plugins after merge");

    // Find each plugin by ID
    AiPluginConfig resultA =
        resultPlugins.stream().filter(p -> "plugin-a".equals(p.getId())).findFirst().orElse(null);
    AiPluginConfig resultB =
        resultPlugins.stream().filter(p -> "plugin-b".equals(p.getId())).findFirst().orElse(null);
    AiPluginConfig resultC =
        resultPlugins.stream().filter(p -> "plugin-c".equals(p.getId())).findFirst().orElse(null);

    // Verify plugin A was updated
    assertNotNull(resultA, "Plugin A should exist");
    assertFalse(resultA.isEnabled(), "Plugin A should be disabled after update");
    assertEquals(resultA.getInstructions(), "Updated instructions for A");

    // Verify plugin B was preserved (not in update input)
    assertNotNull(resultB, "Plugin B should still exist (not in update)");
    assertTrue(resultB.isEnabled(), "Plugin B should still be enabled");
    assertEquals(resultB.getInstructions(), "Instructions for B");

    // Verify plugin C was added
    assertNotNull(resultC, "Plugin C should be added");
    assertTrue(resultC.isEnabled());
    assertEquals(resultC.getInstructions(), "New plugin C");
  }

  /** Test that updateAiPlugins works correctly when there are no existing plugins. */
  @Test
  public void testUpdateAiPluginsWithNoExistingPlugins() throws Exception {
    // Setup empty existing settings
    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    // No aiPluginSettings set

    // Create update input with one new plugin
    AiPluginConfigInput newPlugin = new AiPluginConfigInput();
    newPlugin.setId("new-plugin");
    newPlugin.setType(AiPluginType.MCP_SERVER);
    newPlugin.setServiceUrn("urn:li:service:new-service");
    newPlugin.setEnabled(true);
    newPlugin.setAuthType(AiPluginAuthType.NONE);

    List<AiPluginConfigInput> updates = Arrays.asList(newPlugin);

    // Use reflection to test the private method
    Method updateAiPluginsMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateAiPlugins", GlobalSettingsInfo.class, List.class);
    updateAiPluginsMethod.setAccessible(true);

    // Execute
    updateAiPluginsMethod.invoke(resolver, existingSettings, updates);

    // Verify
    assertTrue(existingSettings.hasAiPluginSettings());
    assertEquals(existingSettings.getAiPluginSettings().getPlugins().size(), 1);
    assertEquals(existingSettings.getAiPluginSettings().getPlugins().get(0).getId(), "new-plugin");
  }

  /** Test that updateAiPlugins with empty update list preserves all existing plugins. */
  @Test
  public void testUpdateAiPluginsWithEmptyUpdatePreservesExisting() throws Exception {
    // Setup existing settings with one plugin
    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray existingPlugins = new AiPluginConfigArray();

    AiPluginConfig existingPlugin = new AiPluginConfig();
    existingPlugin.setId("existing-plugin");
    existingPlugin.setType(com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    existingPlugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:existing"));
    existingPlugin.setEnabled(true);
    existingPlugin.setAuthType(com.linkedin.settings.global.AiPluginAuthType.NONE);
    existingPlugins.add(existingPlugin);

    pluginSettings.setPlugins(existingPlugins);
    existingSettings.setAiPluginSettings(pluginSettings);

    // Empty update list
    List<AiPluginConfigInput> updates = Arrays.asList();

    // Use reflection to test the private method
    Method updateAiPluginsMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateAiPlugins", GlobalSettingsInfo.class, List.class);
    updateAiPluginsMethod.setAccessible(true);

    // Execute
    updateAiPluginsMethod.invoke(resolver, existingSettings, updates);

    // Verify - existing plugin should be preserved
    assertEquals(
        existingSettings.getAiPluginSettings().getPlugins().size(),
        1,
        "Existing plugin should be preserved with empty update");
    assertEquals(
        existingSettings.getAiPluginSettings().getPlugins().get(0).getId(), "existing-plugin");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Integration Settings Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testUpdateSlackIntegrationSettings() throws Exception {
    // Setup
    com.linkedin.settings.global.SlackIntegrationSettings existingSettings =
        new com.linkedin.settings.global.SlackIntegrationSettings();
    existingSettings.setEnabled(false);

    com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput();
    input.setDefaultChannelName("#datahub-alerts");
    input.setBotToken("xoxb-test-token");
    input.setDatahubAtMentionEnabled(true);

    when(secretService.encrypt("xoxb-test-token")).thenReturn("encrypted-token");

    // Use reflection to test the private method
    Method updateSlackMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateSlackIntegrationSettings",
            com.linkedin.settings.global.SlackIntegrationSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput.class);
    updateSlackMethod.setAccessible(true);

    // Execute
    updateSlackMethod.invoke(resolver, existingSettings, input);

    // Verify
    assertTrue(existingSettings.isEnabled());
    assertEquals(existingSettings.getDefaultChannelName(), "#datahub-alerts");
    assertEquals(existingSettings.getEncryptedBotToken(), "encrypted-token");
    assertTrue(existingSettings.isDatahubAtMentionEnabled());
    verify(integrationsService).reloadCredentials();
  }

  @Test
  public void testUpdateSlackIntegrationSettingsNoReloadWhenNoChanges() throws Exception {
    // Setup - existing settings match the update
    com.linkedin.settings.global.SlackIntegrationSettings existingSettings =
        new com.linkedin.settings.global.SlackIntegrationSettings();
    existingSettings.setEnabled(true);
    existingSettings.setDatahubAtMentionEnabled(true);

    com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput();
    input.setDatahubAtMentionEnabled(true); // Same as existing

    // Use reflection to test the private method
    Method updateSlackMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateSlackIntegrationSettings",
            com.linkedin.settings.global.SlackIntegrationSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateSlackIntegrationSettingsInput.class);
    updateSlackMethod.setAccessible(true);

    // Execute
    updateSlackMethod.invoke(resolver, existingSettings, input);

    // Verify - no reload because datahubAtMentionEnabled didn't change
    verify(integrationsService, never()).reloadCredentials();
  }

  @Test
  public void testUpdateEmailIntegrationSettings() throws Exception {
    // Setup
    com.linkedin.settings.global.EmailIntegrationSettings existingSettings =
        new com.linkedin.settings.global.EmailIntegrationSettings();

    com.linkedin.datahub.graphql.generated.UpdateEmailIntegrationSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateEmailIntegrationSettingsInput();
    input.setDefaultEmail("alerts@company.com");

    // Use reflection to test the private method
    Method updateEmailMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateEmailIntegrationSettings",
            com.linkedin.settings.global.EmailIntegrationSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateEmailIntegrationSettingsInput.class);
    updateEmailMethod.setAccessible(true);

    // Execute
    updateEmailMethod.invoke(resolver, existingSettings, input);

    // Verify
    assertEquals(existingSettings.getDefaultEmail(), "alerts@company.com");
  }

  @Test
  public void testUpdateTeamsIntegrationSettings() throws Exception {
    // Setup
    com.linkedin.settings.global.TeamsIntegrationSettings existingSettings =
        new com.linkedin.settings.global.TeamsIntegrationSettings();

    com.linkedin.datahub.graphql.generated.UpdateTeamsIntegrationSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateTeamsIntegrationSettingsInput();
    com.linkedin.datahub.graphql.generated.TeamsChannelInput channelInput =
        new com.linkedin.datahub.graphql.generated.TeamsChannelInput();
    channelInput.setId("channel-123");
    channelInput.setName("DataHub Alerts");
    input.setDefaultChannel(channelInput);

    // Use reflection to test the private method
    Method updateTeamsMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateTeamsIntegrationSettings",
            com.linkedin.settings.global.TeamsIntegrationSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateTeamsIntegrationSettingsInput.class);
    updateTeamsMethod.setAccessible(true);

    // Execute
    updateTeamsMethod.invoke(resolver, existingSettings, input);

    // Verify
    assertTrue(existingSettings.isEnabled());
    assertNotNull(existingSettings.getDefaultChannel());
    assertEquals(existingSettings.getDefaultChannel().getId(), "channel-123");
    assertEquals(existingSettings.getDefaultChannel().getName(), "DataHub Alerts");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Notification Settings Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testUpdateGlobalNotificationSettingsNewSettings() throws Exception {
    // Setup
    com.linkedin.settings.global.GlobalNotificationSettings existingSettings =
        new com.linkedin.settings.global.GlobalNotificationSettings();

    com.linkedin.datahub.graphql.generated.NotificationSettingInput settingInput =
        new com.linkedin.datahub.graphql.generated.NotificationSettingInput();
    settingInput.setType(
        com.linkedin.datahub.graphql.generated.NotificationScenarioType.PROPOSAL_STATUS_CHANGE);
    settingInput.setValue(com.linkedin.datahub.graphql.generated.NotificationSettingValue.ENABLED);

    com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput();
    input.setSettings(Arrays.asList(settingInput));

    // Use reflection to test the private method
    Method updateNotifMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateGlobalNotificationSettings",
            com.linkedin.settings.global.GlobalNotificationSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateGlobalNotificationSettingsInput.class);
    updateNotifMethod.setAccessible(true);

    // Execute
    updateNotifMethod.invoke(resolver, existingSettings, input);

    // Verify
    assertTrue(existingSettings.hasSettings());
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // SSO/OIDC Settings Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testUpdateOidcSettingsFull() throws Exception {
    // Setup
    com.linkedin.settings.global.OidcSettings existingSettings =
        new com.linkedin.settings.global.OidcSettings();

    com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput();
    input.setEnabled(true);
    input.setClientId("client-123");
    input.setClientSecret("secret-456");
    input.setDiscoveryUri("https://auth.example.com/.well-known/openid-configuration");
    input.setUserNameClaim("email");
    input.setUserNameClaimRegex("(.*)@company.com");
    input.setScope("openid profile email");
    input.setClientAuthenticationMethod("client_secret_post");
    input.setJitProvisioningEnabled(true);
    input.setPreProvisioningRequired(false);
    input.setExtractGroupsEnabled(true);
    input.setGroupsClaim("groups");
    input.setResponseType("code");
    input.setResponseMode("query");
    input.setUseNonce(true);
    input.setReadTimeout(30000L);
    input.setExtractJwtAccessTokenClaims(true);
    input.setPreferredJwsAlgorithm("RS256");

    when(secretService.encrypt("secret-456")).thenReturn("encrypted-secret");

    // Use reflection to test the private method
    Method updateOidcMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateOidcSettings",
            com.linkedin.settings.global.OidcSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateOidcSettingsInput.class);
    updateOidcMethod.setAccessible(true);

    // Execute
    updateOidcMethod.invoke(resolver, existingSettings, input);

    // Verify
    assertTrue(existingSettings.isEnabled());
    assertEquals(existingSettings.getClientId(), "client-123");
    assertEquals(existingSettings.getClientSecret(), "encrypted-secret");
    assertEquals(
        existingSettings.getDiscoveryUri(),
        "https://auth.example.com/.well-known/openid-configuration");
    assertEquals(existingSettings.getUserNameClaim(), "email");
    assertEquals(existingSettings.getUserNameClaimRegex(), "(.*)@company.com");
    assertEquals(existingSettings.getScope(), "openid profile email");
    assertEquals(existingSettings.getClientAuthenticationMethod(), "client_secret_post");
    assertTrue(existingSettings.isJitProvisioningEnabled());
    assertFalse(existingSettings.isPreProvisioningRequired());
    assertTrue(existingSettings.isExtractGroupsEnabled());
    assertEquals(existingSettings.getGroupsClaim(), "groups");
    assertEquals(existingSettings.getResponseType(), "code");
    assertEquals(existingSettings.getResponseMode(), "query");
    assertTrue(existingSettings.isUseNonce());
    assertEquals(existingSettings.getReadTimeout().longValue(), 30000L);
    assertTrue(existingSettings.isExtractJwtAccessTokenClaims());
    assertEquals(existingSettings.getPreferredJwsAlgorithm2(), "RS256");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // AI Plugin Config Mapping Tests
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testMapAiPluginConfigInputWithOAuthConfig() throws Exception {
    // Setup
    AiPluginConfigInput input = new AiPluginConfigInput();
    input.setId("oauth-plugin");
    input.setType(AiPluginType.MCP_SERVER);
    input.setServiceUrn("urn:li:service:oauth-service");
    input.setEnabled(true);
    input.setInstructions("OAuth plugin instructions");
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    com.linkedin.datahub.graphql.generated.OAuthAiPluginConfigInput oauthInput =
        new com.linkedin.datahub.graphql.generated.OAuthAiPluginConfigInput();
    oauthInput.setServerUrn("urn:li:oauthAuthorizationServer:glean");
    oauthInput.setRequiredScopes(Arrays.asList("read", "write"));
    input.setOauthConfig(oauthInput);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiPluginConfigInput", AiPluginConfigInput.class);
    mapMethod.setAccessible(true);

    // Execute
    AiPluginConfig result = (AiPluginConfig) mapMethod.invoke(resolver, input);

    // Verify
    assertEquals(result.getId(), "oauth-plugin");
    assertEquals(result.getType(), com.linkedin.settings.global.AiPluginType.MCP_SERVER);
    assertEquals(result.getServiceUrn().toString(), "urn:li:service:oauth-service");
    assertTrue(result.isEnabled());
    assertEquals(result.getInstructions(), "OAuth plugin instructions");
    assertEquals(result.getAuthType(), com.linkedin.settings.global.AiPluginAuthType.USER_OAUTH);
    assertNotNull(result.getOauthConfig());
    assertEquals(
        result.getOauthConfig().getServerUrn().toString(), "urn:li:oauthAuthorizationServer:glean");
    assertEquals(result.getOauthConfig().getRequiredScopes().size(), 2);
    assertTrue(result.getOauthConfig().getRequiredScopes().contains("read"));
    assertTrue(result.getOauthConfig().getRequiredScopes().contains("write"));
  }

  @Test
  public void testMapAiPluginConfigInputWithSharedApiKeyConfig() throws Exception {
    // Setup
    AiPluginConfigInput input = new AiPluginConfigInput();
    input.setId("shared-api-key-plugin");
    input.setType(AiPluginType.MCP_SERVER);
    input.setServiceUrn("urn:li:service:api-key-service");
    input.setEnabled(true);
    input.setAuthType(AiPluginAuthType.SHARED_API_KEY);

    com.linkedin.datahub.graphql.generated.SharedApiKeyAiPluginConfigInput sharedInput =
        new com.linkedin.datahub.graphql.generated.SharedApiKeyAiPluginConfigInput();
    sharedInput.setCredentialUrn(
        "urn:li:dataHubConnection:(urn:li:service:api-key-service,apiKey)");
    sharedInput.setAuthLocation(
        com.linkedin.datahub.graphql.generated.AuthInjectionLocation.HEADER);
    sharedInput.setAuthHeaderName("X-API-Key");
    sharedInput.setAuthScheme("ApiKey");
    input.setSharedApiKeyConfig(sharedInput);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiPluginConfigInput", AiPluginConfigInput.class);
    mapMethod.setAccessible(true);

    // Execute
    AiPluginConfig result = (AiPluginConfig) mapMethod.invoke(resolver, input);

    // Verify
    assertEquals(result.getId(), "shared-api-key-plugin");
    assertEquals(
        result.getAuthType(), com.linkedin.settings.global.AiPluginAuthType.SHARED_API_KEY);
    assertNotNull(result.getSharedApiKeyConfig());
    assertEquals(
        result.getSharedApiKeyConfig().getCredentialUrn().toString(),
        "urn:li:dataHubConnection:(urn:li:service:api-key-service,apiKey)");
    assertEquals(
        result.getSharedApiKeyConfig().getAuthLocation(),
        com.linkedin.settings.global.AuthInjectionLocation.HEADER);
    assertEquals(result.getSharedApiKeyConfig().getAuthHeaderName(), "X-API-Key");
    assertEquals(result.getSharedApiKeyConfig().getAuthScheme(), "ApiKey");
  }

  @Test
  public void testMapAiPluginConfigInputWithDefaultEnabled() throws Exception {
    // Setup - enabled is null, should default to true
    AiPluginConfigInput input = new AiPluginConfigInput();
    input.setId("default-enabled-plugin");
    input.setType(AiPluginType.MCP_SERVER);
    input.setServiceUrn("urn:li:service:test");
    input.setEnabled(null); // Should default to true
    input.setAuthType(AiPluginAuthType.NONE);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiPluginConfigInput", AiPluginConfigInput.class);
    mapMethod.setAccessible(true);

    // Execute
    AiPluginConfig result = (AiPluginConfig) mapMethod.invoke(resolver, input);

    // Verify - should default to true
    assertTrue(result.isEnabled(), "enabled should default to true when input is null");
  }

  @Test
  public void testMapAiPluginConfigInputWithOAuthConfigNoScopes() throws Exception {
    // Setup - OAuth config without required scopes
    AiPluginConfigInput input = new AiPluginConfigInput();
    input.setId("oauth-no-scopes");
    input.setType(AiPluginType.MCP_SERVER);
    input.setServiceUrn("urn:li:service:test");
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    com.linkedin.datahub.graphql.generated.OAuthAiPluginConfigInput oauthInput =
        new com.linkedin.datahub.graphql.generated.OAuthAiPluginConfigInput();
    oauthInput.setServerUrn("urn:li:oauthAuthorizationServer:test");
    oauthInput.setRequiredScopes(null); // No scopes
    input.setOauthConfig(oauthInput);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiPluginConfigInput", AiPluginConfigInput.class);
    mapMethod.setAccessible(true);

    // Execute
    AiPluginConfig result = (AiPluginConfig) mapMethod.invoke(resolver, input);

    // Verify - should not have required scopes set
    assertNotNull(result.getOauthConfig());
    assertFalse(
        result.getOauthConfig().hasRequiredScopes(),
        "Should not have required scopes when input is null");
  }

  @Test
  public void testMapAiPluginConfigInputWithOAuthConfigEmptyScopes() throws Exception {
    // Setup - OAuth config with empty scopes list
    AiPluginConfigInput input = new AiPluginConfigInput();
    input.setId("oauth-empty-scopes");
    input.setType(AiPluginType.MCP_SERVER);
    input.setServiceUrn("urn:li:service:test");
    input.setAuthType(AiPluginAuthType.USER_OAUTH);

    com.linkedin.datahub.graphql.generated.OAuthAiPluginConfigInput oauthInput =
        new com.linkedin.datahub.graphql.generated.OAuthAiPluginConfigInput();
    oauthInput.setServerUrn("urn:li:oauthAuthorizationServer:test");
    oauthInput.setRequiredScopes(Arrays.asList()); // Empty list
    input.setOauthConfig(oauthInput);

    // Use reflection to test the private method
    Method mapMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "mapAiPluginConfigInput", AiPluginConfigInput.class);
    mapMethod.setAccessible(true);

    // Execute
    AiPluginConfig result = (AiPluginConfig) mapMethod.invoke(resolver, input);

    // Verify - should not have required scopes set for empty list
    assertNotNull(result.getOauthConfig());
    assertFalse(
        result.getOauthConfig().hasRequiredScopes(),
        "Should not have required scopes when input is empty list");
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Edge Cases
  // ═══════════════════════════════════════════════════════════════════════════

  @Test
  public void testUpdateSettingsWithAllNullInputs() throws Exception {
    // Setup
    GlobalSettingsInfo existingSettings = new GlobalSettingsInfo();
    existingSettings.setIntegrations(new com.linkedin.settings.global.GlobalIntegrationSettings());
    existingSettings.setNotifications(
        new com.linkedin.settings.global.GlobalNotificationSettings());

    com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput();
    // All fields null

    // Use reflection to test the private method
    Method updateSettingsMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateSettings",
            GlobalSettingsInfo.class,
            com.linkedin.datahub.graphql.generated.UpdateGlobalSettingsInput.class,
            QueryContext.class);
    updateSettingsMethod.setAccessible(true);

    // Execute - should not throw
    updateSettingsMethod.invoke(resolver, existingSettings, input, queryContext);

    // No assertions needed - just verifying no exceptions
  }

  @Test
  public void testUpdateSsoSettings() throws Exception {
    // Setup
    com.linkedin.settings.global.SsoSettings existingSettings =
        new com.linkedin.settings.global.SsoSettings();

    com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput();
    input.setBaseUrl("https://sso.company.com");
    // No OIDC settings

    // Use reflection to test the private method
    Method updateSsoMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateSsoSettings",
            com.linkedin.settings.global.SsoSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateSsoSettingsInput.class);
    updateSsoMethod.setAccessible(true);

    // Execute
    updateSsoMethod.invoke(resolver, existingSettings, input);

    // Verify
    assertEquals(existingSettings.getBaseUrl(), "https://sso.company.com");
  }

  @Test
  public void testUpdateGlobalIntegrationSettings() throws Exception {
    // Setup
    com.linkedin.settings.global.GlobalIntegrationSettings existingSettings =
        new com.linkedin.settings.global.GlobalIntegrationSettings();

    com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput input =
        new com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput();
    // All sub-settings null

    // Use reflection to test the private method
    Method updateIntegrationMethod =
        UpdateGlobalSettingsResolver.class.getDeclaredMethod(
            "updateGlobalIntegrationSettings",
            com.linkedin.settings.global.GlobalIntegrationSettings.class,
            com.linkedin.datahub.graphql.generated.UpdateGlobalIntegrationSettingsInput.class);
    updateIntegrationMethod.setAccessible(true);

    // Execute - should not throw
    updateIntegrationMethod.invoke(resolver, existingSettings, input);

    // No assertions needed - just verifying no exceptions
  }
}
