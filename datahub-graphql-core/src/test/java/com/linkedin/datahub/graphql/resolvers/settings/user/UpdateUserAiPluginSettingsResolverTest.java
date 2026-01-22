package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateUserAiPluginSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.UserAiPluginConfig;
import com.linkedin.identity.UserAiPluginConfigArray;
import com.linkedin.identity.UserAiPluginSettings;
import com.linkedin.identity.UserApiKeyConnectionConfig;
import com.linkedin.identity.UserOAuthConnectionConfig;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateUserAiPluginSettingsResolverTest {

  private static final String TEST_PLUGIN_ID = "urn:li:service:test-plugin";
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testEnablePlugin() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setEnabled(true);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    assertTrue(capturedSettings.hasAiPluginSettings());
    assertEquals(capturedSettings.getAiPluginSettings().getPlugins().size(), 1);

    UserAiPluginConfig pluginConfig = capturedSettings.getAiPluginSettings().getPlugins().get(0);
    assertEquals(pluginConfig.getId(), TEST_PLUGIN_ID);
    assertTrue(pluginConfig.isEnabled());
  }

  @Test
  public void testDisablePlugin() throws Exception {
    // Start with an enabled plugin
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setAiPluginSettings(
                new UserAiPluginSettings()
                    .setPlugins(
                        new UserAiPluginConfigArray(
                            new UserAiPluginConfig().setId(TEST_PLUGIN_ID).setEnabled(true))));

    SettingsService mockService = initSettingsService(TEST_USER_URN, existingSettings);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setEnabled(false);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    UserAiPluginConfig pluginConfig = capturedSettings.getAiPluginSettings().getPlugins().get(0);
    assertFalse(pluginConfig.isEnabled());
  }

  @Test
  public void testSetOAuthConnection() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    String oauthConnectionUrn = "urn:li:dataHubConnection:test-connection";
    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setOauthConnectionUrn(oauthConnectionUrn);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    UserAiPluginConfig pluginConfig = capturedSettings.getAiPluginSettings().getPlugins().get(0);
    assertTrue(pluginConfig.hasOauthConfig());
    assertEquals(pluginConfig.getOauthConfig().getConnectionUrn().toString(), oauthConnectionUrn);
  }

  @Test
  public void testDisconnectOAuth() throws Exception {
    // Start with an OAuth-connected plugin
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setAiPluginSettings(
                new UserAiPluginSettings()
                    .setPlugins(
                        new UserAiPluginConfigArray(
                            new UserAiPluginConfig()
                                .setId(TEST_PLUGIN_ID)
                                .setEnabled(true)
                                .setOauthConfig(
                                    new UserOAuthConnectionConfig()
                                        .setConnectionUrn(
                                            UrnUtils.getUrn(
                                                "urn:li:dataHubConnection:old-conn"))))));

    SettingsService mockService = initSettingsService(TEST_USER_URN, existingSettings);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setDisconnectOAuth(true);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    UserAiPluginConfig pluginConfig = capturedSettings.getAiPluginSettings().getPlugins().get(0);
    assertFalse(pluginConfig.hasOauthConfig());
  }

  @Test
  public void testSetApiKey() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setApiKey("connected"); // Non-empty string signals API key connection

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    UserAiPluginConfig pluginConfig = capturedSettings.getAiPluginSettings().getPlugins().get(0);
    assertTrue(pluginConfig.hasApiKeyConfig());
    assertNotNull(pluginConfig.getApiKeyConfig().getConnectionUrn());
  }

  @Test
  public void testRemoveApiKey() throws Exception {
    // Start with an API key-connected plugin
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setAiPluginSettings(
                new UserAiPluginSettings()
                    .setPlugins(
                        new UserAiPluginConfigArray(
                            new UserAiPluginConfig()
                                .setId(TEST_PLUGIN_ID)
                                .setEnabled(true)
                                .setApiKeyConfig(
                                    new UserApiKeyConnectionConfig()
                                        .setConnectionUrn(
                                            UrnUtils.getUrn(
                                                "urn:li:dataHubConnection:old-conn"))))));

    SettingsService mockService = initSettingsService(TEST_USER_URN, existingSettings);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setApiKey(""); // Empty string means disconnect

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    UserAiPluginConfig pluginConfig = capturedSettings.getAiPluginSettings().getPlugins().get(0);
    assertFalse(pluginConfig.hasApiKeyConfig());
  }

  @Test
  public void testUpdateExistingPlugin() throws Exception {
    // Start with a plugin that has some settings
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setAiPluginSettings(
                new UserAiPluginSettings()
                    .setPlugins(
                        new UserAiPluginConfigArray(
                            new UserAiPluginConfig().setId(TEST_PLUGIN_ID).setEnabled(false))));

    SettingsService mockService = initSettingsService(TEST_USER_URN, existingSettings);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setEnabled(true);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    // Should still have exactly 1 plugin (updated, not duplicated)
    assertEquals(capturedSettings.getAiPluginSettings().getPlugins().size(), 1);
    assertTrue(capturedSettings.getAiPluginSettings().getPlugins().get(0).isEnabled());
  }

  @Test
  public void testMultiplePlugins() throws Exception {
    String plugin2Id = "urn:li:service:another-plugin";

    // Start with one plugin
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            .setAiPluginSettings(
                new UserAiPluginSettings()
                    .setPlugins(
                        new UserAiPluginConfigArray(
                            new UserAiPluginConfig().setId(TEST_PLUGIN_ID).setEnabled(true))));

    SettingsService mockService = initSettingsService(TEST_USER_URN, existingSettings);
    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    // Add a second plugin
    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(plugin2Id);
    input.setEnabled(true);

    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    Mockito.verify(mockService, Mockito.times(1))
        .updateCorpUserSettings(any(), Mockito.eq(TEST_USER_URN), settingsCaptor.capture());

    CorpUserSettings capturedSettings = settingsCaptor.getValue();
    // Should now have 2 plugins
    assertEquals(capturedSettings.getAiPluginSettings().getPlugins().size(), 2);
  }

  @Test
  public void testGetCorpUserSettingsException() throws Exception {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .getCorpUserSettings(any(), Mockito.eq(TEST_USER_URN));

    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setEnabled(true);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testUpdateCorpUserSettingsException() throws Exception {
    SettingsService mockService = initSettingsService(TEST_USER_URN, null);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .updateCorpUserSettings(
            any(), Mockito.eq(TEST_USER_URN), Mockito.any(CorpUserSettings.class));

    UpdateUserAiPluginSettingsResolver resolver =
        new UpdateUserAiPluginSettingsResolver(mockService);

    UpdateUserAiPluginSettingsInput input = new UpdateUserAiPluginSettingsInput();
    input.setPluginId(TEST_PLUGIN_ID);
    input.setEnabled(true);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static SettingsService initSettingsService(Urn user, CorpUserSettings existingSettings) {
    SettingsService mockService = Mockito.mock(SettingsService.class);
    Mockito.when(mockService.getCorpUserSettings(any(), Mockito.eq(user)))
        .thenReturn(existingSettings);
    return mockService;
  }
}
