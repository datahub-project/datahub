package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.settings.global.FeatureSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AiAssistantConfigServiceTest {

  @Test
  public void testUpsertProviderKeyStoresPreviewMetadata() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    when(platformService.exists(any())).thenReturn(false);
    when(platformService.encrypt("sk-ant-api03-1234")).thenReturn("encrypted-value");
    when(platformService.getGlobalSettings()).thenReturn(null);
    when(platformService.getActorUrn()).thenReturn(Urn.createFromString("urn:li:corpuser:datahub"));

    AiAssistantConfigService service = new AiAssistantConfigService(platformService, objectMapper);

    AiAssistantConfigService.ProviderKeyResult result =
        service.upsertProviderKey("claude", "sk-ant-api03-1234");

    Assert.assertEquals(result.getProvider(), "claude");
    Assert.assertTrue(result.isHasKey());
    Assert.assertTrue(result.isUpdated());

    ArgumentCaptor<GlobalSettingsInfo> settingsCaptor =
        ArgumentCaptor.forClass(GlobalSettingsInfo.class);
    Mockito.verify(platformService).updateGlobalSettings(settingsCaptor.capture());

    GlobalSettingsInfo savedSettings = settingsCaptor.getValue();
    Assert.assertTrue(savedSettings.hasAiAssistant());

    AiAssistantConfigService.AiAssistantSettingsConfig config =
        objectMapper.readValue(
            savedSettings.getAiAssistant().getConfig(),
            AiAssistantConfigService.AiAssistantSettingsConfig.class);
    Assert.assertEquals(
        config.getProviderCredentials().get("claude").getKeyPreview(), "sk-ant-...1234");
  }

  @Test
  public void testGetPreferredModelReturnsKeyMetadata() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    AiAssistantConfigService.AiAssistantSettingsConfig config =
        new AiAssistantConfigService.AiAssistantSettingsConfig();
    config.setPreferredModel("claude-sonnet-5");
    config
        .getProviderCredentials()
        .put(
            "claude",
            AiAssistantConfigService.ProviderCredentialMetadata.builder()
                .keyPreview("sk-ant-...1234")
                .build());

    GlobalSettingsInfo globalSettingsInfo =
        new GlobalSettingsInfo()
            .setAiAssistant(
                new FeatureSettings()
                    .setEnabled(true)
                    .setConfig(objectMapper.writeValueAsString(config)));

    when(platformService.getGlobalSettings()).thenReturn(globalSettingsInfo);
    when(platformService.exists(any())).thenReturn(true);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService, objectMapper);

    AiAssistantConfigService.PreferredModelResult result = service.getPreferredModel();

    Assert.assertEquals(result.getModel(), "claude-sonnet-5");
    Assert.assertTrue(result.isHasKey());
    Assert.assertEquals(result.getKeyPreview(), "sk-ant-...1234");
  }

  @Test
  public void testGetProviderKeyReturnsKeyMetadata() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    AiAssistantConfigService.AiAssistantSettingsConfig config =
        new AiAssistantConfigService.AiAssistantSettingsConfig();
    config
        .getProviderCredentials()
        .put(
            "claude",
            AiAssistantConfigService.ProviderCredentialMetadata.builder()
                .keyPreview("sk-ant-...1234")
                .build());

    GlobalSettingsInfo globalSettingsInfo =
        new GlobalSettingsInfo()
            .setAiAssistant(
                new FeatureSettings()
                    .setEnabled(true)
                    .setConfig(objectMapper.writeValueAsString(config)));

    when(platformService.getGlobalSettings()).thenReturn(globalSettingsInfo);
    when(platformService.exists(any())).thenReturn(true);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService, objectMapper);

    AiAssistantConfigService.ProviderKeyResult result = service.getProviderKey("claude");

    Assert.assertEquals(result.getProvider(), "claude");
    Assert.assertTrue(result.isHasKey());
    Assert.assertFalse(result.isUpdated());
    Assert.assertEquals(result.getKeyPreview(), "sk-ant-...1234");
  }

  @Test
  public void testGetProvidersReturnsAllSupportedProviders() {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    AiAssistantConfigService service = new AiAssistantConfigService(platformService, objectMapper);

    AiAssistantConfigService.ProvidersResult result = service.getProviders();

    Assert.assertEquals(
        result.getProviders(),
        java.util.List.of(
            AiAssistantConfigService.Provider.CLAUDE, AiAssistantConfigService.Provider.OPENAI));
  }

  @Test
  public void testGetModelsReturnsAllSupportedModels() {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    AiAssistantConfigService service = new AiAssistantConfigService(platformService, objectMapper);

    AiAssistantConfigService.ModelsResult result = service.getModels();

    Assert.assertEquals(
        result.getModels(),
        java.util.List.of(
            AiAssistantConfigService.Model.SONNET,
            AiAssistantConfigService.Model.OPUS,
            AiAssistantConfigService.Model.GPT_5_5));
  }

  @Test
  public void testUpdatePreferredModelPersistsConfig() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    ObjectMapper objectMapper = new ObjectMapper();

    when(platformService.getGlobalSettings()).thenReturn(null);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService, objectMapper);

    AiAssistantConfigService.UpdatePreferredModelResult result =
        service.updatePreferredModel("claude-sonnet-5");

    Assert.assertEquals(result.getModel(), "claude-sonnet-5");
    Assert.assertTrue(result.isUpdated());

    ArgumentCaptor<GlobalSettingsInfo> settingsCaptor =
        ArgumentCaptor.forClass(GlobalSettingsInfo.class);
    Mockito.verify(platformService).updateGlobalSettings(settingsCaptor.capture());

    AiAssistantConfigService.AiAssistantSettingsConfig config =
        objectMapper.readValue(
            settingsCaptor.getValue().getAiAssistant().getConfig(),
            AiAssistantConfigService.AiAssistantSettingsConfig.class);
    Assert.assertEquals(config.getPreferredModel(), "claude-sonnet-5");
  }
}
