package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AiAssistantConfigServiceTest {

  @Test
  public void testUpsertProviderKey() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);

    when(platformService.exists(any())).thenReturn(false);
    when(platformService.encrypt("sk-ant-api03-1234")).thenReturn("encrypted-value");
    when(platformService.getActorUrn()).thenReturn(Urn.createFromString("urn:li:corpuser:datahub"));

    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.ProviderKeyResult result =
        service.upsertProviderKey("claude", "sk-ant-api03-1234");

    Assert.assertEquals(result.getProvider(), "claude");
    Assert.assertTrue(result.isHasKey());
    Assert.assertTrue(result.isUpdated());
  }

  @Test
  public void testGetProviderKey() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    when(platformService.exists(any())).thenReturn(true);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.ProviderKeyResult result = service.getProviderKey("claude");

    Assert.assertEquals(result.getProvider(), "claude");
    Assert.assertTrue(result.isHasKey());
    Assert.assertFalse(result.isUpdated());
    Assert.assertNull(result.getKeyPreview());
  }

  @Test
  public void testGetProvidersReturnsAllSupportedProviders() {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.ProvidersResult result = service.getProviders();

    Assert.assertEquals(
        result.getProviders(),
        java.util.List.of(
            AiAssistantConfigService.Provider.CLAUDE, AiAssistantConfigService.Provider.OPENAI));
  }

  @Test
  public void testGetModelsReturnsAllSupportedModels() {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.ModelsResult result = service.getModels();

    Assert.assertEquals(
        result.getModels(),
        java.util.List.of(
            AiAssistantConfigService.Model.SONNET,
            AiAssistantConfigService.Model.OPUS,
            AiAssistantConfigService.Model.GPT_5_5));
  }
}
