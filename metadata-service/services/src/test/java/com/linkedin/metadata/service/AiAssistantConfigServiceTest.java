package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.identity.CorpUserAIAssistantSettings;
import com.linkedin.identity.CorpUserSettings;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AiAssistantConfigServiceTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:datahub");
  private static final OperationContext TEST_OP_CONTEXT =
      TestOperationContexts.userContextNoSearchAuthorization(TEST_USER_URN);

  @Test
  public void testUpsertProviderKey() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);

    when(platformService.exists(any(), any())).thenReturn(false);
    when(platformService.encrypt(any(), org.mockito.ArgumentMatchers.eq("sk-ant-api03-1234")))
        .thenReturn("encrypted-value");
    when(platformService.getActorUrn(any())).thenReturn(TEST_USER_URN);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.ProviderKeyResult result =
        service.upsertProviderKey(TEST_OP_CONTEXT, "claude", "sk-ant-api03-1234");

    Assert.assertEquals(result.getProvider(), "claude");
    Assert.assertTrue(result.isHasKey());
    Assert.assertTrue(result.isUpdated());
  }

  @Test
  public void testGetProviderKey() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    when(platformService.exists(any(), any())).thenReturn(true);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.ProviderKeyResult result =
        service.getProviderKey(TEST_OP_CONTEXT, "claude");

    Assert.assertEquals(result.getProvider(), "claude");
    Assert.assertTrue(result.isHasKey());
    Assert.assertFalse(result.isUpdated());
    Assert.assertNull(result.getKeyPreview());
  }

  @Test
  public void testGetPreferredModelReadsCorpUserSettings() throws Exception {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    when(platformService.getActorUrn(any())).thenReturn(TEST_USER_URN);
    when(platformService.getCorpUserSettings(any(), org.mockito.ArgumentMatchers.eq(TEST_USER_URN)))
        .thenReturn(
            new CorpUserSettings()
                .setAiAssistant(
                    new CorpUserAIAssistantSettings().setPreferredModel("claude-sonnet-5")));
    when(platformService.exists(any(), any())).thenReturn(true);

    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.PreferredModelResult result =
        service.getPreferredModel(TEST_OP_CONTEXT);

    Assert.assertEquals(result.getModel(), "claude-sonnet-5");
    Assert.assertTrue(result.isHasKey());
    Assert.assertNull(result.getKeyPreview());
  }

  @Test
  public void testUpdatePreferredModelWritesCorpUserSettings() {
    AiAssistantConfigPlatformService platformService = mock(AiAssistantConfigPlatformService.class);
    when(platformService.getActorUrn(any())).thenReturn(TEST_USER_URN);
    when(platformService.getCorpUserSettings(any(), org.mockito.ArgumentMatchers.eq(TEST_USER_URN)))
        .thenReturn(new CorpUserSettings());

    AiAssistantConfigService service = new AiAssistantConfigService(platformService);

    AiAssistantConfigService.UpdatePreferredModelResult result =
        service.updatePreferredModel(TEST_OP_CONTEXT, "gpt-5-5");

    Assert.assertEquals(result.getModel(), "gpt-5-5");
    Assert.assertTrue(result.isUpdated());
    org.mockito.Mockito.verify(platformService)
        .updateCorpUserSettings(any(), org.mockito.ArgumentMatchers.eq(TEST_USER_URN), any());
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
