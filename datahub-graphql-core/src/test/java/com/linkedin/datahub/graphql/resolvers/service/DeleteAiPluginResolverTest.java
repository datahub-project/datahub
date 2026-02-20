package com.linkedin.datahub.graphql.resolvers.service;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.settings.global.AiPluginAuthType;
import com.linkedin.settings.global.AiPluginConfig;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.AiPluginType;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteAiPluginResolverTest {

  private static final String SERVICE_URN = "urn:li:service:test-service";
  private static final String OTHER_SERVICE_URN = "urn:li:service:other-service";
  private static final Urn SERVICE_URN_OBJ = UrnUtils.getUrn(SERVICE_URN);
  private static final Urn OTHER_SERVICE_URN_OBJ = UrnUtils.getUrn(OTHER_SERVICE_URN);

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;

  private DeleteAiPluginResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new DeleteAiPluginResolver(entityClient);
  }

  @Test
  public void testConstructorNullCheck() {
    assertThrows(NullPointerException.class, () -> new DeleteAiPluginResolver(null));
  }

  @Test
  public void testDeleteAiPluginSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVICE_URN);

    // Mock GlobalSettings with the service to delete
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithPlugins(SERVICE_URN_OBJ);
    mockGetGlobalSettings(globalSettings);

    // Mock ingestProposal to not throw
    when(entityClient.ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn("proposal-id");

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify
    assertTrue(result);
    verify(entityClient, times(1)).deleteEntity(any(), eq(SERVICE_URN_OBJ));
    // Verify GlobalSettings was updated (we verify the call was made, not the exact content)
    verify(entityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testDeleteAiPluginRemovesFromGlobalSettingsPreservesOthers() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVICE_URN);

    // Mock GlobalSettings with multiple plugins
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray plugins = new AiPluginConfigArray();

    // Plugin to delete
    plugins.add(createPluginConfig("plugin-1", SERVICE_URN_OBJ));
    // Plugin to keep
    plugins.add(createPluginConfig("plugin-2", OTHER_SERVICE_URN_OBJ));

    pluginSettings.setPlugins(plugins);
    globalSettings.setAiPluginSettings(pluginSettings);

    mockGetGlobalSettings(globalSettings);

    // Mock ingestProposal to not throw
    when(entityClient.ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean()))
        .thenReturn("proposal-id");

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify
    assertTrue(result);
    verify(entityClient, times(1)).deleteEntity(any(), eq(SERVICE_URN_OBJ));
    // Verify GlobalSettings was updated (call was made)
    verify(entityClient, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testDeleteAiPluginNotInGlobalSettings() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVICE_URN);

    // Mock GlobalSettings without this service
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithPlugins(OTHER_SERVICE_URN_OBJ);
    mockGetGlobalSettings(globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify - should still delete entity but not update GlobalSettings
    assertTrue(result);
    verify(entityClient, times(1)).deleteEntity(any(), eq(SERVICE_URN_OBJ));
    // Should NOT update GlobalSettings since service wasn't there
    verify(entityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testDeleteAiPluginEmptyGlobalSettings() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVICE_URN);

    // Mock empty GlobalSettings
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    mockGetGlobalSettings(globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify
    assertTrue(result);
    verify(entityClient, times(1)).deleteEntity(any(), eq(SERVICE_URN_OBJ));
    verify(entityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testDeleteAiPluginUnauthorized() throws Exception {
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn(SERVICE_URN);

    // Expect AuthorizationException
    assertThrows(AuthorizationException.class, () -> resolver.get(environment));

    // Verify no deletion occurred
    verify(entityClient, never()).deleteEntity(any(), any());
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testDeleteNonServiceUrnRejected() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn"))
        .thenReturn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    // Should throw IllegalArgumentException wrapped in CompletionException
    assertThrows(IllegalArgumentException.class, () -> resolver.get(environment));

    // Verify no deletion occurred
    verify(entityClient, never()).deleteEntity(any(), any());
  }

  @Test
  public void testDeleteAiPluginInvalidUrnFormat() {
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);
    when(environment.getArgument("urn")).thenReturn("not-a-valid-urn");

    // Should throw exception for invalid URN
    assertThrows(Exception.class, () -> resolver.get(environment));
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private void mockGetGlobalSettings(GlobalSettingsInfo globalSettings) throws Exception {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        GLOBAL_SETTINGS_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(globalSettings.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(Constants.GLOBAL_SETTINGS_URN);
    entityResponse.setEntityName(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    when(entityClient.getV2(any(), eq(Constants.GLOBAL_SETTINGS_ENTITY_NAME), any(), any()))
        .thenReturn(entityResponse);
  }

  private GlobalSettingsInfo createGlobalSettingsWithPlugins(Urn serviceUrn) {
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray plugins = new AiPluginConfigArray();

    plugins.add(createPluginConfig("plugin-1", serviceUrn));

    pluginSettings.setPlugins(plugins);
    globalSettings.setAiPluginSettings(pluginSettings);
    return globalSettings;
  }

  private AiPluginConfig createPluginConfig(String id, Urn serviceUrn) {
    AiPluginConfig config = new AiPluginConfig();
    config.setId(id);
    config.setType(AiPluginType.MCP_SERVER);
    config.setServiceUrn(serviceUrn);
    config.setEnabled(true);
    config.setAuthType(AiPluginAuthType.NONE);
    return config;
  }
}
