package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.settings.global.AiPluginAuthType;
import com.linkedin.settings.global.AiPluginConfig;
import com.linkedin.settings.global.AiPluginConfigArray;
import com.linkedin.settings.global.AiPluginSettings;
import com.linkedin.settings.global.AiPluginType;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DeleteAiPluginResolverTest {

  private static final String TEST_PLUGIN_ID = "test-plugin-id";

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;

  private DeleteAiPluginResolver resolver;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new DeleteAiPluginResolver(entityClient);
    when(environment.getArgument("id")).thenReturn(TEST_PLUGIN_ID);
  }

  @Test
  public void testDeletePluginSuccess() throws Exception {
    // Use TestUtils for proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create GlobalSettings with the plugin to delete
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithPlugin(TEST_PLUGIN_ID);

    // Mock getGlobalSettings
    mockGetGlobalSettings(mockContext, globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify
    assertTrue(result);

    // Verify ingestProposal was called to update settings
    verify(entityClient, times(1)).ingestProposal(any(), any(), eq(false));
  }

  @Test
  public void testDeletePluginNotFound() throws Exception {
    // Use TestUtils for proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create GlobalSettings WITHOUT the target plugin
    GlobalSettingsInfo globalSettings = createGlobalSettingsWithPlugin("other-plugin-id");

    // Mock getGlobalSettings
    mockGetGlobalSettings(mockContext, globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify - should return false when plugin not found
    assertFalse(result);

    // Verify ingestProposal was NOT called
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testDeletePluginNoPluginSettings() throws Exception {
    // Use TestUtils for proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create GlobalSettings without any plugin settings
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();

    // Mock getGlobalSettings
    mockGetGlobalSettings(mockContext, globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify - should return false when no plugins configured
    assertFalse(result);

    // Verify ingestProposal was NOT called
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testDeletePluginEmptyPluginsArray() throws Exception {
    // Use TestUtils for proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create GlobalSettings with empty plugins array
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    pluginSettings.setPlugins(new AiPluginConfigArray());
    globalSettings.setAiPluginSettings(pluginSettings);

    // Mock getGlobalSettings
    mockGetGlobalSettings(mockContext, globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify - should return false when no plugins in array
    assertFalse(result);

    // Verify ingestProposal was NOT called
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testDeletePluginUnauthorized() throws Exception {
    // Use TestUtils for proper deny context
    QueryContext mockContext = getMockDenyContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Execute and expect exception
    try {
      resolver.get(environment).join();
      fail("Expected AuthorizationException");
    } catch (CompletionException e) {
      assertTrue(e.getCause() instanceof AuthorizationException);
    }

    // Verify no entity operations were performed
    verify(entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testDeletePluginMultiplePlugins() throws Exception {
    // Use TestUtils for proper auth context
    QueryContext mockContext = getMockAllowContext();
    when(environment.getContext()).thenReturn(mockContext);

    // Create GlobalSettings with multiple plugins
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    // Plugin to delete
    AiPluginConfig plugin1 = createPlugin(TEST_PLUGIN_ID);
    pluginsArray.add(plugin1);

    // Plugin to keep
    AiPluginConfig plugin2 = createPlugin("keep-me-plugin");
    pluginsArray.add(plugin2);

    pluginSettings.setPlugins(pluginsArray);
    globalSettings.setAiPluginSettings(pluginSettings);

    // Mock getGlobalSettings
    mockGetGlobalSettings(mockContext, globalSettings);

    // Execute
    Boolean result = resolver.get(environment).join();

    // Verify
    assertTrue(result);

    // Verify ingestProposal was called
    ArgumentCaptor<com.linkedin.mxe.MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(com.linkedin.mxe.MetadataChangeProposal.class);
    verify(entityClient, times(1)).ingestProposal(any(), proposalCaptor.capture(), eq(false));

    // The remaining plugin should still be there (we can't easily verify the content,
    // but we verified that the update happened)
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  private void mockGetGlobalSettings(QueryContext context, GlobalSettingsInfo globalSettings)
      throws Exception {
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        Constants.GLOBAL_SETTINGS_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(globalSettings.data())));

    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(Constants.GLOBAL_SETTINGS_URN);
    entityResponse.setEntityName(Constants.GLOBAL_SETTINGS_ENTITY_NAME);
    entityResponse.setAspects(aspects);

    when(entityClient.getV2(
            eq(context.getOperationContext()),
            eq(Constants.GLOBAL_SETTINGS_ENTITY_NAME),
            any(),
            any()))
        .thenReturn(entityResponse);
  }

  private GlobalSettingsInfo createGlobalSettingsWithPlugin(String pluginId) {
    GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    AiPluginSettings pluginSettings = new AiPluginSettings();
    AiPluginConfigArray pluginsArray = new AiPluginConfigArray();

    AiPluginConfig plugin = createPlugin(pluginId);
    pluginsArray.add(plugin);

    pluginSettings.setPlugins(pluginsArray);
    globalSettings.setAiPluginSettings(pluginSettings);
    return globalSettings;
  }

  private AiPluginConfig createPlugin(String pluginId) {
    AiPluginConfig plugin = new AiPluginConfig();
    plugin.setId(pluginId);
    plugin.setType(AiPluginType.MCP_SERVER);
    plugin.setServiceUrn(UrnUtils.getUrn("urn:li:service:" + pluginId));
    plugin.setEnabled(true);
    plugin.setAuthType(AiPluginAuthType.NONE);
    return plugin;
  }
}
