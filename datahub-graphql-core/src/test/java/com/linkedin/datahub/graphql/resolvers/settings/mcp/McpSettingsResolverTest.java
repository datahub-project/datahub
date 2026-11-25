package com.linkedin.datahub.graphql.resolvers.settings.mcp;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.agent.AIAgentToolRef;
import com.linkedin.agent.AIAgentToolRefArray;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.McpInstructionSize;
import com.linkedin.datahub.graphql.generated.McpSettings;
import com.linkedin.datahub.graphql.generated.ToolRefType;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.McpServerConfig;
import com.linkedin.settings.global.McpServerConfigArray;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class McpSettingsResolverTest {

  @Test
  public void testNoStoredSettingsReturnsDefaults() throws Exception {
    final SettingsService settingsService = Mockito.mock(SettingsService.class);
    Mockito.when(settingsService.getGlobalSettings(any())).thenReturn(new GlobalSettingsInfo());

    final McpSettings result = resolve(settingsService);

    assertTrue(result.getEnabled());
    assertTrue(result.getObservationTelemetryEnabled());
    assertTrue(result.getObservationToolEnabled());
    assertTrue(result.getServers().isEmpty());
    assertNull(result.getDefaultMcpServerOverrides());
  }

  @Test
  public void testManagerReadsScopedServers() throws Exception {
    final McpSettings result = resolve(storedSettings(), getMockAllowContext());

    assertFalse(result.getEnabled());
    assertEquals(result.getServers().size(), 1);
    final com.linkedin.datahub.graphql.generated.McpServerConfig server =
        result.getServers().get(0);
    assertEquals(server.getSlug(), "finance");
    assertTrue(server.getEnabled());
    assertEquals(server.getInstructionSize(), McpInstructionSize.SMALL);
    assertEquals(server.getViewUrn(), "urn:li:dataHubView:finance");
    assertEquals(server.getTools().size(), 1);
    assertEquals(server.getTools().get(0).getRefType(), ToolRefType.GROUP);
    assertEquals(server.getTools().get(0).getToolId(), "read_metadata");
    assertTrue(server.getTools().get(0).getEnabled());
    assertEquals(server.getCreated().getActor(), "urn:li:corpuser:admin");
  }

  @Test
  public void testNonManagerReadsPolicyWithoutScopedServers() throws Exception {
    // MCP callers without privileges still need the switch and the default server's overrides:
    // datahub-ai-service enforces them with the caller's own token.
    final McpSettings result = resolve(storedSettings(), getMockDenyContext());

    assertFalse(result.getEnabled());
    assertEquals(
        result.getDefaultMcpServerOverrides().getInstructionSize(), McpInstructionSize.SMALL);
    assertTrue(result.getServers().isEmpty());
  }

  @Test
  public void testSettingsServiceFailure() {
    final SettingsService settingsService = Mockito.mock(SettingsService.class);
    Mockito.when(settingsService.getGlobalSettings(any())).thenThrow(new RuntimeException("boom"));

    assertThrows(CompletionException.class, () -> resolveAsync(settingsService).join());
  }

  private static SettingsService storedSettings() {
    final AuditStamp stamp =
        new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:admin"));
    final SettingsService settingsService = Mockito.mock(SettingsService.class);
    Mockito.when(settingsService.getGlobalSettings(any()))
        .thenReturn(
            new GlobalSettingsInfo()
                .setMcpSettings(
                    new com.linkedin.settings.global.McpSettings()
                        .setEnabled(false)
                        .setDefaultMcpServerOverrides(
                            new McpServerConfig()
                                .setName("Default")
                                .setInstructionSize(
                                    com.linkedin.settings.global.McpInstructionSize.SMALL)
                                .setCreated(stamp)
                                .setLastModified(stamp))
                        .setServers(
                            new McpServerConfigArray(
                                new McpServerConfig()
                                    .setSlug("finance")
                                    .setName("Finance")
                                    .setInstructionSize(
                                        com.linkedin.settings.global.McpInstructionSize.SMALL)
                                    .setViewUrn(UrnUtils.getUrn("urn:li:dataHubView:finance"))
                                    .setTools(
                                        new AIAgentToolRefArray(
                                            new AIAgentToolRef()
                                                .setRefType(com.linkedin.agent.ToolRefType.GROUP)
                                                .setToolId("read_metadata")))
                                    .setCreated(stamp)
                                    .setLastModified(stamp)))));
    return settingsService;
  }

  private static McpSettings resolve(final SettingsService settingsService) throws Exception {
    return resolve(settingsService, getMockAllowContext());
  }

  private static McpSettings resolve(
      final SettingsService settingsService, final QueryContext context) throws Exception {
    final DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getContext()).thenReturn(context);
    return new McpSettingsResolver(settingsService).get(env).get();
  }

  private static java.util.concurrent.CompletableFuture<McpSettings> resolveAsync(
      final SettingsService settingsService) throws Exception {
    final QueryContext allowContext = getMockAllowContext();
    final DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getContext()).thenReturn(allowContext);
    return new McpSettingsResolver(settingsService).get(env);
  }
}
