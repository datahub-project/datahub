package com.linkedin.datahub.graphql.resolvers.settings.mcp;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.linkedin.agent.AIAgentToolRef;
import com.linkedin.agent.AIAgentToolRefArray;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.AgentToolRefInput;
import com.linkedin.datahub.graphql.generated.McpInstructionSize;
import com.linkedin.datahub.graphql.generated.McpServerConfigInput;
import com.linkedin.datahub.graphql.generated.McpSettings;
import com.linkedin.datahub.graphql.generated.ToolRefType;
import com.linkedin.datahub.graphql.generated.UpdateMcpSettingsInput;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.McpServerConfig;
import com.linkedin.settings.global.McpServerConfigArray;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class UpdateMcpSettingsResolverTest {

  private static final String VIEW_URN = "urn:li:dataHubView:finance-view";
  private static final AuditStamp CREATED =
      new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:creator"));

  @Test
  public void testNewServerRoundTrip() throws Exception {
    final SettingsService settingsService = settingsService(null);
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    input.setObservationTelemetryEnabled(false);
    final McpServerConfigInput server = serverInput("finance");
    server.setDescription("Finance data only");
    server.setCustomInstructions("Prefer certified tables.");
    server.setInstructionSize(McpInstructionSize.SMALL);
    server.setViewUrn(VIEW_URN);
    server.setTools(
        List.of(toolRef(ToolRefType.GROUP, "read_metadata", true), toolRef(null, "search", false)));
    input.setServers(List.of(server));

    assertTrue(update(settingsService, flags(true), getMockAllowContext(), input).get());

    final McpSettings stored =
        McpSettingsMapper.map(captureWrite(settingsService).getMcpSettings());
    assertTrue(stored.getEnabled());
    assertFalse(stored.getObservationTelemetryEnabled());
    assertEquals(stored.getServers().size(), 1);
    final com.linkedin.datahub.graphql.generated.McpServerConfig mapped =
        stored.getServers().get(0);
    assertEquals(mapped.getSlug(), "finance");
    assertEquals(mapped.getName(), "finance server");
    assertTrue(mapped.getEnabled());
    assertEquals(mapped.getDescription(), "Finance data only");
    assertEquals(mapped.getCustomInstructions(), "Prefer certified tables.");
    assertEquals(mapped.getInstructionSize(), McpInstructionSize.SMALL);
    assertEquals(mapped.getViewUrn(), VIEW_URN);
    assertEquals(mapped.getTools().size(), 2);
    assertEquals(mapped.getTools().get(0).getRefType(), ToolRefType.GROUP);
    // A ref without a type is a single tool.
    assertEquals(mapped.getTools().get(1).getRefType(), ToolRefType.TOOL);
    assertFalse(mapped.getTools().get(1).getEnabled());
    assertEquals(mapped.getCreated().getActor(), "urn:li:corpuser:test");
    assertEquals(mapped.getLastModified().getActor(), "urn:li:corpuser:test");
  }

  @Test
  public void testUnauthorizedUserCannotUpdate() throws Exception {
    final SettingsService settingsService = settingsService(null);
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    input.setEnabled(false);

    final CompletionException e =
        expectThrows(
            CompletionException.class,
            () -> update(settingsService, flags(true), getMockDenyContext(), input).join());
    assertTrue(e.getCause() instanceof AuthorizationException);
    Mockito.verify(settingsService, Mockito.never()).updateGlobalSettings(any(), any());
  }

  @Test
  public void testNewServerRejectedWhenCreationDisabled() throws Exception {
    final SettingsService settingsService = settingsService(null);
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    input.setServers(List.of(serverInput("finance")));

    assertRejected(settingsService, flags(false), input);
  }

  @Test
  public void testExistingServerEditableWhenCreationDisabled() throws Exception {
    final SettingsService settingsService =
        settingsService(
            new com.linkedin.settings.global.McpSettings()
                .setServers(
                    new McpServerConfigArray(
                        storedServer("finance").setSkills(new UrnArray(skillUrn())),
                        storedServer("sales"))));
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    final McpServerConfigInput finance = serverInput("finance");
    finance.setEnabled(false);
    input.setServers(List.of(finance));

    assertTrue(update(settingsService, flags(false), getMockAllowContext(), input).get());

    final McpServerConfigArray servers =
        captureWrite(settingsService).getMcpSettings().getServers();
    assertEquals(servers.size(), 2);
    final McpServerConfig updated = servers.get(0);
    assertEquals(updated.getSlug(), "finance");
    assertFalse(updated.isEnabled());
    assertEquals(updated.getCreated(), CREATED);
    // Skills cannot be set through this API, so an update keeps them.
    assertEquals(updated.getSkills(), new UrnArray(skillUrn()));
    assertEquals(servers.get(1), storedServer("sales"));
  }

  @Test
  public void testInvalidSlugsRejected() throws Exception {
    for (String slug : List.of("mcp", "default", "Finance_Team", "-finance", " ")) {
      final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
      input.setServers(List.of(serverInput(slug)));
      assertRejected(settingsService(null), flags(true), input);
    }
  }

  @Test
  public void testNonViewUrnRejected() throws Exception {
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    final McpServerConfigInput server = serverInput("finance");
    server.setViewUrn("urn:li:corpuser:someone");
    input.setServers(List.of(server));

    assertRejected(settingsService(null), flags(true), input);
  }

  @Test
  public void testDefaultServerOverrides() throws Exception {
    final SettingsService settingsService = settingsService(null);
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    final McpServerConfigInput overrides = serverInput("ignored");
    overrides.setCustomInstructions("Answer in French.");
    input.setDefaultMcpServerOverrides(overrides);

    // Customizing the default server does not need the creation flag.
    assertTrue(update(settingsService, flags(false), getMockAllowContext(), input).get());

    final McpServerConfig stored =
        captureWrite(settingsService).getMcpSettings().getDefaultMcpServerOverrides();
    assertEquals(stored.getCustomInstructions(), "Answer in French.");
    assertFalse(stored.hasSlug());
  }

  @Test
  public void testBlankDefaultServerOverridesAreCleared() throws Exception {
    final SettingsService settingsService =
        settingsService(
            new com.linkedin.settings.global.McpSettings()
                .setDefaultMcpServerOverrides(
                    storedServer("unused").setCustomInstructions("Answer in French.")));
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    final McpServerConfigInput overrides = new McpServerConfigInput();
    overrides.setName("");
    overrides.setCustomInstructions(" ");
    input.setDefaultMcpServerOverrides(overrides);

    assertTrue(update(settingsService, flags(false), getMockAllowContext(), input).get());

    assertFalse(captureWrite(settingsService).getMcpSettings().hasDefaultMcpServerOverrides());
  }

  @Test
  public void testDefaultServerOverrideWithOnlyANameIsKept() throws Exception {
    final SettingsService settingsService = settingsService(null);
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    final McpServerConfigInput overrides = new McpServerConfigInput();
    overrides.setName("Company MCP");
    // The edit modal always sends an instruction size.
    overrides.setInstructionSize(McpInstructionSize.LARGE);
    input.setDefaultMcpServerOverrides(overrides);

    assertTrue(update(settingsService, flags(false), getMockAllowContext(), input).get());

    final McpServerConfig stored =
        captureWrite(settingsService).getMcpSettings().getDefaultMcpServerOverrides();
    assertEquals(stored.getName(), "Company MCP");
  }

  @Test
  public void testUpdateKeepsDomainScope() throws Exception {
    final com.linkedin.common.urn.Urn domainUrn = UrnUtils.getUrn("urn:li:domain:finance");
    final SettingsService settingsService =
        settingsService(
            new com.linkedin.settings.global.McpSettings()
                .setServers(
                    new McpServerConfigArray(
                        storedServer("finance")
                            .setDomainUrn(domainUrn)
                            .setDescription("Old description")))
                .setDefaultMcpServerOverrides(storedServer("unused").setDomainUrn(domainUrn)));
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    input.setServers(List.of(serverInput("finance")));
    // Otherwise blank, so only the stored domain scope keeps this override.
    final McpServerConfigInput overrides = new McpServerConfigInput();
    overrides.setName("");
    input.setDefaultMcpServerOverrides(overrides);

    assertTrue(update(settingsService, flags(false), getMockAllowContext(), input).get());

    final com.linkedin.settings.global.McpSettings written =
        captureWrite(settingsService).getMcpSettings();
    final McpServerConfig server = written.getServers().get(0);
    assertEquals(server.getDomainUrn(), domainUrn);
    // A field the input carries is still replaced: leaving it out clears it.
    assertFalse(server.hasDescription());
    assertEquals(written.getDefaultMcpServerOverrides().getDomainUrn(), domainUrn);
  }

  @Test
  public void testLoadAndSaveKeepsToolRefsGraphqlCannotShow() throws Exception {
    final AIAgentToolRef plugin =
        new AIAgentToolRef()
            .setRefType(com.linkedin.agent.ToolRefType.PLUGIN)
            .setToolId("github")
            .setEnabled(true);
    final AIAgentToolRef future =
        new AIAgentToolRef(
            new DataMap(Map.of("refType", "FUTURE_TYPE", "toolId", "future", "enabled", true)));
    final AIAgentToolRef group =
        new AIAgentToolRef()
            .setRefType(com.linkedin.agent.ToolRefType.GROUP)
            .setToolId("read_metadata")
            .setEnabled(true);
    final McpServerConfig stored =
        storedServer("finance").setTools(new AIAgentToolRefArray(group, plugin, future));
    final SettingsService settingsService =
        settingsService(
            new com.linkedin.settings.global.McpSettings()
                .setServers(new McpServerConfigArray(stored)));

    // Save exactly what a client loaded, as the settings page does.
    final com.linkedin.datahub.graphql.generated.McpServerConfig loaded =
        McpSettingsMapper.mapMcpServerConfig(stored);
    assertEquals(loaded.getTools().size(), 1);
    final McpServerConfigInput server = serverInput("finance");
    server.setTools(
        loaded.getTools().stream()
            .map(t -> toolRef(t.getRefType(), t.getToolId(), t.getEnabled()))
            .collect(Collectors.toList()));
    final UpdateMcpSettingsInput input = new UpdateMcpSettingsInput();
    input.setServers(List.of(server));

    assertTrue(update(settingsService, flags(false), getMockAllowContext(), input).get());

    final AIAgentToolRefArray written =
        captureWrite(settingsService).getMcpSettings().getServers().get(0).getTools();
    assertEquals(written.size(), 3);
    assertEquals(written.get(0).getRefType(), com.linkedin.agent.ToolRefType.GROUP);
    assertEquals(written.get(1).getRefType(), com.linkedin.agent.ToolRefType.PLUGIN);
    assertEquals(written.get(1).getToolId(), "github");
    assertEquals(written.get(2).data().get("refType"), "FUTURE_TYPE");
  }

  private static void assertRejected(
      final SettingsService settingsService,
      final FeatureFlags flags,
      final UpdateMcpSettingsInput input) {
    final CompletionException e =
        expectThrows(
            CompletionException.class,
            () -> update(settingsService, flags, getMockAllowContext(), input).join());
    assertTrue(hasCause(e, IllegalArgumentException.class), "expected a validation error: " + e);
    Mockito.verify(settingsService, Mockito.never()).updateGlobalSettings(any(), any());
  }

  private static boolean hasCause(Throwable t, final Class<? extends Throwable> type) {
    for (; t != null; t = t.getCause()) {
      if (type.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  private static java.util.concurrent.CompletableFuture<Boolean> update(
      final SettingsService settingsService,
      final FeatureFlags flags,
      final QueryContext context,
      final UpdateMcpSettingsInput input)
      throws Exception {
    final DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(env.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(env.getContext()).thenReturn(context);
    return new UpdateMcpSettingsResolver(settingsService, flags).get(env);
  }

  private static SettingsService settingsService(
      @Nullable final com.linkedin.settings.global.McpSettings existing) {
    final SettingsService settingsService = Mockito.mock(SettingsService.class);
    final GlobalSettingsInfo globalSettings = new GlobalSettingsInfo();
    if (existing != null) {
      globalSettings.setMcpSettings(existing);
    }
    Mockito.when(settingsService.getGlobalSettings(any())).thenReturn(globalSettings);
    return settingsService;
  }

  private static GlobalSettingsInfo captureWrite(final SettingsService settingsService) {
    final ArgumentCaptor<GlobalSettingsInfo> captor =
        ArgumentCaptor.forClass(GlobalSettingsInfo.class);
    Mockito.verify(settingsService).updateGlobalSettings(any(), captor.capture());
    return captor.getValue();
  }

  private static FeatureFlags flags(final boolean createMcpServersEnabled) {
    final FeatureFlags flags = new FeatureFlags();
    flags.setCreateMcpServersEnabled(createMcpServersEnabled);
    return flags;
  }

  private static McpServerConfigInput serverInput(final String slug) {
    final McpServerConfigInput input = new McpServerConfigInput();
    input.setSlug(slug);
    input.setName(slug + " server");
    return input;
  }

  private static AgentToolRefInput toolRef(
      @Nullable final ToolRefType refType, final String toolId, final boolean enabled) {
    final AgentToolRefInput ref = new AgentToolRefInput();
    ref.setRefType(refType);
    ref.setToolId(toolId);
    ref.setEnabled(enabled);
    return ref;
  }

  private static McpServerConfig storedServer(final String slug) {
    return new McpServerConfig()
        .setSlug(slug)
        .setName(slug)
        .setCreated(CREATED)
        .setLastModified(CREATED);
  }

  private static com.linkedin.common.urn.Urn skillUrn() {
    return UrnUtils.getUrn("urn:li:agentSkill:sql");
  }
}
