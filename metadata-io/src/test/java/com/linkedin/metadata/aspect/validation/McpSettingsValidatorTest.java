package com.linkedin.metadata.aspect.validation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.McpServerConfig;
import com.linkedin.settings.global.McpServerConfigArray;
import com.linkedin.settings.global.McpSettings;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class McpSettingsValidatorTest {

  private static final Urn VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:finance-view");
  private static final Urn NOT_A_VIEW_URN = UrnUtils.getUrn("urn:li:corpuser:someone");
  private static final AuditStamp STAMP =
      new AuditStamp().setTime(1L).setActor(UrnUtils.getUrn("urn:li:corpuser:admin"));

  private McpSettingsValidator validator;

  @BeforeMethod
  public void setUp() {
    validator =
        new McpSettingsValidator()
            .setConfig(
                AspectPluginConfig.builder()
                    .className("test")
                    .enabled(true)
                    .supportedOperations(List.of("UPSERT"))
                    .supportedEntityAspectNames(List.of())
                    .build());
  }

  @Test
  public void allowsValidSettings() {
    McpSettings settings =
        new McpSettings()
            .setServers(
                new McpServerConfigArray(
                    server("finance").setViewUrn(VIEW_URN), server("sales-team-2")))
            .setDefaultMcpServerOverrides(server(null).setViewUrn(VIEW_URN));

    assertTrue(validate(settings, null).isEmpty());
    assertTrue(validate(new McpSettings(), null).isEmpty());
    assertTrue(validate(null, null).isEmpty());
  }

  @Test
  public void rejectsBadSlugsDuplicatesAndNonViewUrns() {
    McpSettings settings =
        new McpSettings()
            .setServers(
                new McpServerConfigArray(
                    server(null),
                    server("Finance_Team"),
                    server("mcp"),
                    server("sales"),
                    server("sales"),
                    server("hr").setViewUrn(NOT_A_VIEW_URN)))
            .setDefaultMcpServerOverrides(server(null).setViewUrn(NOT_A_VIEW_URN));

    // Missing slug, bad format, reserved slug, duplicate slug, and a non-View on a scoped server
    // and on the default server.
    assertEquals(validate(settings, null).size(), 6);
  }

  @Test
  public void allowsWriteThatKeepsStoredProblems() {
    McpSettings stored =
        new McpSettings()
            .setServers(
                new McpServerConfigArray(server("mcp"), server("hr").setViewUrn(NOT_A_VIEW_URN)));
    McpSettings proposed =
        new McpSettings()
            .setEnabled(false)
            .setServers(
                new McpServerConfigArray(
                    server("mcp"), server("hr").setViewUrn(NOT_A_VIEW_URN), server("finance")));

    assertTrue(validate(proposed, stored).isEmpty());
  }

  @Test
  public void rejectsNewProblemNextToStoredOne() {
    McpSettings stored = new McpSettings().setServers(new McpServerConfigArray(server("mcp")));
    McpSettings proposed =
        new McpSettings().setServers(new McpServerConfigArray(server("mcp"), server("default")));

    List<String> problems = validate(proposed, stored);

    assertEquals(problems.size(), 1);
    assertTrue(problems.get(0).contains("'default'"), problems.toString());
  }

  private List<String> validate(@Nullable McpSettings proposed, @Nullable McpSettings previous) {
    ChangeMCP item = mock(ChangeMCP.class);
    when(item.getUrn()).thenReturn(Constants.GLOBAL_SETTINGS_URN);
    when(item.getAspect(GlobalSettingsInfo.class)).thenReturn(settingsInfo(proposed));
    if (previous != null) {
      GlobalSettingsInfo previousInfo = settingsInfo(previous);
      when(item.getPreviousRecordTemplate()).thenReturn(previousInfo);
      when(item.getPreviousAspect(GlobalSettingsInfo.class)).thenReturn(previousInfo);
    }
    return validator
        .validatePreCommitAspects(
            OperationFingerprint.EMPTY, List.of(item), mock(RetrieverContext.class))
        .map(AspectValidationException::getMessage)
        .collect(Collectors.toList());
  }

  private static GlobalSettingsInfo settingsInfo(@Nullable McpSettings settings) {
    GlobalSettingsInfo info = new GlobalSettingsInfo();
    if (settings != null) {
      info.setMcpSettings(settings);
    }
    return info;
  }

  private static McpServerConfig server(@Nullable String slug) {
    McpServerConfig server =
        new McpServerConfig().setName("server").setCreated(STAMP).setLastModified(STAMP);
    if (slug != null) {
      server.setSlug(slug);
    }
    return server;
  }
}
