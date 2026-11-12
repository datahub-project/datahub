package com.linkedin.datahub.graphql.resolvers.settings.mcp;

import com.linkedin.datahub.graphql.generated.McpInstructionSize;
import com.linkedin.datahub.graphql.generated.McpServerConfig;
import com.linkedin.datahub.graphql.generated.McpSettings;
import com.linkedin.datahub.graphql.types.agent.AgentToolRefMapper;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps the {@code mcpSettings} part of GlobalSettingsInfo to its GraphQL form. */
public class McpSettingsMapper {

  private McpSettingsMapper() {}

  /**
   * Maps stored MCP settings, or the PDL defaults (everything on, no scoped servers, no default
   * server overrides) when none are stored.
   */
  @Nonnull
  public static McpSettings map(@Nullable final com.linkedin.settings.global.McpSettings settings) {
    final McpSettings result = new McpSettings();
    if (settings == null) {
      result.setEnabled(true);
      result.setObservationTelemetryEnabled(true);
      result.setObservationToolEnabled(true);
      result.setServers(Collections.emptyList());
      return result;
    }
    result.setEnabled(settings.isEnabled());
    result.setObservationTelemetryEnabled(settings.isObservationTelemetryEnabled());
    result.setObservationToolEnabled(settings.isObservationToolEnabled());
    result.setServers(mapServers(settings));
    if (settings.hasDefaultMcpServerOverrides()) {
      result.setDefaultMcpServerOverrides(
          mapMcpServerConfig(settings.getDefaultMcpServerOverrides()));
    }
    return result;
  }

  private static List<McpServerConfig> mapServers(
      @Nonnull final com.linkedin.settings.global.McpSettings settings) {
    if (!settings.hasServers()) {
      return Collections.emptyList();
    }
    return settings.getServers().stream()
        .map(McpSettingsMapper::mapMcpServerConfig)
        .collect(Collectors.toList());
  }

  @Nonnull
  static McpServerConfig mapMcpServerConfig(
      @Nonnull final com.linkedin.settings.global.McpServerConfig gmsConfig) {
    final McpServerConfig result = new McpServerConfig();
    if (gmsConfig.hasSlug()) {
      result.setSlug(gmsConfig.getSlug());
    }
    result.setName(gmsConfig.getName());
    result.setEnabled(gmsConfig.isEnabled());
    if (gmsConfig.hasDescription()) {
      result.setDescription(gmsConfig.getDescription());
    }
    if (gmsConfig.hasCustomInstructions()) {
      result.setCustomInstructions(gmsConfig.getCustomInstructions());
    }
    final com.linkedin.settings.global.McpInstructionSize gmsSize = gmsConfig.getInstructionSize();
    // A value written by a newer schema ($UNKNOWN) leaves the field unset rather than failing.
    if (gmsSize != null) {
      result.setInstructionSize(PdlEnumMapper.mapDefaultNull(McpInstructionSize.class, gmsSize));
    }
    if (gmsConfig.hasTools()) {
      result.setTools(AgentToolRefMapper.toGraphql(gmsConfig.getTools()));
    }
    if (gmsConfig.hasViewUrn()) {
      result.setViewUrn(gmsConfig.getViewUrn().toString());
    }
    result.setCreated(AuditStampMapper.map(null, gmsConfig.getCreated()));
    result.setLastModified(AuditStampMapper.map(null, gmsConfig.getLastModified()));
    return result;
  }
}
