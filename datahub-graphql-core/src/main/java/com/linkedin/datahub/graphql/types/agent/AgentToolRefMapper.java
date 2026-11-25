package com.linkedin.datahub.graphql.types.agent;

import com.linkedin.agent.AIAgentToolRef;
import com.linkedin.agent.AIAgentToolRefArray;
import com.linkedin.agent.ToolRefType;
import com.linkedin.datahub.graphql.generated.AgentToolRefInput;
import com.linkedin.datahub.graphql.types.mappers.PdlEnumMapper;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps {@link com.linkedin.agent.AIAgentToolRef} between its GMS (PDL) and GraphQL forms. Used by
 * the MCP server (McpServerConfig) tool-selection path, which shares its tool-selection model with
 * AI agents.
 *
 * <p>The GraphQL {@code ToolRefType} enum has fewer values than the PDL one (it lacks PLUGIN, and a
 * newer schema can add more). Refs of those types are left out of reads rather than shown as some
 * other type, and writers keep them via {@link #unrepresentable}.
 */
public class AgentToolRefMapper {

  private AgentToolRefMapper() {}

  public static List<com.linkedin.datahub.graphql.generated.AgentToolRef> toGraphql(
      final AIAgentToolRefArray tools) {
    return tools.stream()
        .filter(t -> toGraphqlRefType(t.getRefType()) != null)
        .map(
            t -> {
              final com.linkedin.datahub.graphql.generated.AgentToolRef ref =
                  new com.linkedin.datahub.graphql.generated.AgentToolRef();
              ref.setRefType(toGraphqlRefType(t.getRefType()));
              ref.setToolId(t.getToolId());
              ref.setEnabled(t.isEnabled());
              return ref;
            })
        .collect(Collectors.toList());
  }

  /** The stored refs {@link #toGraphql} leaves out because GraphQL has no value for their type. */
  @Nonnull
  public static List<AIAgentToolRef> unrepresentable(@Nullable final AIAgentToolRefArray tools) {
    if (tools == null) {
      return Collections.emptyList();
    }
    return tools.stream()
        .filter(t -> toGraphqlRefType(t.getRefType()) == null)
        .collect(Collectors.toList());
  }

  public static AIAgentToolRefArray toGms(final List<AgentToolRefInput> inputs) {
    return new AIAgentToolRefArray(
        inputs.stream()
            .map(
                t -> {
                  final AIAgentToolRef ref = new AIAgentToolRef();
                  ref.setRefType(toGmsRefType(t.getRefType()));
                  ref.setToolId(t.getToolId());
                  ref.setEnabled(t.getEnabled());
                  return ref;
                })
            .collect(Collectors.toList()));
  }

  @Nullable
  private static com.linkedin.datahub.graphql.generated.ToolRefType toGraphqlRefType(
      @Nonnull final ToolRefType refType) {
    return PdlEnumMapper.mapDefaultNull(
        com.linkedin.datahub.graphql.generated.ToolRefType.class, refType);
  }

  private static ToolRefType toGmsRefType(
      final com.linkedin.datahub.graphql.generated.ToolRefType refType) {
    if (refType == null) {
      return ToolRefType.TOOL;
    }
    switch (refType) {
      case GROUP:
        return ToolRefType.GROUP;
      case AGENT:
        return ToolRefType.AGENT;
      default:
        return ToolRefType.TOOL;
    }
  }
}
