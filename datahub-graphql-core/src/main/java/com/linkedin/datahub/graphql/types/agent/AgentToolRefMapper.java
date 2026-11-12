package com.linkedin.datahub.graphql.types.agent;

import com.linkedin.agent.AIAgentToolRefArray;
import com.linkedin.agent.ToolRefType;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps {@link com.linkedin.agent.AIAgentToolRef} between its GMS (PDL) and GraphQL forms. Used by
 * the MCP server (McpServerConfig) tool-selection path, which shares its tool-selection model with
 * AI agents.
 */
public class AgentToolRefMapper {

  private AgentToolRefMapper() {}

  public static List<com.linkedin.datahub.graphql.generated.AgentToolRef> toGraphql(
      final AIAgentToolRefArray tools) {
    return tools.stream()
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

  private static com.linkedin.datahub.graphql.generated.ToolRefType toGraphqlRefType(
      final ToolRefType refType) {
    switch (refType) {
      case GROUP:
        return com.linkedin.datahub.graphql.generated.ToolRefType.GROUP;
      case AGENT:
        return com.linkedin.datahub.graphql.generated.ToolRefType.AGENT;
      default:
        return com.linkedin.datahub.graphql.generated.ToolRefType.TOOL;
    }
  }
}
