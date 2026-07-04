package io.datahubproject.metadata.context.usage;

import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;

/** Surface-neutral API usage operation keys (canonical list matches usage_operations.yaml). */
public enum UsageOperation {
  METADATA_READ("metadata_read"),
  METADATA_INGEST("metadata_ingest"),
  METADATA_WRITE("metadata_write"),
  ENTITY_DELETE("entity_delete"),
  ASPECT_DELETE("aspect_delete"),
  SEARCH_QUERY("search_query"),
  LINEAGE_QUERY("lineage_query"),
  METADATA_QUERY("metadata_query"),
  OTHER_READ("other_read"),
  OTHER_WRITE("other_write"),
  OTHER_OPERATIONS("other_operations"),
  MCP_QUERY("mcp_query");

  private final String key;

  UsageOperation(String key) {
    this.key = key;
  }

  public String key() {
    return key;
  }

  /** Default GraphQL operation kind when HTTP operation name maps via yaml (no AST parse). */
  public GraphQLOperationKind defaultGraphqlOperationKind() {
    return switch (this) {
      case METADATA_WRITE,
          METADATA_INGEST,
          ENTITY_DELETE,
          ASPECT_DELETE,
          OTHER_WRITE,
          OTHER_OPERATIONS -> GraphQLOperationKind.MUTATION;
      default -> GraphQLOperationKind.QUERY;
    };
  }

  public static UsageOperation fromKey(String key) {
    for (UsageOperation op : values()) {
      if (op.key.equals(key)) {
        return op;
      }
    }
    throw new IllegalArgumentException("Unknown usage_operation key: " + key);
  }
}
