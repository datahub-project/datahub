package com.linkedin.metadata.graph.postgres;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import javax.annotation.Nonnull;

/** Qualified SqlSetup pgGraph table/view names for JDBC graph reads. */
public final class PostgresGraphTables {

  private final String schema;
  private final String prefix;

  public PostgresGraphTables(@Nonnull PostgresSqlSetupProperties props) {
    this.schema = props.normalizedPostgresSchema();
    this.prefix = props.normalizedPgGraphTablePrefix();
  }

  @Nonnull
  public String vertices() {
    return schema + "." + prefix + "_vertices";
  }

  @Nonnull
  public String edges() {
    return schema + "." + prefix + "_edges";
  }

  @Nonnull
  public String edgeTypes() {
    return schema + "." + prefix + "_edge_types";
  }

  @Nonnull
  public String pgroutingNetwork() {
    return schema + "." + prefix + "_pgrouting_network";
  }

  @Nonnull
  public String sqlgEdges() {
    return schema + "." + prefix + "_sqlg_edges";
  }
}
