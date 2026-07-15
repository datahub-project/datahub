package com.linkedin.metadata.graph.postgres;

import com.google.common.collect.Lists;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.models.graph.Edge;
import com.linkedin.metadata.aspect.models.graph.EdgeUrnType;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * PostgreSQL {@link com.linkedin.metadata.graph.write.GraphWriteSink} for SqlSetup pgRouting tables
 * ({@code {prefix}_vertices}, {@code {prefix}_edges}, {@code {prefix}_edge_types}); see {@code
 * datahub-upgrade/src/main/resources/sqlsetup/pgrouting/}.
 */
@Slf4j
public class PostgresGraphWriteSink extends AbstractPostgresGraphWriteSink {

  @Nonnull private final Database database;
  @Nonnull private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  @Nonnull private final GraphServiceConfiguration graphServiceConfiguration;

  public PostgresGraphWriteSink(
      @Nonnull Database database, @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties) {
    this(database, postgresSqlSetupProperties, new GraphServiceConfiguration());
  }

  public PostgresGraphWriteSink(
      @Nonnull Database database,
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties,
      @Nonnull GraphServiceConfiguration graphServiceConfiguration) {
    super(resolveGraphIdHashAlgo(postgresSqlSetupProperties));
    this.database = database;
    this.postgresSqlSetupProperties = postgresSqlSetupProperties;
    this.graphServiceConfiguration = graphServiceConfiguration;
  }

  /**
   * Effective JDBC batch chunk size: minimum of {@code postgres.pgGraph.maxEdgeWriteBatchSize} and
   * {@code graphService.maxEdgeBatchSize} when the latter is {@code > 0}; otherwise only the
   * PostgreSQL limit applies.
   */
  private int effectiveMaxEdgeWriteBatchSize() {
    int pg = postgresSqlSetupProperties.getPgGraph().getMaxEdgeWriteBatchSize();
    pg = Math.max(1, pg);
    int gs = graphServiceConfiguration.getMaxEdgeBatchSize();
    if (gs < 1) {
      return pg;
    }
    return Math.min(pg, gs);
  }

  private static String resolveGraphIdHashAlgo(
      @Nonnull PostgresSqlSetupProperties postgresSqlSetupProperties) {
    String raw = postgresSqlSetupProperties.getPgGraph().getIdHashAlgo();
    if (raw == null || raw.isBlank()) {
      return "XXHASH64";
    }
    return raw.trim();
  }

  @Override
  public void addEdge(@Nonnull Edge edge) {
    applyUpsert(edge);
  }

  @Override
  public void addEdges(@Nonnull List<Edge> edges) {
    if (edges.isEmpty()) {
      return;
    }
    for (List<Edge> chunk : Lists.partition(edges, effectiveMaxEdgeWriteBatchSize())) {
      addEdgesChunk(chunk);
    }
  }

  private void addEdgesChunk(@Nonnull List<Edge> edges) {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String vertices = schema + "." + prefix + "_vertices";
    String edgesTable = schema + "." + prefix + "_edges";
    LinkedHashMap<Long, String> vertexRows = new LinkedHashMap<>();
    for (Edge edge : edges) {
      vertexRows.put(urnVertexId(edge.getSource()), edge.getSource().toString());
      vertexRows.put(urnVertexId(edge.getDestination()), edge.getDestination().toString());
    }
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      String vertexSql =
          "INSERT INTO "
              + vertices
              + " (xxhash64_id, urn, removed, properties) VALUES (?, ?, false, '{}'::jsonb) "
              + "ON CONFLICT (xxhash64_id) DO UPDATE SET urn = EXCLUDED.urn";
      try (PreparedStatement vps = conn.prepareStatement(vertexSql)) {
        for (Map.Entry<Long, String> v : vertexRows.entrySet()) {
          vps.setLong(1, v.getKey());
          vps.setString(2, v.getValue());
          vps.addBatch();
        }
        vps.executeBatch();
      }
      String edgeSql =
          "INSERT INTO "
              + edgesTable
              + " (source_id, target_id, edge_type, owner_id, removed, created_at, updated_at, properties) "
              + "VALUES (?, ?, ?, ?, false, ?, ?, ?::jsonb) "
              + "ON CONFLICT (source_id, edge_type, target_id, owner_id) DO UPDATE SET "
              + "removed = false, updated_at = EXCLUDED.updated_at, properties = EXCLUDED.properties";
      try (PreparedStatement ps = conn.prepareStatement(edgeSql)) {
        for (Edge edge : edges) {
          long sourceId = urnVertexId(edge.getSource());
          long targetId = urnVertexId(edge.getDestination());
          long ownerId =
              edge.getLifecycleOwner() != null ? urnVertexId(edge.getLifecycleOwner()) : 0L;
          long tsSec = edgeTimestampMillis(edge) / 1000L;
          String propsJson = edgePropertiesJson(edge);
          short edgeTypeId = ensureEdgeTypeId(conn, edge.getRelationshipType());
          ps.setLong(1, sourceId);
          ps.setLong(2, targetId);
          ps.setShort(3, edgeTypeId);
          ps.setLong(4, ownerId);
          ps.setLong(5, tsSec);
          ps.setLong(6, tsSec);
          ps.setObject(7, propsJson, Types.OTHER);
          ps.addBatch();
        }
        ps.executeBatch();
      }
    } catch (SQLException e) {
      log.error("Postgres graph write addEdges batch failed: {}", e.getMessage(), e);
    }
  }

  @Override
  public void removeEdge(@Nonnull Edge edge) {
    long sourceId = urnVertexId(edge.getSource());
    long targetId = urnVertexId(edge.getDestination());
    long ownerId = edge.getLifecycleOwner() != null ? urnVertexId(edge.getLifecycleOwner()) : 0L;
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String edges = schema + "." + prefix + "_edges";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      Short edgeTypeId = lookupEdgeTypeId(conn, schema, prefix, edge.getRelationshipType());
      if (edgeTypeId == null) {
        return;
      }
      String sql =
          "DELETE FROM "
              + edges
              + " WHERE source_id = ? AND target_id = ? AND edge_type = ? AND owner_id = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setLong(1, sourceId);
        ps.setLong(2, targetId);
        ps.setShort(3, edgeTypeId);
        ps.setLong(4, ownerId);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(
          "Postgres graph write removeEdge failed ({} -> {} {}): {}",
          edge.getSource(),
          edge.getDestination(),
          edge.getRelationshipType(),
          e.getMessage(),
          e);
    }
  }

  @Override
  public void removeEdges(@Nonnull List<Edge> edges) {
    if (edges.isEmpty()) {
      return;
    }
    for (List<Edge> chunk : Lists.partition(edges, effectiveMaxEdgeWriteBatchSize())) {
      removeEdgesChunk(chunk);
    }
  }

  private void removeEdgesChunk(@Nonnull List<Edge> edges) {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String edgesTable = schema + "." + prefix + "_edges";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      String sql =
          "DELETE FROM "
              + edgesTable
              + " WHERE source_id = ? AND target_id = ? AND edge_type = ? AND owner_id = ?";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int batched = 0;
        for (Edge edge : edges) {
          long sourceId = urnVertexId(edge.getSource());
          long targetId = urnVertexId(edge.getDestination());
          long ownerId =
              edge.getLifecycleOwner() != null ? urnVertexId(edge.getLifecycleOwner()) : 0L;
          Short edgeTypeId = lookupEdgeTypeId(conn, schema, prefix, edge.getRelationshipType());
          if (edgeTypeId == null) {
            continue;
          }
          ps.setLong(1, sourceId);
          ps.setLong(2, targetId);
          ps.setShort(3, edgeTypeId);
          ps.setLong(4, ownerId);
          ps.addBatch();
          batched++;
        }
        if (batched > 0) {
          ps.executeBatch();
        }
      }
    } catch (SQLException e) {
      log.error("Postgres graph write removeEdges batch failed: {}", e.getMessage(), e);
    }
  }

  @Override
  public void removeNode(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String vertices = schema + "." + prefix + "_vertices";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps =
          conn.prepareStatement("DELETE FROM " + vertices + " WHERE urn = ?")) {
        ps.setString(1, urn.toString());
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error("Postgres graph write removeNode failed for {}: {}", urn, e.getMessage(), e);
    }
  }

  @Override
  public void removeEdgesFromNode(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull Set<String> relationshipTypes,
      @Nonnull RelationshipFilter relationshipFilter) {
    if (relationshipTypes.isEmpty()) {
      return;
    }
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String edges = schema + "." + prefix + "_edges";
    long vertexId = urnVertexId(urn);
    RelationshipDirection direction = relationshipFilter.getDirection();
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      for (String rel : relationshipTypes) {
        Short edgeTypeId = lookupEdgeTypeId(conn, schema, prefix, rel);
        if (edgeTypeId == null) {
          continue;
        }
        if (direction == RelationshipDirection.OUTGOING
            || direction == RelationshipDirection.UNDIRECTED) {
          deleteEdgesEndpoint(conn, edges, "source_id", vertexId, edgeTypeId);
        }
        if (direction == RelationshipDirection.INCOMING
            || direction == RelationshipDirection.UNDIRECTED) {
          deleteEdgesEndpoint(conn, edges, "target_id", vertexId, edgeTypeId);
        }
      }
    } catch (SQLException e) {
      log.error(
          "Postgres graph write removeEdgesFromNode failed for {}: {}", urn, e.getMessage(), e);
    }
  }

  private static void deleteEdgesEndpoint(
      Connection conn, String edgesTable, String column, long vertexId, short edgeTypeId)
      throws SQLException {
    String sql = "DELETE FROM " + edgesTable + " WHERE " + column + " = ? AND edge_type = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, vertexId);
      ps.setShort(2, edgeTypeId);
      ps.executeUpdate();
    }
  }

  @Override
  public void setEdgeStatus(
      @Nonnull Urn urn, boolean removed, @Nonnull EdgeUrnType... edgeUrnTypes) {
    if (edgeUrnTypes.length == 0) {
      return;
    }
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String vertices = schema + "." + prefix + "_vertices";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      try (PreparedStatement ps =
          conn.prepareStatement("UPDATE " + vertices + " SET removed = ? WHERE urn = ?")) {
        ps.setBoolean(1, removed);
        ps.setString(2, urn.toString());
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error("Postgres graph write setEdgeStatus failed for {}: {}", urn, e.getMessage(), e);
    }
  }

  @Override
  public void clear() {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String vertices = schema + "." + prefix + "_vertices";
    String edges = schema + "." + prefix + "_edges";
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      try (Statement st = conn.createStatement()) {
        st.executeUpdate("DELETE FROM " + edges);
        st.executeUpdate("DELETE FROM " + vertices);
      }
    } catch (SQLException e) {
      log.error("Postgres graph write clear failed: {}", e.getMessage(), e);
    }
  }

  private void applyUpsert(@Nonnull Edge edge) {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    String vertices = schema + "." + prefix + "_vertices";
    String edges = schema + "." + prefix + "_edges";
    long sourceId = urnVertexId(edge.getSource());
    long targetId = urnVertexId(edge.getDestination());
    long ownerId = edge.getLifecycleOwner() != null ? urnVertexId(edge.getLifecycleOwner()) : 0L;
    long tsMillis = edgeTimestampMillis(edge);
    long tsSec = tsMillis / 1000L;
    String propsJson = edgePropertiesJson(edge);
    try (Connection conn = database.dataSource().getConnection()) {
      conn.setAutoCommit(true);
      upsertVertex(conn, vertices, sourceId, edge.getSource().toString());
      upsertVertex(conn, vertices, targetId, edge.getDestination().toString());
      short edgeTypeId = ensureEdgeTypeId(conn, edge.getRelationshipType());
      String sql =
          "INSERT INTO "
              + edges
              + " (source_id, target_id, edge_type, owner_id, removed, created_at, updated_at, properties) "
              + "VALUES (?, ?, ?, ?, false, ?, ?, ?::jsonb) "
              + "ON CONFLICT (source_id, edge_type, target_id, owner_id) DO UPDATE SET "
              + "removed = false, updated_at = EXCLUDED.updated_at, properties = EXCLUDED.properties";
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setLong(1, sourceId);
        ps.setLong(2, targetId);
        ps.setShort(3, edgeTypeId);
        ps.setLong(4, ownerId);
        ps.setLong(5, tsSec);
        ps.setLong(6, tsSec);
        ps.setObject(7, propsJson, Types.OTHER);
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      log.error(
          "Postgres graph write addEdge failed ({} -> {} {}): {}",
          edge.getSource(),
          edge.getDestination(),
          edge.getRelationshipType(),
          e.getMessage(),
          e);
    }
  }

  private static void upsertVertex(Connection conn, String verticesTable, long id, String urn)
      throws SQLException {
    String sql =
        "INSERT INTO "
            + verticesTable
            + " (xxhash64_id, urn, removed, properties) VALUES (?, ?, false, '{}'::jsonb) "
            + "ON CONFLICT (xxhash64_id) DO UPDATE SET urn = EXCLUDED.urn";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, id);
      ps.setString(2, urn);
      ps.executeUpdate();
    }
  }

  private Short lookupEdgeTypeId(
      Connection conn, String schema, String prefix, String relationshipType) throws SQLException {
    String table = schema + "." + prefix + "_edge_types";
    String sql = "SELECT id FROM " + table + " WHERE type_name = ?";
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, relationshipType);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getShort(1);
        }
      }
    }
    return null;
  }

  private short ensureEdgeTypeId(@Nonnull Connection conn, @Nonnull String relationshipType)
      throws SQLException {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgGraphTablePrefix();
    Short existing = lookupEdgeTypeId(conn, schema, prefix, relationshipType);
    if (existing != null) {
      return existing;
    }
    short candidate = stableEdgeTypeCandidateId(relationshipType);
    for (int attempt = 0; attempt < 1024; attempt++) {
      try (PreparedStatement ps =
          conn.prepareStatement("SELECT dh_create_edge_type_if_not_exists(?, ?)")) {
        ps.setShort(1, candidate);
        ps.setString(2, relationshipType);
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) {
            throw new SQLException("dh_create_edge_type_if_not_exists returned no row");
          }
          return rs.getShort(1);
        }
      } catch (SQLException e) {
        String msg = e.getMessage();
        if (msg != null && msg.contains("already taken")) {
          candidate = nextEdgeTypeCandidate(candidate);
          continue;
        }
        throw e;
      }
    }
    throw new SQLException("Could not register edge type: " + relationshipType);
  }

  private static short nextEdgeTypeCandidate(short candidate) {
    int v = (candidate & 0xFFFF);
    v = (v % 32767) + 1;
    return (short) (v == 0 ? 1 : v);
  }
}
