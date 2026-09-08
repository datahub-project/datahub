package com.linkedin.metadata.graph.postgres;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.models.registry.LineageRegistry;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Multi-hop impact walk via {@code pgr_breadthFirstSearch} on a typed lineage network. Does not
 * join connected-component tables.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresGraphPgRoutingDao {

  public record ReachedNode(
      @Nonnull Urn urn,
      int depth,
      @Nonnull String relationshipType,
      long nodeId,
      long parentId,
      @Nullable String via,
      @Nullable String lifecycleOwner) {}

  @Nonnull private final Database database;
  @Nonnull private final PostgresGraphTables tables;
  @Nonnull private final LineageRegistry lineageRegistry;

  @Nonnull
  public List<ReachedNode> breadthFirstSearch(
      @Nonnull Urn startUrn, @Nonnull LineageGraphFilters filters, int maxHops) {
    if (maxHops < 1) {
      return List.of();
    }
    long startVid = UrnFingerprint64.ofUtf8String(startUrn.toString());
    List<PostgresGraphImpactEdgesSql.Branch> branches =
        PostgresGraphImpactEdgesSql.enumerateBranches(lineageRegistry, filters);
    String fullSql = PostgresGraphImpactEdgesSql.render(tables, branches);
    String innerSql = PostgresGraphImpactEdgesSql.idSourceTargetCostSql(fullSql);
    String sql =
        "SELECT b.depth, b.node, e.rel_type, v.urn, e.source AS parent_id, e.via,"
            + " e.lifecycle_owner FROM"
            + " pgr_breadthFirstSearch(?::text, ?::bigint, max_depth := ?::bigint, directed := TRUE)"
            + " AS b INNER JOIN ("
            + fullSql
            + ") e ON e.id = b.edge INNER JOIN "
            + tables.vertices()
            + " v ON v.xxhash64_id = b.node AND v.removed = FALSE WHERE b.node <> ? AND b.edge >="
            + " 0 ORDER BY b.depth, v.urn";

    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement exists =
            conn.prepareStatement(
                "SELECT 1 FROM "
                    + tables.vertices()
                    + " WHERE xxhash64_id = ? AND removed = FALSE");
        PreparedStatement ps = conn.prepareStatement(sql)) {
      exists.setLong(1, startVid);
      try (ResultSet rs = exists.executeQuery()) {
        if (!rs.next()) {
          return List.of();
        }
      }
      ps.setString(1, innerSql);
      ps.setLong(2, startVid);
      ps.setLong(3, maxHops);
      ps.setLong(4, startVid);
      List<ReachedNode> out = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String urnStr = rs.getString("urn");
          try {
            out.add(
                new ReachedNode(
                    Urn.createFromString(urnStr),
                    rs.getInt("depth"),
                    rs.getString("rel_type"),
                    rs.getLong("node"),
                    rs.getLong("parent_id"),
                    blankToNull(rs.getString("via")),
                    blankToNull(rs.getString("lifecycle_owner"))));
          } catch (java.net.URISyntaxException e) {
            log.debug("Skipping impact node with bad URN: {}", urnStr);
          }
        }
      }
      return out;
    } catch (SQLException e) {
      throw new IllegalStateException("Postgres graph pgr_breadthFirstSearch failed", e);
    }
  }

  @Nullable
  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
