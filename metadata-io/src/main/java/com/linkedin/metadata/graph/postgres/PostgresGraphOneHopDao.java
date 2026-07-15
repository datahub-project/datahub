package com.linkedin.metadata.graph.postgres;

import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_DESTINATION;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_RELNSHIP_TYPE;
import static com.linkedin.metadata.aspect.models.graph.Edge.EDGE_FIELD_SOURCE;

import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** One-hop graph reads (joins on vertices/edges; no pgRouting). */
@Slf4j
@RequiredArgsConstructor
public class PostgresGraphOneHopDao {

  /**
   * One row for lineage BFS; exposes optional lifecycle owner for paths without duplicating via.
   */
  public record LineageHopRow(@Nonnull RelatedEntity related, @Nullable String lifecycleOwnerUrn) {}

  @Nonnull private final Database database;
  @Nonnull private final PostgresGraphTables tables;

  /**
   * Same edges as {@link #findRelatedEntities} but includes lifecycle owner from edge JSON for
   * multi-hop paths; {@link RelatedEntity#getVia()} stays null when only lifecycle distinguishes
   * parallel edges.
   */
  @Nonnull
  public List<LineageHopRow> findRelatedForLineage(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      final int offset,
      @Nullable final Integer count) {
    if (graphFilters.getRelationshipTypes().isEmpty() || graphFilters.noResultsByType()) {
      return Collections.emptyList();
    }
    RelationshipDirection dir = graphFilters.getRelationshipDirection();
    if (dir == RelationshipDirection.UNDIRECTED) {
      throw new UnsupportedOperationException(
          "Postgres lineage hop does not support UNDIRECTED filters");
    }
    AnchorAlias anchor = anchorAliases(dir);
    List<Object> params = new ArrayList<>();
    String sql =
        buildSelectSimple(graphFilters, anchor, params, offset, count != null ? count : 100000);

    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      bindAll(ps, params);
      List<LineageHopRow> rows = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          rows.add(lineageHopFromRow(rs));
        }
      }
      return rows;
    } catch (SQLException e) {
      throw new IllegalStateException("Postgres graph findRelatedForLineage failed", e);
    }
  }

  @Nonnull
  public RelatedEntitiesResult findRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      final int offset,
      @Nullable final Integer count) {
    if (graphFilters.getRelationshipTypes().isEmpty() || graphFilters.noResultsByType()) {
      return new RelatedEntitiesResult(offset, 0, 0, Collections.emptyList());
    }

    RelationshipDirection dir = graphFilters.getRelationshipDirection();
    if (dir == RelationshipDirection.UNDIRECTED) {
      return findRelatedUndirected(graphFilters, offset, count);
    }

    AnchorAlias anchor = anchorAliases(dir);
    List<Object> params = new ArrayList<>();
    String sql =
        buildSelectSimple(graphFilters, anchor, params, offset, count != null ? count : 100000);
    List<Object> countParams = new ArrayList<>();
    String countSql = buildCount(graphFilters, anchor, countParams);

    try (Connection conn = database.dataSource().getConnection()) {
      int total = queryInt(conn, countSql, countParams);
      List<RelatedEntity> entities = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        bindAll(ps, params);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            entities.add(relatedEntityFromRow(rs));
          }
        }
      }
      return new RelatedEntitiesResult(offset, entities.size(), total, entities);
    } catch (SQLException e) {
      throw new IllegalStateException("Postgres graph findRelatedEntities failed", e);
    }
  }

  private static RelatedEntity relatedEntityFromRow(ResultSet rs) throws SQLException {
    String rel = rs.getString(1);
    String urn = rs.getString(2);
    String via = rs.getString(3);
    if (rs.wasNull()) {
      via = null;
    }
    return new RelatedEntity(rel, urn, via);
  }

  private static LineageHopRow lineageHopFromRow(ResultSet rs) throws SQLException {
    RelatedEntity related = relatedEntityFromRow(rs);
    String lifecycle = rs.getString(4);
    if (rs.wasNull()) {
      lifecycle = null;
    }
    return new LineageHopRow(related, lifecycle);
  }

  private RelatedEntitiesResult findRelatedUndirected(
      GraphFilters graphFilters, int offset, Integer count) {
    GraphFilters out =
        new GraphFilters(
            graphFilters.getSourceEntityFilter(),
            graphFilters.getDestinationEntityFilter(),
            graphFilters.getSourceTypes(),
            graphFilters.getDestinationTypes(),
            graphFilters.getRelationshipTypes(),
            new RelationshipFilter().setDirection(RelationshipDirection.OUTGOING));
    GraphFilters in =
        new GraphFilters(
            graphFilters.getSourceEntityFilter(),
            graphFilters.getDestinationEntityFilter(),
            graphFilters.getSourceTypes(),
            graphFilters.getDestinationTypes(),
            graphFilters.getRelationshipTypes(),
            new RelationshipFilter().setDirection(RelationshipDirection.INCOMING));

    List<Object> pUnion = new ArrayList<>();
    String inner =
        "SELECT r.rel_type, r.related_urn, r.via_urn FROM ("
            + unionBranch(out, anchorAliases(RelationshipDirection.OUTGOING), "o")
            + " UNION "
            + unionBranch(in, anchorAliases(RelationshipDirection.INCOMING), "i")
            + ") r";

    String sql =
        "SELECT DISTINCT ON (x.related_urn, x.rel_type, COALESCE(x.via_urn, '')) x.rel_type,"
            + " x.related_urn, x.via_urn FROM ("
            + inner
            + ") x ORDER BY x.related_urn, x.rel_type, COALESCE(x.via_urn, '') LIMIT ? OFFSET ?";
    pUnion.addAll(collectParams(out, anchorAliases(RelationshipDirection.OUTGOING)));
    pUnion.addAll(collectParams(in, anchorAliases(RelationshipDirection.INCOMING)));
    pUnion.add(count != null ? count : 100000);
    pUnion.add(offset);

    String countSql =
        "SELECT COUNT(*) FROM (SELECT DISTINCT r.rel_type, r.related_urn, COALESCE(r.via_urn, '')"
            + " FROM ("
            + unionBranch(out, anchorAliases(RelationshipDirection.OUTGOING), "o")
            + " UNION "
            + unionBranch(in, anchorAliases(RelationshipDirection.INCOMING), "i")
            + ") r) s";

    List<Object> pCount = new ArrayList<>();
    pCount.addAll(collectParams(out, anchorAliases(RelationshipDirection.OUTGOING)));
    pCount.addAll(collectParams(in, anchorAliases(RelationshipDirection.INCOMING)));

    try (Connection conn = database.dataSource().getConnection()) {
      int total = queryInt(conn, countSql, pCount);
      List<RelatedEntity> entities = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        bindAll(ps, pUnion);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            entities.add(relatedEntityFromRow(rs));
          }
        }
      }
      return new RelatedEntitiesResult(offset, entities.size(), total, entities);
    } catch (SQLException e) {
      throw new IllegalStateException("Postgres graph undirected findRelatedEntities failed", e);
    }
  }

  /** SELECT et.type_name AS rel_type, <related>.urn AS related_urn FROM ... (no ORDER/LIMIT). */
  private String unionBranch(GraphFilters gf, AnchorAlias anchor, String subAlias) {
    List<Object> tmp = new ArrayList<>();
    String core = buildInnerSelectCore(gf, anchor, tmp);
    return "SELECT "
        + subAlias
        + ".rel_type, "
        + subAlias
        + ".related_urn, "
        + subAlias
        + ".via_urn FROM ("
        + core
        + ") "
        + subAlias;
  }

  private List<Object> collectParams(GraphFilters gf, AnchorAlias anchor) {
    List<Object> tmp = new ArrayList<>();
    buildInnerSelectCore(gf, anchor, tmp);
    return tmp;
  }

  private String buildInnerSelectCore(GraphFilters gf, AnchorAlias anchor, List<Object> params) {
    String vs = "vs";
    String vt = "vt";
    String etn = "et";
    String anchorAlias = anchor.anchorUrnFilterAlias;
    String oppositeAlias = anchor.oppositeUrnFilterAlias;

    String sourceFilter =
        PostgresGraphFilterSql.vertexFilterSql(
            gf.getSourceEntityFilter(), anchorAlias + ".urn", params);
    String destFilter =
        PostgresGraphFilterSql.vertexFilterSql(
            gf.getDestinationEntityFilter(), oppositeAlias + ".urn", params);

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ")
        .append(etn)
        .append(".type_name AS rel_type, ")
        .append(oppositeAlias)
        .append(".urn AS related_urn, ")
        .append("NULLIF(TRIM(e.properties->>'via'), '') AS via_urn FROM ")
        .append(tables.edges())
        .append(" e JOIN ")
        .append(tables.vertices())
        .append(" vs ON e.source_id = vs.xxhash64_id JOIN ")
        .append(tables.vertices())
        .append(" vt ON e.target_id = vt.xxhash64_id JOIN ")
        .append(tables.edgeTypes())
        .append(" ")
        .append(etn)
        .append(" ON e.edge_type = ")
        .append(etn)
        .append(".id WHERE e.removed = FALSE AND vs.removed = FALSE AND vt.removed = FALSE AND ");
    sb.append(sourceFilter).append(" AND ").append(destFilter);
    appendRelationshipTypes(gf, sb, params);
    appendOptionalEntityTypes(gf, anchor, sb, params);
    return sb.toString();
  }

  @Nonnull
  public RelatedEntitiesScrollResult scrollRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull GraphFilters graphFilters,
      @Nullable String scrollId,
      @Nullable Integer count) {
    if (graphFilters.getRelationshipTypes().isEmpty() || graphFilters.noResultsByType()) {
      return RelatedEntitiesScrollResult.builder()
          .entities(Collections.emptyList())
          .pageSize(0)
          .numResults(0)
          .scrollId(null)
          .build();
    }
    int offset = 0;
    if (scrollId != null) {
      offset = Integer.parseInt(SearchAfterWrapper.fromScrollId(scrollId).getPitId());
    }
    int limit = count != null ? count : 100;
    RelationshipDirection dir = graphFilters.getRelationshipDirection();
    if (dir == RelationshipDirection.UNDIRECTED) {
      dir = RelationshipDirection.OUTGOING;
    }
    AnchorAlias anchor = anchorAliases(dir);
    List<Object> params = new ArrayList<>();
    String sql = buildSelectScroll(graphFilters, anchor, params, offset, limit);
    List<Object> countParams = new ArrayList<>();
    String countSql = buildCount(graphFilters, anchor, countParams);

    try (Connection conn = database.dataSource().getConnection()) {
      int total = queryInt(conn, countSql, countParams);
      List<RelatedEntities> entities = new ArrayList<>();
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        bindAll(ps, params);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String rel = rs.getString(1);
            String relatedUrn = rs.getString(2);
            String anchorUrn = rs.getString(3);
            String viaStr = rs.getString(4);
            if (rs.wasNull()) {
              viaStr = null;
            }
            if (dir == RelationshipDirection.OUTGOING) {
              entities.add(new RelatedEntities(rel, anchorUrn, relatedUrn, dir, viaStr));
            } else {
              entities.add(new RelatedEntities(rel, relatedUrn, anchorUrn, dir, viaStr));
            }
          }
        }
      }
      String nextScroll = null;
      if (entities.size() == limit) {
        nextScroll =
            new SearchAfterWrapper(null, Integer.toString(offset + limit), 0L).toScrollId();
      }
      return RelatedEntitiesScrollResult.builder()
          .entities(entities)
          .pageSize(entities.size())
          .numResults(total)
          .scrollId(nextScroll)
          .build();
    } catch (SQLException e) {
      throw new IllegalStateException("Postgres graph scrollRelatedEntities failed", e);
    }
  }

  @Nonnull
  public List<Map<String, Object>> raw(@Nonnull List<GraphService.EdgeTuple> edgeTuples) {
    if (edgeTuples.isEmpty()) {
      return Collections.emptyList();
    }
    String vs = "vs";
    String vt = "vt";
    String etn = "et";
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ")
        .append(vs)
        .append(".urn AS surn, ")
        .append(vt)
        .append(".urn AS turn, ")
        .append(etn)
        .append(".type_name AS rel_type ")
        .append("FROM ")
        .append(tables.edges())
        .append(" e JOIN ")
        .append(tables.vertices())
        .append(" ")
        .append(vs)
        .append(" ON e.source_id = ")
        .append(vs)
        .append(".xxhash64_id JOIN ")
        .append(tables.vertices())
        .append(" ")
        .append(vt)
        .append(" ON e.target_id = ")
        .append(vt)
        .append(".xxhash64_id JOIN ")
        .append(tables.edgeTypes())
        .append(" ")
        .append(etn)
        .append(" ON e.edge_type = ")
        .append(etn)
        .append(".id WHERE e.removed = FALSE AND ")
        .append(vs)
        .append(".removed = FALSE AND ")
        .append(vt)
        .append(".removed = FALSE AND (");
    List<Object> params = new ArrayList<>();
    int clauses = 0;
    for (GraphService.EdgeTuple t : edgeTuples) {
      if (t.getA() == null || t.getB() == null || t.getRelationshipType() == null) {
        continue;
      }
      if (clauses > 0) {
        sb.append(" OR ");
      }
      clauses++;
      sb.append("(")
          .append(etn)
          .append(".type_name = ? AND ((")
          .append(vs)
          .append(".urn = ? AND ")
          .append(vt)
          .append(".urn = ?) OR (")
          .append(vs)
          .append(".urn = ? AND ")
          .append(vt)
          .append(".urn = ?)))");
      params.add(t.getRelationshipType());
      params.add(t.getA());
      params.add(t.getB());
      params.add(t.getB());
      params.add(t.getA());
    }
    sb.append(")");
    if (clauses == 0) {
      return Collections.emptyList();
    }

    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement(sb.toString())) {
      bindAll(ps, params);
      List<Map<String, Object>> out = new ArrayList<>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Map<String, Object> source = new HashMap<>();
          source.put("urn", rs.getString("surn"));
          Map<String, Object> dest = new HashMap<>();
          dest.put("urn", rs.getString("turn"));
          Map<String, Object> doc = new HashMap<>();
          doc.put(EDGE_FIELD_SOURCE, source);
          doc.put(EDGE_FIELD_DESTINATION, dest);
          doc.put(EDGE_FIELD_RELNSHIP_TYPE, rs.getString("rel_type"));
          out.add(doc);
        }
      }
      return out;
    } catch (SQLException e) {
      throw new IllegalStateException("Postgres graph raw failed", e);
    }
  }

  private static final class AnchorAlias {
    final String anchorUrnFilterAlias;
    final String oppositeUrnFilterAlias;

    AnchorAlias(String anchorUrnFilterAlias, String oppositeUrnFilterAlias) {
      this.anchorUrnFilterAlias = anchorUrnFilterAlias;
      this.oppositeUrnFilterAlias = oppositeUrnFilterAlias;
    }
  }

  private static AnchorAlias anchorAliases(RelationshipDirection dir) {
    if (dir == RelationshipDirection.OUTGOING) {
      return new AnchorAlias("vs", "vt");
    }
    return new AnchorAlias("vt", "vs");
  }

  private String buildSelectSimple(
      GraphFilters graphFilters, AnchorAlias anchor, List<Object> params, int offset, int limit) {
    String oppositeAlias = anchor.oppositeUrnFilterAlias;
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT DISTINCT ON (")
        .append(oppositeAlias)
        .append(".urn, et.type_name, COALESCE(e.properties->>'via',''), e.owner_id) ")
        .append("et.type_name, ")
        .append(oppositeAlias)
        .append(".urn, ")
        .append("NULLIF(TRIM(e.properties->>'via'), '') AS via, ")
        .append("NULLIF(TRIM(e.properties->>'lifecycleOwner'), '') AS lifecycle_owner FROM ");
    appendJoinBody(graphFilters, anchor, sb, params);
    sb.append(" ORDER BY ")
        .append(oppositeAlias)
        .append(".urn, et.type_name, COALESCE(e.properties->>'via',''), e.owner_id");
    sb.append(" LIMIT ? OFFSET ?");
    params.add(limit);
    params.add(offset);
    return sb.toString();
  }

  private String buildSelectScroll(
      GraphFilters graphFilters, AnchorAlias anchor, List<Object> params, int offset, int limit) {
    String anchorAlias = anchor.anchorUrnFilterAlias;
    String oppositeAlias = anchor.oppositeUrnFilterAlias;
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT DISTINCT ON (")
        .append(anchorAlias)
        .append(".urn, ")
        .append(oppositeAlias)
        .append(".urn, et.type_name, COALESCE(e.properties->>'via','')) et.type_name, ")
        .append(oppositeAlias)
        .append(".urn, ")
        .append(anchorAlias)
        .append(".urn AS anchor_urn, ")
        .append("NULLIF(TRIM(e.properties->>'via'), '') AS via_col FROM ");
    appendJoinBody(graphFilters, anchor, sb, params);
    sb.append(" ORDER BY ")
        .append(anchorAlias)
        .append(".urn, ")
        .append(oppositeAlias)
        .append(".urn, et.type_name, COALESCE(e.properties->>'via','')");
    sb.append(" LIMIT ? OFFSET ?");
    params.add(limit);
    params.add(offset);
    return sb.toString();
  }

  private void appendJoinBody(
      GraphFilters graphFilters, AnchorAlias anchor, StringBuilder sb, List<Object> params) {
    String anchorAlias = anchor.anchorUrnFilterAlias;
    String oppositeAlias = anchor.oppositeUrnFilterAlias;
    String sourceFilter =
        PostgresGraphFilterSql.vertexFilterSql(
            graphFilters.getSourceEntityFilter(), anchorAlias + ".urn", params);
    String destFilter =
        PostgresGraphFilterSql.vertexFilterSql(
            graphFilters.getDestinationEntityFilter(), oppositeAlias + ".urn", params);

    sb.append(tables.edges())
        .append(" e JOIN ")
        .append(tables.vertices())
        .append(" vs ON e.source_id = vs.xxhash64_id JOIN ")
        .append(tables.vertices())
        .append(" vt ON e.target_id = vt.xxhash64_id JOIN ")
        .append(tables.edgeTypes())
        .append(
            " et ON e.edge_type = et.id WHERE e.removed = FALSE AND vs.removed = FALSE AND vt.removed = FALSE AND ");
    sb.append(sourceFilter).append(" AND ").append(destFilter);
    appendRelationshipTypes(graphFilters, sb, params);
    appendOptionalEntityTypes(graphFilters, anchor, sb, params);
  }

  private String buildCount(GraphFilters graphFilters, AnchorAlias anchor, List<Object> params) {
    String oppositeAlias = anchor.oppositeUrnFilterAlias;
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT COUNT(*) FROM (SELECT DISTINCT et.type_name, ")
        .append(oppositeAlias)
        .append(".urn, COALESCE(e.properties->>'via',''), e.owner_id FROM ");
    appendJoinBody(graphFilters, anchor, sb, params);
    sb.append(") sub");
    return sb.toString();
  }

  private static void appendRelationshipTypes(
      GraphFilters graphFilters, StringBuilder sb, List<Object> params) {
    sb.append(" AND et.type_name IN (");
    List<String> ordered = graphFilters.getRelationshipTypesOrdered();
    for (int i = 0; i < ordered.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("?");
      params.add(ordered.get(i));
    }
    sb.append(")");
  }

  private void appendOptionalEntityTypes(
      GraphFilters graphFilters, AnchorAlias anchor, StringBuilder sb, List<Object> params) {
    if (graphFilters.isSourceTypesFilterEnabled()) {
      sb.append(" AND ")
          .append(PostgresGraphFilterSql.entityTypeExpr(anchor.anchorUrnFilterAlias + ".urn"))
          .append(" IN (");
      appendStrings(graphFilters.getSourceTypes(), sb, params);
      sb.append(")");
    }
    if (graphFilters.isDestinationTypesFilterEnabled()) {
      sb.append(" AND ")
          .append(PostgresGraphFilterSql.entityTypeExpr(anchor.oppositeUrnFilterAlias + ".urn"))
          .append(" IN (");
      appendStrings(graphFilters.getDestinationTypes(), sb, params);
      sb.append(")");
    }
  }

  private static void appendStrings(
      java.util.Set<String> vals, StringBuilder sb, List<Object> params) {
    List<String> sorted = new ArrayList<>(vals);
    Collections.sort(sorted);
    for (int i = 0; i < sorted.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append("?");
      params.add(sorted.get(i));
    }
  }

  private static void bindAll(PreparedStatement ps, List<Object> params) throws SQLException {
    for (int i = 0; i < params.size(); i++) {
      ps.setObject(i + 1, params.get(i));
    }
  }

  private static int queryInt(Connection conn, String sql, List<Object> params)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      bindAll(ps, params);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getInt(1);
        }
      }
    }
    return 0;
  }
}
