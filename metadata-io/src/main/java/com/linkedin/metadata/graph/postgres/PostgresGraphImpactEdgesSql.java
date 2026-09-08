package com.linkedin.metadata.graph.postgres;

import static com.linkedin.metadata.Constants.SCHEMA_FIELD_ENTITY_NAME;

import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Builds a directed pgRouting edges SQL fragment from {@link LineageRegistry} triplets (entity
 * type, relationship, opposing type, orientation). Does not use connected-component tables or
 * {@code reverse_cost}.
 */
public final class PostgresGraphImpactEdgesSql {

  public record Branch(
      @Nonnull String currentEntityType,
      @Nonnull String relationshipType,
      @Nonnull String opposingEntityType,
      boolean outgoing) {}

  private PostgresGraphImpactEdgesSql() {}

  @Nonnull
  public static List<Branch> enumerateBranches(
      @Nonnull LineageRegistry lineageRegistry, @Nonnull LineageGraphFilters filters) {
    Set<String> entityTypes = new LinkedHashSet<>(lineageRegistry.getLineageSpecs().keySet());
    entityTypes.addAll(filters.getEdgesPerEntityType().keySet());
    if (filters.getAllowedEntityTypes() != null) {
      entityTypes.addAll(filters.getAllowedEntityTypes());
    }
    // schemaField lineage edges are synthesized in LineageRegistry.getLineageRelationships and are
    // omitted from getLineageSpecs() because the stored LineageSpec is empty.
    if (lineageRegistry.getLineageSpec(SCHEMA_FIELD_ENTITY_NAME) != null) {
      entityTypes.add(SCHEMA_FIELD_ENTITY_NAME);
    }
    LinkedHashSet<Branch> branches = new LinkedHashSet<>();
    for (String entityType : entityTypes) {
      for (LineageRegistry.EdgeInfo edgeInfo : filters.getEdgeInfo(lineageRegistry, entityType)) {
        for (boolean outgoing : orientations(edgeInfo.getDirection())) {
          branches.add(
              new Branch(
                  entityType.toLowerCase(Locale.ROOT),
                  edgeInfo.getType(),
                  edgeInfo.getOpposingEntityType().toLowerCase(Locale.ROOT),
                  outgoing));
        }
      }
    }
    return new ArrayList<>(branches);
  }

  @Nonnull
  public static String render(@Nonnull PostgresGraphTables tables, @Nonnull List<Branch> branches) {
    if (branches.isEmpty()) {
      return emptySelect();
    }
    List<String> parts = new ArrayList<>(branches.size());
    for (Branch branch : branches) {
      parts.add(renderBranch(tables, branch));
    }
    return String.join(" UNION ALL ", parts);
  }

  @Nonnull
  public static String idSourceTargetCostSql(@Nonnull String fullEdgesSql) {
    return "SELECT id, source, target, cost FROM (" + fullEdgesSql + ") impact_edges";
  }

  private static List<Boolean> orientations(RelationshipDirection direction) {
    if (direction == RelationshipDirection.UNDIRECTED) {
      return List.of(true, false);
    }
    if (direction == RelationshipDirection.OUTGOING) {
      return List.of(true);
    }
    if (direction == RelationshipDirection.INCOMING) {
      return List.of(false);
    }
    return List.of();
  }

  private static String renderBranch(PostgresGraphTables tables, Branch branch) {
    String orientation = branch.outgoing() ? "fwd" : "rev";
    String currentAlias = branch.outgoing() ? "vs" : "vt";
    String opposingAlias = branch.outgoing() ? "vt" : "vs";
    String sourceExpr = branch.outgoing() ? "e.source_id" : "e.target_id";
    String targetExpr = branch.outgoing() ? "e.target_id" : "e.source_id";
    return "SELECT (abs(('x' || substr(md5(e.source_id::text || '|' || e.target_id::text || '|' ||"
        + " e.edge_type::text || '|' || e.owner_id::text || '|' || "
        + sqlStringLiteral(orientation)
        + "), 1, 16))::bit(64)::bigint)) AS id, "
        + sourceExpr
        + " AS source, "
        + targetExpr
        + " AS target, 1.0::double precision AS cost, et.type_name AS rel_type,"
        + " NULLIF(TRIM(e.properties->>'via'), '') AS via,"
        + " NULLIF(TRIM(e.properties->>'lifecycleOwner'), '') AS lifecycle_owner FROM "
        + tables.edges()
        + " e JOIN "
        + tables.vertices()
        + " vs ON e.source_id = vs.xxhash64_id JOIN "
        + tables.vertices()
        + " vt ON e.target_id = vt.xxhash64_id JOIN "
        + tables.edgeTypes()
        + " et ON e.edge_type = et.id WHERE e.removed = FALSE AND vs.removed = FALSE AND"
        + " vt.removed = FALSE AND et.type_name = "
        + sqlStringLiteral(branch.relationshipType())
        + " AND "
        + PostgresGraphFilterSql.entityTypeExpr(currentAlias + ".urn")
        + " = "
        + sqlStringLiteral(branch.currentEntityType())
        + " AND "
        + PostgresGraphFilterSql.entityTypeExpr(opposingAlias + ".urn")
        + " = "
        + sqlStringLiteral(branch.opposingEntityType());
  }

  private static String emptySelect() {
    return "SELECT CAST(NULL AS bigint) AS id, CAST(NULL AS bigint) AS source, CAST(NULL AS"
        + " bigint) AS target, CAST(NULL AS double precision) AS cost, CAST(NULL AS varchar)"
        + " AS rel_type, CAST(NULL AS varchar) AS via, CAST(NULL AS varchar) AS"
        + " lifecycle_owner WHERE FALSE";
  }

  static String sqlStringLiteral(@Nonnull String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
