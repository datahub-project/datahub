package com.linkedin.metadata.graph.postgres;

import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;
import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;

import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.graph.GraphServiceConfiguration;
import com.linkedin.metadata.graph.EntityLineageResult;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.LineageGraphFilters;
import com.linkedin.metadata.graph.LineageRelationship;
import com.linkedin.metadata.graph.LineageRelationshipArray;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Multi-hop lineage using breadth-first expansion aligned with {@link
 * com.linkedin.metadata.graph.elastic.GraphQueryBaseDAO}: each hop applies {@link GraphFilters} per
 * {@link LineageRegistry.EdgeInfo} direction (OUTGOING vs INCOMING), matching how Elasticsearch
 * lineage resolves edges.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresGraphLineageDao {

  @Nonnull private final PostgresGraphOneHopDao oneHopDao;
  @Nonnull private final LineageRegistry lineageRegistry;
  @Nonnull private final GraphServiceConfiguration graphServiceConfig;

  @Nonnull
  public EntityLineageResult getLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int offset,
      @Nullable Integer count,
      int maxHops) {

    count = ConfigUtils.applyLimit(graphServiceConfig, count);
    if (maxHops < 1) {
      return emptyLineage(offset, count);
    }

    Map<String, LineageRelationship> byRowKey = new LinkedHashMap<>();
    Map<Urn, Urn> treeParent = new HashMap<>();

    List<Urn> frontier = new ArrayList<>();
    frontier.add(entityUrn);

    SearchContext searchContext = opContext.getSearchContext();
    Integer perHopLimit =
        searchContext != null && searchContext.getLineageFlags() != null
            ? searchContext.getLineageFlags().getEntitiesExploredPerHopLimit()
            : null;

    for (int hop = 1; hop <= maxHops; hop++) {
      Set<Urn> nextFrontier = new LinkedHashSet<>();
      for (Urn u : frontier) {
        Set<Urn> distinctDestinationsFromU = new HashSet<>();
        Set<LineageRegistry.EdgeInfo> edgeInfos = lineageEdgesForHop(u, lineageGraphFilters);
        if (edgeInfos.isEmpty()) {
          continue;
        }
        Map<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>> groups =
            groupLineageEdges(edgeInfos);
        hop_groups:
        for (Map.Entry<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>> e :
            groups.entrySet()) {
          String relType = e.getKey().getLeft();
          RelationshipDirection direction = e.getKey().getRight();
          if (direction == RelationshipDirection.UNDIRECTED) {
            continue;
          }
          Set<String> opposingTypes =
              e.getValue().stream()
                  .map(LineageRegistry.EdgeInfo::getOpposingEntityType)
                  .collect(Collectors.toSet());
          GraphFilters gf = buildLineageHopGraphFilters(u, relType, direction, opposingTypes);
          if (gf == null) {
            continue;
          }
          List<PostgresGraphOneHopDao.LineageHopRow> hopRows =
              oneHopDao.findRelatedForLineage(opContext, gf, 0, null);
          for (PostgresGraphOneHopDao.LineageHopRow hopRow : hopRows) {
            RelatedEntity re = hopRow.related();
            String lifecycleUrn = hopRow.lifecycleOwnerUrn();
            Urn v;
            try {
              v = Urn.createFromString(re.getUrn());
            } catch (URISyntaxException ex) {
              log.debug("Skipping lineage hop with bad URN: {}", re.getUrn());
              continue;
            }
            if (v.equals(entityUrn)) {
              continue;
            }

            String rowKey = lineageRowKey(re.getRelationshipType(), v, re, lifecycleUrn);
            LineageRelationship existing = byRowKey.get(rowKey);
            if (existing != null) {
              mergeAdditionalHop(existing, hop);
              if (!distinctDestinationsFromU.contains(v)) {
                if (perHopLimit != null && distinctDestinationsFromU.size() >= perHopLimit) {
                  break hop_groups;
                }
                distinctDestinationsFromU.add(v);
              }
              continue;
            }

            if (!distinctDestinationsFromU.contains(v)) {
              if (perHopLimit != null && distinctDestinationsFromU.size() >= perHopLimit) {
                break hop_groups;
              }
              distinctDestinationsFromU.add(v);
            }

            if (!treeParent.containsKey(v)) {
              treeParent.put(v, u);
            }

            try {
              UrnArray path =
                  hop == 1
                      ? buildFirstHopPath(entityUrn, v, re, lifecycleUrn)
                      : buildPath(entityUrn, v, treeParent);
              IntegerArray degrees = new IntegerArray();
              degrees.add(hop);
              LineageRelationship lr =
                  new LineageRelationship()
                      .setEntity(v)
                      .setType(re.getRelationshipType())
                      .setDegree(hop)
                      .setDegrees(degrees)
                      .setPaths(new UrnArrayArray(List.of(path)));
              byRowKey.put(rowKey, lr);
              nextFrontier.add(v);
            } catch (URISyntaxException ex) {
              log.debug("Lineage path build failed: {}", ex.getMessage());
            }
          }
        }
      }
      if (nextFrontier.isEmpty()) {
        break;
      }
      frontier = new ArrayList<>(nextFrontier);
    }

    List<LineageRelationship> relationships = finalizeLineageRows(byRowKey);
    relationships.sort(
        java.util.Comparator.comparingInt(
                (LineageRelationship lr) ->
                    lr.getDegree() != null ? lr.getDegree() : Integer.MAX_VALUE)
            .thenComparing(PostgresGraphLineageDao::tieBreakSecondPathUrn));

    int total = relationships.size();
    int from = Math.min(offset, relationships.size());
    int to = count != null ? Math.min(from + count, relationships.size()) : relationships.size();
    List<LineageRelationship> page = relationships.subList(from, to);

    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray(page))
        .setStart(offset)
        .setCount(page.size())
        .setTotal(total)
        .setPartial(false);
  }

  /**
   * Uses the same edge selection as Elasticsearch lineage ({@link
   * LineageGraphFilters#getEdgeInfo(LineageRegistry, String)}).
   */
  private Set<LineageRegistry.EdgeInfo> lineageEdgesForHop(
      Urn u, LineageGraphFilters lineageGraphFilters) {
    return lineageGraphFilters.getEdgeInfo(lineageRegistry, u.getEntityType());
  }

  private static Map<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>>
      groupLineageEdges(Set<LineageRegistry.EdgeInfo> edgeInfos) {
    Map<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>> map = new HashMap<>();
    for (LineageRegistry.EdgeInfo ei : edgeInfos) {
      if (ei.getDirection() == RelationshipDirection.UNDIRECTED) {
        addToGroup(map, ei, Pair.of(ei.getType(), RelationshipDirection.INCOMING));
        addToGroup(map, ei, Pair.of(ei.getType(), RelationshipDirection.OUTGOING));
      } else {
        addToGroup(map, ei, Pair.of(ei.getType(), ei.getDirection()));
      }
    }
    return map;
  }

  private static void addToGroup(
      Map<Pair<String, RelationshipDirection>, List<LineageRegistry.EdgeInfo>> map,
      LineageRegistry.EdgeInfo ei,
      Pair<String, RelationshipDirection> key) {
    map.computeIfAbsent(key, k -> new ArrayList<>()).add(ei);
  }

  @Nullable
  private GraphFilters buildLineageHopGraphFilters(
      Urn anchorUrn,
      String relationshipType,
      RelationshipDirection direction,
      Set<String> opposingEntityTypes) {

    Set<String> opposing = lowercaseEntityTypes(opposingEntityTypes);
    if (opposing.isEmpty()) {
      return null;
    }
    RelationshipFilter rf = new RelationshipFilter().setDirection(direction);
    return new GraphFilters(
        newFilter("urn", anchorUrn.toString()),
        EMPTY_FILTER,
        null,
        opposing,
        Set.of(relationshipType),
        rf);
  }

  private static Set<String> lowercaseEntityTypes(Set<String> types) {
    Set<String> out = new HashSet<>();
    for (String t : types) {
      if (t != null) {
        out.add(t.toLowerCase());
      }
    }
    return out;
  }

  @Nonnull
  public EntityLineageResult getImpactLineage(
      @Nonnull OperationContext opContext,
      @Nonnull Urn entityUrn,
      @Nonnull LineageGraphFilters lineageGraphFilters,
      int maxHops) {
    return getLineage(opContext, entityUrn, lineageGraphFilters, 0, null, maxHops);
  }

  /**
   * Stable ordering when degrees tie (matches ES/Neo4j: explicit via paths before lifecycle-only).
   */
  private static String tieBreakSecondPathUrn(LineageRelationship lr) {
    if (!lr.hasPaths() || lr.getPaths().isEmpty()) {
      return "";
    }
    UrnArray p = lr.getPaths().get(0);
    if (p.size() < 2) {
      return "";
    }
    return p.get(1).toString();
  }

  private EntityLineageResult emptyLineage(int offset, @Nullable Integer count) {
    return new EntityLineageResult()
        .setRelationships(new LineageRelationshipArray())
        .setStart(offset)
        .setCount(0)
        .setTotal(0)
        .setPartial(false);
  }

  /**
   * Merges rows that share the same end entity when edges differ only by relationship type (empty
   * via). Keeps separate rows when {@code via} differs (parallel lifecycle edges).
   */
  private static List<LineageRelationship> finalizeLineageRows(
      Map<String, LineageRelationship> byRowKey) {
    Map<Urn, List<String>> keysByEntity = new HashMap<>();
    for (String rowKey : byRowKey.keySet()) {
      Urn entity;
      try {
        entity = rowKeyEntity(rowKey);
      } catch (URISyntaxException e) {
        continue;
      }
      keysByEntity.computeIfAbsent(entity, k -> new ArrayList<>()).add(rowKey);
    }

    List<LineageRelationship> out = new ArrayList<>();
    for (List<String> keys : keysByEntity.values()) {
      if (keys.size() == 1) {
        out.add(byRowKey.get(keys.get(0)));
        continue;
      }
      Set<String> distinctDisambig =
          keys.stream().map(PostgresGraphLineageDao::rowKeyDisambig).collect(Collectors.toSet());
      boolean allDisambigEmpty =
          distinctDisambig.size() == 1 && distinctDisambig.iterator().next().isEmpty();
      if (allDisambigEmpty) {
        LineageRelationship merged = byRowKey.get(keys.get(0));
        for (int i = 1; i < keys.size(); i++) {
          merged = mergeLineageRelationships(merged, byRowKey.get(keys.get(i)));
        }
        out.add(merged);
      } else {
        for (String k : keys) {
          out.add(byRowKey.get(k));
        }
      }
    }
    return out;
  }

  private static Urn rowKeyEntity(String rowKey) throws URISyntaxException {
    String[] parts = rowKey.split("\u0001", -1);
    return Urn.createFromString(parts[1]);
  }

  private static String rowKeyDisambig(String rowKey) {
    String[] parts = rowKey.split("\u0001", -1);
    return parts.length >= 3 ? parts[2] : "";
  }

  private static LineageRelationship mergeLineageRelationships(
      LineageRelationship existingRelationship, LineageRelationship newRelationship) {
    try {
      LineageRelationship copyRelationship = existingRelationship.copy();
      copyRelationship.setDegree(
          Math.min(existingRelationship.getDegree(), newRelationship.getDegree()));
      Set<Integer> degrees = new HashSet<>();
      if (copyRelationship.hasDegrees()) {
        for (Integer d : copyRelationship.getDegrees()) {
          degrees.add(d);
        }
      }
      degrees.add(newRelationship.getDegree());
      IntegerArray degreeArray = new IntegerArray();
      degrees.stream().sorted().forEach(degreeArray::add);
      copyRelationship.setDegrees(degreeArray);
      Set<UrnArray> uniquePaths = new HashSet<>();
      if (existingRelationship.hasPaths()) {
        for (UrnArray path : existingRelationship.getPaths()) {
          uniquePaths.add(path);
        }
      }
      if (newRelationship.hasPaths()) {
        for (UrnArray path : newRelationship.getPaths()) {
          uniquePaths.add(path);
        }
      }

      UrnArrayArray copyPaths = new UrnArrayArray(uniquePaths.size());
      copyPaths.addAll(uniquePaths);
      copyRelationship.setPaths(copyPaths);
      return copyRelationship;
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String lineageRowKey(
      String relationshipType, Urn end, RelatedEntity re, @Nullable String lifecycleOwnerUrn) {
    String via = re.getVia();
    String disambig =
        (via != null && !via.isBlank())
            ? via
            : (lifecycleOwnerUrn != null && !lifecycleOwnerUrn.isBlank() ? lifecycleOwnerUrn : "");
    return relationshipType + "\u0001" + end.toString() + "\u0001" + disambig;
  }

  private static void mergeAdditionalHop(LineageRelationship lr, int hop) {
    lr.setDegree(Math.min(lr.getDegree(), hop));
    IntegerArray degrees = lr.getDegrees();
    if (degrees == null) {
      degrees = new IntegerArray();
      if (lr.hasDegree()) {
        degrees.add(lr.getDegree());
      }
      lr.setDegrees(degrees);
    }
    for (int i = 0; i < degrees.size(); i++) {
      if (degrees.get(i) == hop) {
        return;
      }
    }
    degrees.add(hop);
  }

  private static UrnArray buildFirstHopPath(
      Urn root, Urn end, RelatedEntity re, @Nullable String lifecycleOwnerUrn)
      throws URISyntaxException {
    UrnArray path = new UrnArray();
    path.add(root);
    if (re.getVia() != null && !re.getVia().isBlank()) {
      path.add(Urn.createFromString(re.getVia()));
    } else if (lifecycleOwnerUrn != null && !lifecycleOwnerUrn.isBlank()) {
      path.add(Urn.createFromString(lifecycleOwnerUrn));
    }
    path.add(end);
    return path;
  }

  private UrnArray buildPath(Urn start, Urn end, Map<Urn, Urn> parent) throws URISyntaxException {
    List<Urn> backward = new ArrayList<>();
    Urn cur = end;
    Set<Urn> seen = new HashSet<>();
    while (cur != null && seen.add(cur)) {
      backward.add(cur);
      if (cur.equals(start)) {
        break;
      }
      cur = parent.get(cur);
    }
    Collections.reverse(backward);
    UrnArray forward = new UrnArray();
    forward.addAll(backward);
    return forward;
  }
}
