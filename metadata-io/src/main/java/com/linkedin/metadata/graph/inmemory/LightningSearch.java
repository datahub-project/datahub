package com.linkedin.metadata.graph.inmemory;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.graph.lineage.core.BaseGraph;
import com.linkedin.metadata.graph.lineage.core.GraphSnapshot;
import com.linkedin.metadata.graph.lineage.core.ImpactSearchResponse;
import com.linkedin.metadata.graph.lineage.core.WalkResult;
import com.linkedin.metadata.graph.lineage.core.WireCodec;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.LightningUrnFields;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The lightning lineage search over a walk result, run on the lineage graph server. It reproduces
 * {@code LineageSearchService}: the schema field grouping, {@code filterRelationships} (entity
 * types, then the degree filter), the lightning threshold, and {@code getLightningSearchResult}
 * (filters, page, facets), all in walk order, so its page and counts are exactly what GMS would
 * have computed from the full walk.
 *
 * <p>What a urn contributes (entity type, platform, environment, schema field parent) is stored in
 * the snapshot, computed at build time with {@link LightningUrnFields}, the same code GMS uses.
 */
public final class LightningSearch {

  private LightningSearch() {}

  /**
   * Thrown when a urn on the walk cannot be read the way GMS reads it; GMS then searches itself.
   */
  public static final class UnservableException extends RuntimeException {
    UnservableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /** Entity type, platform and environment of a urn: what it contributes to filters and facets. */
  record Keys(String entityType, @Nullable String platform, @Nullable String environment) {}

  /**
   * What one node contributes. {@code reportedNode} is the node the row is reported as (itself, or
   * a grouped schema field's parent); {@code parentNode} is the schema field parent the {@code
   * parent} filter reads, or -1.
   */
  record NodeKeys(int keys, int reportedNode, int parentNode) {}

  /**
   * Interned {@link Keys} for one snapshot. Every fact is stored in the snapshot (computed at build
   * time by {@link LightningNodeFacts}, the same code GMS uses), so this only maps a node's packed
   * facts to a small id for the per-search tables.
   */
  public static final class NodeFacets {
    private final BaseGraph base;
    private final List<Keys> keys = new ArrayList<>();
    private final Map<Integer, Integer> idOfMeta = new HashMap<>();

    private NodeFacets(BaseGraph base) {
      this.base = base;
    }

    /** The facets for {@code snapshot}: {@code previous} when it is over the same base graph. */
    public static NodeFacets forSnapshot(GraphSnapshot snapshot, @Nullable NodeFacets previous) {
      if (previous != null && previous.base == snapshot.base()) {
        return previous;
      }
      return new NodeFacets(snapshot.base());
    }

    synchronized Keys keys(int id) {
      return keys.get(id);
    }

    synchronized int keyCount() {
      return keys.size();
    }

    private synchronized int keysOf(int node) {
      int meta = base.meta(node);
      if (BaseGraph.unreadableOf(meta)) {
        throw new UnservableException("Cannot read facets of " + base.urns().get(node), null);
      }
      Integer id = idOfMeta.get(meta);
      if (id == null) {
        int platform = BaseGraph.platformOf(meta);
        int environment = BaseGraph.environmentOf(meta);
        id = keys.size();
        keys.add(
            new Keys(
                base.entityTypes().name(meta & 0xFF),
                platform < 0 ? null : base.platforms().name(platform),
                environment < 0 ? null : base.environments().name(environment)));
        idOfMeta.put(meta, id);
      }
      return id;
    }

    NodeKeys of(int node, boolean convertSchemaFields) {
      int parent = base.parent(node);
      if (parent < 0) {
        return new NodeKeys(keysOf(node), node, -1);
      }
      if (convertSchemaFields) {
        // Reported as the dataset it belongs to, which is not itself a schema field.
        return new NodeKeys(keysOf(parent), parent, -1);
      }
      return new NodeKeys(keysOf(node), node, parent);
    }
  }

  /**
   * Searches a walk. When the graph-only search does not apply (too few relationships after the
   * entity type and degree filters, and not forced), the page is not {@link Page#lightning} and
   * only {@link Page#prefiltered} is set: the caller returns the full walk instead.
   */
  public static Page search(
      WalkResult walk, ImpactLineageSearchPushdown.Spec spec, NodeFacets facets) {
    GraphSnapshot g = walk.graph();
    boolean degreeFilter = !spec.degreeValues().isEmpty();
    boolean degree1 = spec.degreeValues().contains("1");
    boolean degree2 = spec.degreeValues().contains("2");
    boolean degree3 = spec.degreeValues().contains("3+");
    boolean allTypes = spec.entityTypes().isEmpty();

    // Per keys id, filled on first sight: the keys, whether the type is wanted and whether the
    // filters pass (0 not known yet, 1 yes, 2 no; the filter verdict only for nodes without a
    // schema field parent), and how many matches it had.
    Keys[] keys = new Keys[Math.max(16, facets.keyCount())];
    byte[] typeOk = new byte[keys.length];
    byte[] filterOk = new byte[keys.length];
    long[] keyMatches = new long[keys.length];
    List<Integer> keyOrder = new ArrayList<>();
    // Schema field parents, decoded once per search: only the distinct parents in the walk.
    Map<Integer, String> parentUrns = new HashMap<>();

    int prefiltered = 0;
    int total = 0;
    int pageSize = 0;
    List<Integer> pageRows = new ArrayList<>();
    List<String> pageUrns = new ArrayList<>();
    // Per facet (platform, entity type, origin): values in first-seen order, and the match number
    // of
    // the last match that counted the facet and of the last that added a value to it.
    List<LinkedHashMap<String, long[]>> values =
        List.of(new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashMap<>());
    long[] lastCounted = {-1, -1, -1};
    long[] lastNew = {-1, -1, -1};

    for (int i = 0; i < walk.size(); i++) {
      NodeKeys nk = facets.of(walk.node(i), spec.convertSchemaFields());
      int id = nk.keys();
      if (id >= keys.length) {
        int n = Math.max(id + 1, keys.length * 2);
        keys = Arrays.copyOf(keys, n);
        typeOk = Arrays.copyOf(typeOk, n);
        filterOk = Arrays.copyOf(filterOk, n);
        keyMatches = Arrays.copyOf(keyMatches, n);
      }
      if (keys[id] == null) {
        keys[id] = facets.keys(id);
        typeOk[id] =
            (byte) (allTypes || spec.entityTypes().contains(keys[id].entityType()) ? 1 : 2);
      }
      Keys k = keys[id];
      if (typeOk[id] != 1) {
        continue;
      }
      if (degreeFilter) {
        int d = walk.degree(i);
        if (!((degree1 && d == 1) || (degree2 && d == 2) || (degree3 && d > 2))) {
          continue;
        }
      }
      prefiltered++;

      boolean passes;
      if (nk.parentNode() >= 0) {
        String parent = parentUrns.computeIfAbsent(nk.parentNode(), g::urn);
        passes = LightningUrnFields.passes(parent, k.platform(), k.environment(), spec.filters());
      } else {
        if (filterOk[id] == 0) {
          filterOk[id] =
              (byte)
                  (LightningUrnFields.passes(null, k.platform(), k.environment(), spec.filters())
                      ? 1
                      : 2);
        }
        passes = filterOk[id] == 1;
      }
      if (!passes) {
        continue;
      }

      // As getLightningSearchResult pages: matches after the first `from`, up to `size` of them.
      total++;
      if (total > spec.from() && pageSize < spec.size()) {
        pageRows.add(i);
        pageUrns.add(nk.reportedNode() == walk.node(i) ? null : g.urn(nk.reportedNode()));
        pageSize++;
      }
      String[] facetValues = {k.platform(), k.entityType(), k.environment()};
      if (keyMatches[id] == 0) {
        keyOrder.add(id);
        for (int f = 0; f < 3; f++) {
          if (facetValues[f] != null && !values.get(f).containsKey(facetValues[f])) {
            values.get(f).put(facetValues[f], new long[1]);
            lastNew[f] = total;
          }
        }
      }
      keyMatches[id]++;
      for (int f = 0; f < 3; f++) {
        if (facetValues[f] != null) {
          lastCounted[f] = total;
        }
      }
    }

    if (!(spec.forceLightning() || prefiltered > spec.lightningThreshold())) {
      return new Page(false, prefiltered, 0, List.of(), List.of());
    }

    for (int id : keyOrder) {
      String[] facetValues = {keys[id].platform(), keys[id].entityType(), keys[id].environment()};
      for (int f = 0; f < 3; f++) {
        if (facetValues[f] != null) {
          values.get(f).get(facetValues[f])[0] += keyMatches[id];
        }
      }
    }
    List<ImpactSearchResponse.Facet> facetList = new ArrayList<>(3);
    for (int f = 0; f < 3; f++) {
      List<String> v = new ArrayList<>(values.get(f).keySet());
      long[] counts = values.get(f).values().stream().mapToLong(c -> c[0]).toArray();
      facetList.add(new ImpactSearchResponse.Facet(v, counts, lastCounted[f] > lastNew[f]));
    }

    List<ImpactSearchResponse.Row> rows = new ArrayList<>(pageRows.size());
    for (int r = 0; r < pageRows.size(); r++) {
      int i = pageRows.get(r);
      List<List<String>> paths = new ArrayList<>(2);
      for (int[] p : walk.paths(i)) {
        List<String> path = new ArrayList<>(p.length);
        for (int node : p) {
          path.add(g.urn(node));
        }
        paths.add(path);
      }
      rows.add(
          new ImpactSearchResponse.Row(
              Objects.requireNonNullElseGet(pageUrns.get(r), () -> walk.urn(i)),
              walk.degree(i),
              paths));
    }
    return new Page(true, prefiltered, total, facetList, rows);
  }

  /**
   * The result of {@link #search}.
   *
   * @param lightning whether the graph-only search applied; if not, only {@code prefiltered} is set
   * @param facets platform, entity type and origin
   */
  public record Page(
      boolean lightning,
      int prefiltered,
      int total,
      List<ImpactSearchResponse.Facet> facets,
      List<ImpactSearchResponse.Row> rows) {}

  /** Reads a {@link ImpactLineageSearchPushdown.Spec} as the GMS client sends it. */
  public static ImpactLineageSearchPushdown.Spec specFromJson(JsonNode s) {
    if (!s.isObject()) {
      throw new IllegalArgumentException("search is required");
    }
    Set<String> types = new HashSet<>();
    s.path("entityTypes").forEach(t -> types.add(t.asText()));
    List<String> degrees = new ArrayList<>();
    s.path("degreeValues").forEach(d -> degrees.add(d.asText()));
    Filter filters =
        s.hasNonNull("filters")
            ? RecordUtils.toRecordTemplate(Filter.class, s.get("filters").asText())
            : null;
    return new ImpactLineageSearchPushdown.Spec(
        types,
        degrees,
        filters,
        s.path("convertSchemaFields").asBoolean(true),
        s.path("forceLightning").asBoolean(false),
        s.path("lightningThreshold").asLong(300),
        s.path("from").asInt(0),
        s.path("size").asInt(10));
  }

  /** Writes the answer: the page ({@code DHLS} lightning), or the full walk. */
  public static long encode(
      WalkResult walk,
      Page page,
      long walkMicros,
      long searchMicros,
      long lagMillis,
      OutputStream out)
      throws IOException {
    if (!page.lightning()) {
      return WireCodec.encodeSearchFull(
          walk, walkMicros, searchMicros, lagMillis, page.prefiltered(), out);
    }
    return WireCodec.encodeSearch(
        new ImpactSearchResponse(
            ImpactSearchResponse.Mode.LIGHTNING,
            null,
            walk.status(),
            walk.partial(),
            walk.hops(),
            walk.edgesScanned(),
            walkMicros,
            searchMicros,
            walk.graph().builtAtMillis(),
            lagMillis,
            walk.size(),
            page.prefiltered(),
            page.total(),
            page.facets(),
            page.rows()),
        out);
  }
}
