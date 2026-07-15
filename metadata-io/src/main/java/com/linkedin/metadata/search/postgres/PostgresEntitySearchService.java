package com.linkedin.metadata.search.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.browse.BrowseResultEntity;
import com.linkedin.metadata.browse.BrowseResultEntityArray;
import com.linkedin.metadata.browse.BrowseResultGroup;
import com.linkedin.metadata.browse.BrowseResultGroupArray;
import com.linkedin.metadata.browse.BrowseResultGroupV2;
import com.linkedin.metadata.browse.BrowseResultGroupV2Array;
import com.linkedin.metadata.browse.BrowseResultMetadata;
import com.linkedin.metadata.browse.BrowseResultV2;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import com.linkedin.metadata.config.search.SearchServiceConfiguration;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.AutoCompleteEntityArray;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.postgres.query.PostgresSearchDocumentFieldPathUtil;
import com.linkedin.metadata.search.postgres.query.PostgresSearchFilterSqlBuilder;
import com.linkedin.metadata.search.postgres.query.PostgresSearchFilterSqlBuilder.SqlFragment;
import com.linkedin.metadata.search.utils.ESAccessControlUtil;
import com.linkedin.metadata.search.utils.SearchResultUtils;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.metadata.context.OperationContext;
import io.ebean.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.Explanation;
import org.opensearch.action.explain.ExplainResponse;

/**
 * PostgreSQL-backed keyword entity search reads against SqlSetup {@code {prefix}_search_row}.
 *
 * <p><strong>Write APIs</strong> are not supported here — index writes remain on OpenSearch; {@link
 * com.linkedin.metadata.search.RoutingEntitySearchService} delegates mutations to {@link
 * com.linkedin.metadata.search.elasticsearch.ElasticSearchService}.
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresEntitySearchService implements EntitySearchService {

  private static final String ALIAS = "t";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Nonnull private final Database database;
  @Nonnull private final PostgresSqlSetupProperties postgresSqlSetupProperties;
  @Nonnull private final SearchServiceConfiguration searchServiceConfiguration;

  @Override
  public SearchServiceConfiguration getSearchServiceConfig() {
    return searchServiceConfiguration;
  }

  @Nonnull
  private String qualifiedSearchRowTable() {
    String schema = postgresSqlSetupProperties.normalizedPostgresSchema();
    String prefix = postgresSqlSetupProperties.normalizedPgSearchEntityTablePrefix();
    return schema + "." + prefix + "_search_row";
  }

  private int effectiveTierCount() {
    int n =
        postgresSqlSetupProperties
            .getPgSearch()
            .getEntity()
            .getFulltext()
            .getTierTsvectorColumnCount();
    if (n < 1) {
      return 1;
    }
    return Math.min(n, 32);
  }

  @Nonnull
  private String fulltextLanguage() {
    String raw =
        postgresSqlSetupProperties.getPgSearch().getEntity().getFulltext().getDefaultLanguage();
    if (raw == null || raw.isBlank()) {
      return "english";
    }
    return raw.trim().replace("'", "");
  }

  @Override
  public void clear(@Nonnull OperationContext opContext) {
    throw new UnsupportedOperationException(
        "PostgreSQL entity search does not implement clear(); handled by OpenSearch delegate.");
  }

  @Override
  public long docCount(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nullable Filter filter) {
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            filter, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, List.of(entityName));
    String table = qualifiedSearchRowTable();
    String scope = entityScopeWhere(opContext, List.of(entityName));
    String sql =
        "SELECT COUNT(*) FROM "
            + table
            + " "
            + ALIAS
            + " WHERE "
            + scope
            + " AND ("
            + frag.getSql()
            + ")";
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = 1;
        i = bindAll(ps, i, frag.getArgs());
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return rs.getLong(1);
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL docCount failed", e);
    }
    return 0L;
  }

  @Nonnull
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    return search(opContext, entityNames, input, postFilters, sortCriteria, from, size, List.of());
  }

  @Nonnull
  @Override
  public SearchResult search(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return searchInternal(
        opContext, entityNames, input, postFilters, sortCriteria, from, size, facets, null);
  }

  /**
   * Shared search path; {@code scrollCursor} enables keyset continuation (no OFFSET) for scroll.
   */
  private SearchResult searchInternal(
      OperationContext opContext,
      List<String> entityNames,
      String input,
      Filter postFilters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size,
      List<String> facets,
      @Nullable PostgresScrollId.KeysetCursor scrollCursor) {
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            postFilters, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, entityNames);
    String table = qualifiedSearchRowTable();
    String scope = entityScopeWhere(opContext, entityNames);
    boolean fulltext =
        Optional.ofNullable(opContext.getSearchContext().getSearchFlags())
            .map(SearchFlags::isFulltext)
            .orElse(true);
    FtsSql fts = buildFtsSql(input, fulltext);
    String filterSql = scope + " AND (" + frag.getSql() + ") AND (" + fts.predicate + ")";
    Integer limit = ConfigUtils.applyLimit(searchServiceConfiguration, size);
    int effectiveFrom = scrollCursor != null ? 0 : from;

    long total = executeCount(table, fts.fromJoin, filterSql, frag.getArgs(), fts, input);
    List<SearchEntity> hits =
        executeSearchHits(
            opContext,
            entityNames,
            table,
            fts.fromJoin,
            filterSql,
            frag.getArgs(),
            fts,
            sortCriteria,
            effectiveFrom,
            limit,
            input,
            scrollCursor);

    SearchResultMetadata meta =
        new SearchResultMetadata().setAggregations(new AggregationMetadataArray());
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    if (flags == null || !Boolean.TRUE.equals(flags.isSkipAggregates())) {
      meta.setAggregations(
          buildFacetAggregations(
              opContext, table, fts.fromJoin, filterSql, frag.getArgs(), fts, facets, input));
    }

    ESAccessControlUtil.restrictSearchResult(opContext, hits);
    return new SearchResult()
        .setEntities(new SearchEntityArray(hits))
        .setMetadata(meta)
        .setFrom(effectiveFrom)
        .setPageSize(limit != null ? limit : 0)
        .setNumEntities((int) Math.min(total, Integer.MAX_VALUE));
  }

  private static final class FtsSql {
    final String fromJoin;
    final String rankExpr;
    final String predicate;
    final boolean active;

    FtsSql(String fromJoin, String rankExpr, String predicate, boolean active) {
      this.fromJoin = fromJoin;
      this.rankExpr = rankExpr;
      this.predicate = predicate;
      this.active = active;
    }
  }

  private FtsSql buildFtsSql(String input, boolean fulltext) {
    if (!fulltext || input == null || input.isBlank() || "*".equals(input.trim())) {
      return new FtsSql("", "1.0", "TRUE", false);
    }
    String lang = fulltextLanguage().replace("'", "");
    String join = " CROSS JOIN (SELECT plainto_tsquery('" + lang + "', ?) AS tsq) fts";
    StringBuilder rank = new StringBuilder("(");
    int tiers = effectiveTierCount();
    for (int t = 1; t <= tiers; t++) {
      if (t > 1) {
        rank.append(" + ");
      }
      rank.append("COALESCE(ts_rank_cd(")
          .append(ALIAS)
          .append(".")
          .append(PostgresSqlSetupProperties.searchVectorTierColumnName(t))
          .append(", fts.tsq), 0)");
    }
    rank.append(")");
    List<String> ors = new ArrayList<>();
    for (int t = 1; t <= tiers; t++) {
      ors.add(
          ALIAS + "." + PostgresSqlSetupProperties.searchVectorTierColumnName(t) + " @@ fts.tsq");
    }
    String pred = "(" + String.join(" OR ", ors) + ")";
    return new FtsSql(join, rank.toString(), pred, true);
  }

  private int bindFtsQuery(PreparedStatement ps, int start, FtsSql fts, String input)
      throws SQLException {
    if (!fts.active) {
      return start;
    }
    ps.setString(start, input.trim());
    return start + 1;
  }

  private AggregationMetadataArray buildFacetAggregations(
      OperationContext opContext,
      String table,
      String ftsJoin,
      String filterSql,
      List<Object> filterArgs,
      FtsSql fts,
      List<String> facets,
      String input) {
    if (facets == null || facets.isEmpty()) {
      return new AggregationMetadataArray();
    }
    List<AggregationMetadata> list = new ArrayList<>();
    for (String facet : facets) {
      if (SearchUtil.INDEX_VIRTUAL_FIELD.equals(facet) || "_entityType".equalsIgnoreCase(facet)) {
        Map<String, Long> counts =
            groupCountByColumn(
                table, ftsJoin, filterSql, filterArgs, fts, ALIAS + ".entity_type", input);
        list.add(
            new AggregationMetadata()
                .setName(facet)
                .setDisplayName("Type")
                .setAggregations(new com.linkedin.data.template.LongMap(counts))
                .setFilterValues(
                    new FilterValueArray(
                        SearchUtil.convertToFilters(counts, Collections.emptySet()))));
      }
    }
    return new AggregationMetadataArray(list);
  }

  private Map<String, Long> groupCountByColumn(
      String table,
      String ftsJoin,
      String filterSql,
      List<Object> filterArgs,
      FtsSql fts,
      String groupExpr,
      String input) {
    String sql =
        "SELECT "
            + groupExpr
            + ", COUNT(*)::bigint FROM "
            + table
            + " "
            + ALIAS
            + ftsJoin
            + " WHERE "
            + filterSql
            + " GROUP BY "
            + groupExpr;
    Map<String, Long> map = new LinkedHashMap<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindFtsQuery(ps, 1, fts, input);
        i = bindAll(ps, i, filterArgs);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String k = rs.getString(1);
            if (k != null) {
              map.put(k, rs.getLong(2));
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL facet aggregation failed", e);
    }
    return map;
  }

  @Nonnull
  @Override
  public SearchResult filter(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nullable Filter filters,
      List<SortCriterion> sortCriteria,
      int from,
      @Nullable Integer size) {
    return search(
        opContext, List.of(entityName), "*", filters, sortCriteria, from, size, List.of());
  }

  @Override
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String document,
      @Nonnull String docId) {
    throw new UnsupportedOperationException("Use OpenSearch delegate for index writes.");
  }

  @Override
  public void deleteDocument(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String docId) {
    throw new UnsupportedOperationException("Use OpenSearch delegate for index writes.");
  }

  @Override
  public void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull Urn urn, @Nullable String runId) {
    throw new UnsupportedOperationException("Use OpenSearch delegate for index writes.");
  }

  @Nonnull
  @Override
  public AutoCompleteResult autoComplete(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String query,
      @Nullable String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            requestParams, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, List.of(entityName));
    String table = qualifiedSearchRowTable();
    String scope = entityScopeWhere(opContext, List.of(entityName));
    int lim = limit != null ? Math.min(limit, 50) : 10;
    String textCol = PostgresSqlSetupProperties.searchTextTierColumnName(1);
    String sql =
        "SELECT DISTINCT "
            + textCol
            + "::text FROM "
            + table
            + " "
            + ALIAS
            + " WHERE "
            + scope
            + " AND ("
            + frag.getSql()
            + ") AND COALESCE("
            + textCol
            + "::text, '') ILIKE ? ORDER BY 1 LIMIT ?";
    List<String> suggestions = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindAll(ps, 1, frag.getArgs());
        ps.setString(i++, "%" + query + "%");
        ps.setInt(i, lim);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String s = rs.getString(1);
            if (s != null && !s.isBlank()) {
              suggestions.add(s);
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL autoComplete failed", e);
    }
    return new AutoCompleteResult()
        .setQuery(query)
        .setSuggestions(new StringArray(suggestions))
        .setEntities(new AutoCompleteEntityArray());
  }

  @Nonnull
  @Override
  public Map<String, Long> aggregateByValue(
      @Nonnull OperationContext opContext,
      @Nullable List<String> entityNames,
      @Nonnull String field,
      @Nullable Filter requestParams,
      @Nullable Integer limit) {
    List<String> entities =
        entityNames == null || entityNames.isEmpty()
            ? List.copyOf(opContext.getEntityRegistry().getEntitySpecs().keySet())
            : entityNames;
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            requestParams, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, entities);
    String scope = entityScopeWhere(opContext, entities);
    String table = qualifiedSearchRowTable();
    int lim = limit != null ? limit : 20;
    final String sql;
    if (SearchUtil.INDEX_VIRTUAL_FIELD.equals(field) || "_entityType".equals(field)) {
      String expr = ALIAS + ".entity_type";
      sql =
          "SELECT "
              + expr
              + " AS k, COUNT(*)::bigint AS c FROM "
              + table
              + " "
              + ALIAS
              + " WHERE "
              + scope
              + " AND ("
              + frag.getSql()
              + ") GROUP BY k ORDER BY c DESC NULLS LAST LIMIT ?";
    } else {
      String jsonbExpr =
          PostgresSearchDocumentFieldPathUtil.toDocumentJsonbSqlExpr(
              opContext.getEntityRegistry(), ALIAS, field, entities);
      String facetJsonbNormalized =
          "(CASE WHEN ("
              + jsonbExpr
              + ") IS NULL OR jsonb_typeof(("
              + jsonbExpr
              + ")) = 'null' THEN '[]'::jsonb WHEN jsonb_typeof(("
              + jsonbExpr
              + ")) = 'array' THEN ("
              + jsonbExpr
              + ") ELSE jsonb_build_array(("
              + jsonbExpr
              + ")) END)";
      sql =
          "SELECT facet_bucket.k AS k, COUNT(*)::bigint AS c FROM "
              + table
              + " "
              + ALIAS
              + " CROSS JOIN LATERAL jsonb_array_elements_text("
              + facetJsonbNormalized
              + ") AS facet_bucket(k) WHERE "
              + scope
              + " AND ("
              + frag.getSql()
              + ") AND facet_bucket.k IS NOT NULL AND btrim(facet_bucket.k) <> '' GROUP BY facet_bucket.k ORDER BY c DESC NULLS LAST LIMIT ?";
    }
    Map<String, Long> out = new LinkedHashMap<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindAll(ps, 1, frag.getArgs());
        ps.setInt(i, lim);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            String k = rs.getString(1);
            if (k != null) {
              out.put(k, rs.getLong(2));
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL aggregateByValue failed", e);
    }
    return out;
  }

  @Nonnull
  @Override
  public BrowseResult browse(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filters,
      int from,
      @Nullable Integer size) {
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            filters, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, List.of(entityName));
    String table = qualifiedSearchRowTable();
    String scope = entityScopeWhere(opContext, List.of(entityName));
    Integer pageSize = ConfigUtils.applyLimit(searchServiceConfiguration, size);
    String sql =
        "SELECT "
            + ALIAS
            + ".urn, "
            + ALIAS
            + ".document::text FROM "
            + table
            + " "
            + ALIAS
            + " WHERE "
            + scope
            + " AND ("
            + frag.getSql()
            + ") LIMIT 50000";
    List<JsonRow> rows = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        bindAll(ps, 1, frag.getArgs());
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            try {
              rows.add(new JsonRow(rs.getString(1), rs.getString(2)));
            } catch (JsonProcessingException e) {
              log.debug("Skipping malformed browse document: {}", e.toString());
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL browse failed", e);
    }
    BrowseComputation bc =
        BrowseComputation.compute(path, rows, from, pageSize != null ? pageSize : 10);
    return new BrowseResult()
        .setEntities(new BrowseResultEntityArray(bc.entities))
        .setGroups(new BrowseResultGroupArray(bc.groups))
        .setMetadata(new BrowseResultMetadata())
        .setFrom(from)
        .setPageSize(pageSize != null ? pageSize : 0)
        .setNumEntities(bc.totalEntities)
        .setNumGroups(bc.groups.size())
        .setNumElements(bc.groups.size() + bc.entities.size());
  }

  @Nonnull
  @Override
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      @Nullable Integer count) {
    return browseV2(opContext, List.of(entityName), path, filter, input, start, count);
  }

  @Nonnull
  @Override
  public BrowseResultV2 browseV2(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityNames,
      @Nonnull String path,
      @Nullable Filter filter,
      @Nonnull String input,
      int start,
      @Nullable Integer count) {
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            filter, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, entityNames);
    String table = qualifiedSearchRowTable();
    String scope = entityScopeWhere(opContext, entityNames);
    Integer pageSize = ConfigUtils.applyLimit(searchServiceConfiguration, count);
    FtsSql fts = buildFtsSql(input, true);
    String filterSql = scope + " AND (" + frag.getSql() + ") AND (" + fts.predicate + ")";
    String sql =
        "SELECT "
            + ALIAS
            + ".urn, "
            + ALIAS
            + ".document::text, "
            + fts.rankExpr
            + " AS _rank FROM "
            + table
            + " "
            + ALIAS
            + fts.fromJoin
            + " WHERE "
            + filterSql
            + " ORDER BY _rank DESC NULLS LAST, urn ASC LIMIT 50000";
    List<JsonRow> rows = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindFtsQuery(ps, 1, fts, input);
        i = bindAll(ps, i, frag.getArgs());
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            try {
              rows.add(new JsonRow(rs.getString(1), rs.getString(2)));
            } catch (JsonProcessingException e) {
              log.debug("Skipping malformed browseV2 document: {}", e.toString());
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL browseV2 failed", e);
    }
    BrowseComputationV2 bc =
        BrowseComputationV2.compute(path, rows, start, pageSize != null ? pageSize : 10);
    return new BrowseResultV2()
        .setGroups(new BrowseResultGroupV2Array(bc.groups))
        .setMetadata(new BrowseResultMetadata())
        .setFrom(start)
        .setPageSize(pageSize != null ? pageSize : 0)
        .setNumGroups(bc.groups.size());
  }

  @Nonnull
  @Override
  public List<String> getBrowsePaths(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull Urn urn) {
    String table = qualifiedSearchRowTable();
    String sql =
        "SELECT document::text FROM " + table + " WHERE urn = ? AND entity_type = ? LIMIT 1";
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, urn.toString());
        ps.setString(2, entityName);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            String doc = rs.getString(1);
            JsonNode root = OBJECT_MAPPER.readTree(doc);
            JsonNode bp = root.get("browsePaths");
            if (bp != null && bp.isArray()) {
              List<String> paths = new ArrayList<>();
              for (JsonNode n : bp) {
                if (n.isTextual()) {
                  paths.add(n.asText());
                }
              }
              return paths;
            }
          }
        }
      }
    } catch (Exception e) {
      log.warn("getBrowsePaths failed for {}: {}", urn, e.toString());
    }
    return List.of();
  }

  @Nonnull
  @Override
  public ScrollResult fullTextScroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return scrollInternal(
        opContext, entities, input, postFilters, sortCriteria, scrollId, size, facets, true);
  }

  @Nonnull
  @Override
  public ScrollResult structuredScroll(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entities,
      @Nonnull String input,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    return scrollInternal(
        opContext, entities, input, postFilters, sortCriteria, scrollId, size, facets, false);
  }

  private ScrollResult scrollInternal(
      OperationContext opContext,
      List<String> entities,
      String input,
      Filter postFilters,
      List<SortCriterion> sortCriteria,
      String scrollId,
      Integer size,
      List<String> facets,
      boolean fulltextFlag) {
    Optional<PostgresScrollId.KeysetCursor> cursor = PostgresScrollId.decodeKeyset(scrollId);
    OperationContext oc = opContext.withSearchFlags(f -> f.setFulltext(fulltextFlag));
    SearchResult sr =
        searchInternal(
            oc, entities, input, postFilters, sortCriteria, 0, size, facets, cursor.orElse(null));
    List<SearchEntity> ents = new ArrayList<>(sr.getEntities());
    Integer limit = ConfigUtils.applyLimit(searchServiceConfiguration, size);
    int page = limit != null && limit > 0 ? limit : 10;
    String next = null;
    if (ents.size() == page && !ents.isEmpty()) {
      SearchEntity last = ents.get(ents.size() - 1);
      next =
          PostgresScrollId.encodeKeysetV1(
              last.getScore().doubleValue(), last.getEntity().toString());
    }
    ScrollResult scrollResult =
        new ScrollResult()
            .setEntities(new SearchEntityArray(ents))
            .setMetadata(sr.getMetadata())
            .setPageSize(sr.getPageSize())
            .setNumEntities(sr.getNumEntities());
    if (next != null) {
      scrollResult.setScrollId(next);
    }
    return scrollResult;
  }

  @Override
  public ExplainResponse explain(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nonnull String documentId,
      @Nonnull String entityName,
      @Nullable Filter postFilters,
      List<SortCriterion> sortCriteria,
      @Nullable String scrollId,
      @Nullable String keepAlive,
      @Nullable Integer size,
      @Nonnull List<String> facets) {
    Filter transformed =
        SearchUtil.transformFilterForEntities(
            postFilters, opContext.getSearchContext().getIndexConvention());
    SqlFragment frag =
        PostgresSearchFilterSqlBuilder.build(transformed, opContext, ALIAS, List.of(entityName));
    String table = qualifiedSearchRowTable();
    String scope = entityScopeWhere(opContext, List.of(entityName));
    FtsSql fts = buildFtsSql(query, true);
    String filterSql =
        scope
            + " AND ("
            + frag.getSql()
            + ") AND ("
            + fts.predicate
            + ") AND "
            + ALIAS
            + ".urn = ?";
    String sql =
        "SELECT "
            + fts.rankExpr
            + " FROM "
            + table
            + " "
            + ALIAS
            + fts.fromJoin
            + " WHERE "
            + filterSql
            + " LIMIT 1";
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindFtsQuery(ps, 1, fts, query);
        i = bindAll(ps, i, frag.getArgs());
        ps.setString(i, documentId);
        try (ResultSet rs = ps.executeQuery()) {
          float score = 0f;
          if (rs.next()) {
            score = rs.getFloat(1);
          }
          Explanation explanation =
              Explanation.match(
                  score, "PostgreSQL ts_rank_cd / pgSearch tier vectors", Collections.emptyList());
          return new ExplainResponse("pgSearch", documentId, true, explanation);
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL explain failed", e);
    }
  }

  @Nonnull
  @Override
  public Map<Urn, Map<String, Object>> raw(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    String table = qualifiedSearchRowTable();
    String inClause = urns.stream().map(u -> "?").collect(Collectors.joining(","));
    String sql = "SELECT urn, document::text FROM " + table + " WHERE urn IN (" + inClause + ")";
    Map<Urn, Map<String, Object>> out = new HashMap<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = 1;
        for (Urn u : urns) {
          ps.setString(i++, u.toString());
        }
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            Urn u = UrnUtils.getUrn(rs.getString(1));
            @SuppressWarnings("unchecked")
            Map<String, Object> m = OBJECT_MAPPER.readValue(rs.getString(2), Map.class);
            out.put(u, m);
          }
        }
      }
    } catch (SQLException | JsonProcessingException e) {
      throw new IllegalStateException("PostgreSQL raw fetch failed", e);
    }
    return out;
  }

  @Override
  public boolean validateAndSwapAlias(
      @Nonnull OperationContext opContext,
      @Nonnull String aliasName,
      @Nonnull String newBackingIndex) {
    throw new UnsupportedOperationException("Alias swaps are handled by OpenSearch index builder.");
  }

  @Override
  public void appendRunId(
      @Nonnull OperationContext opContext, @Nonnull List<IngestResult> results) {
    EntitySearchService.super.appendRunId(opContext, results);
  }

  private long executeCount(
      String table,
      String ftsJoin,
      String filterSql,
      List<Object> filterArgs,
      FtsSql fts,
      String input) {
    String sql = "SELECT COUNT(*) FROM " + table + " " + ALIAS + ftsJoin + " WHERE " + filterSql;
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindFtsQuery(ps, 1, fts, input);
        i = bindAll(ps, i, filterArgs);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return rs.getLong(1);
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL search count failed", e);
    }
    return 0L;
  }

  private List<SearchEntity> executeSearchHits(
      OperationContext opContext,
      List<String> entityNames,
      String table,
      String ftsJoin,
      String filterSql,
      List<Object> filterArgs,
      FtsSql fts,
      List<SortCriterion> sortCriteria,
      int from,
      Integer size,
      String input,
      @Nullable PostgresScrollId.KeysetCursor scrollCursor) {
    String orderBy = orderByClause(sortCriteria, opContext, entityNames, scrollCursor != null);
    int lim = size != null ? size : 10;
    List<Object> keysetBinds = new ArrayList<>();
    String keysetSql = "";
    if (scrollCursor != null) {
      keysetSql = keysetContinuationSql(sortCriteria, fts, scrollCursor, keysetBinds);
    }
    String limitOffset = scrollCursor != null ? " LIMIT ?" : " LIMIT ? OFFSET ?";
    String sql =
        "SELECT "
            + ALIAS
            + ".urn, "
            + ALIAS
            + ".document::text, "
            + fts.rankExpr
            + " AS _rank FROM "
            + table
            + " "
            + ALIAS
            + ftsJoin
            + " WHERE "
            + filterSql
            + keysetSql
            + " "
            + orderBy
            + limitOffset;
    List<SearchEntity> hits = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection()) {
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        int i = bindFtsQuery(ps, 1, fts, input);
        i = bindAll(ps, i, filterArgs);
        for (Object o : keysetBinds) {
          if (o instanceof Double) {
            ps.setDouble(i++, (Double) o);
          } else {
            ps.setString(i++, Objects.toString(o, ""));
          }
        }
        ps.setInt(i++, lim);
        if (scrollCursor == null) {
          ps.setInt(i, from);
        }
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            Urn urn = UrnUtils.getUrn(rs.getString(1));
            double rank = rs.getDouble(3);
            hits.add(
                new SearchEntity()
                    .setEntity(urn)
                    .setScore((float) rank)
                    .setMatchedFields(new MatchedFieldArray())
                    .setFeatures(SearchResultUtils.buildBaseFeatures(rank, null)));
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("PostgreSQL search failed", e);
    }
    return hits;
  }

  /**
   * Elasticsearch {@code search_after}–style keyset: continue after the last row of the previous
   * page using the same sort as {@link #orderByClause}. Matches {@link
   * com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper} (opaque sort
   * values), without OFFSET.
   */
  private String keysetContinuationSql(
      List<SortCriterion> sortCriteria,
      FtsSql fts,
      PostgresScrollId.KeysetCursor cursor,
      List<Object> binds) {
    Optional<Boolean> urnDesc = urnSortDescending(sortCriteria);
    if (urnDesc.isPresent()) {
      binds.add(cursor.getLastUrn());
      if (Boolean.TRUE.equals(urnDesc.get())) {
        return " AND " + ALIAS + ".urn < ?";
      }
      return " AND " + ALIAS + ".urn > ?";
    }
    String r = "(" + fts.rankExpr + ")";
    binds.add(cursor.getLastRank());
    binds.add(cursor.getLastRank());
    binds.add(cursor.getLastUrn());
    return " AND (" + r + " < ? OR (" + r + " = ? AND " + ALIAS + ".urn > ?))";
  }

  /** If sort is by {@code urn} only, return whether it is descending; empty if not urn-only. */
  private Optional<Boolean> urnSortDescending(List<SortCriterion> sortCriteria) {
    if (sortCriteria == null) {
      return Optional.empty();
    }
    for (SortCriterion sc : sortCriteria) {
      if ("urn".equalsIgnoreCase(sc.getField())) {
        return Optional.of(sc.getOrder() == SortOrder.DESCENDING);
      }
    }
    return Optional.empty();
  }

  private String orderByClause(
      List<SortCriterion> sortCriteria,
      OperationContext opContext,
      List<String> entityNames,
      boolean scrollKeysetPagination) {
    if (sortCriteria != null) {
      for (SortCriterion sc : sortCriteria) {
        if ("urn".equalsIgnoreCase(sc.getField())) {
          boolean desc = sc.getOrder() == SortOrder.DESCENDING;
          return "ORDER BY urn " + (desc ? "DESC" : "ASC");
        }
      }
      if (!scrollKeysetPagination) {
        for (SortCriterion sc : sortCriteria) {
          String resolved =
              resolveSortableDocumentOrderExpr(
                  opContext.getEntityRegistry(), entityNames, sc.getField());
          if (resolved != null) {
            boolean desc = sc.getOrder() == SortOrder.DESCENDING;
            return "ORDER BY " + resolved + " " + (desc ? "DESC" : "ASC") + " NULLS LAST, urn ASC";
          }
        }
      }
    }
    return "ORDER BY _rank DESC NULLS LAST, urn ASC";
  }

  /**
   * ORDER BY expression for a searchable document field, or null to fall back to relevance sort.
   */
  @Nullable
  private static String resolveSortableDocumentOrderExpr(
      EntityRegistry registry, List<String> entityNames, String field) {
    if (entityNames == null || entityNames.isEmpty()) {
      return null;
    }
    String normalized = PostgresSearchFilterSqlBuilder.normalizeFieldForJson(field);
    if (PostgresSearchDocumentFieldPathUtil.skipRegistryResolution(normalized, entityNames)) {
      return null;
    }
    String legacy = PostgresSearchFilterSqlBuilder.documentTextPathExpr(ALIAS, normalized);
    String resolved =
        PostgresSearchDocumentFieldPathUtil.toDocumentTextSqlExpr(
            registry, ALIAS, field, entityNames);
    if (resolved.equals(legacy)) {
      return null;
    }
    return resolved;
  }

  private String entityScopeWhere(OperationContext opContext, List<String> entityNames) {
    if (entityNames == null || entityNames.isEmpty()) {
      return "FALSE";
    }
    List<String> types =
        entityNames.stream()
            .map(n -> opContext.getEntityRegistry().getEntitySpec(n))
            .filter(Objects::nonNull)
            .map(EntitySpec::getName)
            .distinct()
            .collect(Collectors.toList());
    Set<String> groups = new HashSet<>();
    for (String name : entityNames) {
      EntitySpec spec = opContext.getEntityRegistry().getEntitySpec(name);
      if (spec != null && spec.getSearchGroup() != null) {
        groups.add(spec.getSearchGroup());
      }
    }
    String typeList =
        types.stream().map(t -> "'" + t.replace("'", "''") + "'").collect(Collectors.joining(","));
    String groupList =
        groups.stream().map(g -> "'" + g.replace("'", "''") + "'").collect(Collectors.joining(","));
    StringBuilder sb = new StringBuilder();
    sb.append(ALIAS).append(".entity_type IN (").append(typeList).append(")");
    if (!groups.isEmpty()) {
      sb.append(" AND ").append(ALIAS).append(".search_group IN (").append(groupList).append(")");
    }
    return sb.toString();
  }

  private static int bindAll(PreparedStatement ps, int start, List<Object> args)
      throws SQLException {
    int i = start;
    for (Object o : args) {
      ps.setObject(i++, o);
    }
    return i;
  }

  /** Holder for browse rows. */
  private static final class JsonRow {
    final String urn;
    final JsonNode document;

    JsonRow(String urn, String json) throws JsonProcessingException {
      this.urn = urn;
      this.document = OBJECT_MAPPER.readTree(json);
    }
  }

  private static final class BrowseComputation {
    final List<BrowseResultEntity> entities;
    final List<BrowseResultGroup> groups;
    final int totalEntities;

    BrowseComputation(
        List<BrowseResultEntity> entities, List<BrowseResultGroup> groups, int totalEntities) {
      this.entities = entities;
      this.groups = groups;
      this.totalEntities = totalEntities;
    }

    static BrowseComputation compute(String path, List<JsonRow> rows, int from, int size) {
      String normalizedPath = path == null ? "" : path;
      Map<String, Long> groupCounts = new LinkedHashMap<>();
      List<BrowseResultEntity> entityHits = new ArrayList<>();
      int matched = 0;
      for (JsonRow row : rows) {
        JsonNode bp = row.document.get("browsePaths");
        if (bp == null || !bp.isArray()) {
          continue;
        }
        boolean any = false;
        for (JsonNode p : bp) {
          if (!p.isTextual()) {
            continue;
          }
          String browsePath = p.asText();
          if (underPath(normalizedPath, browsePath)) {
            any = true;
            String next = nextSegment(normalizedPath, browsePath);
            if (next != null) {
              groupCounts.merge(next, 1L, Long::sum);
            } else {
              matched++;
              if (matched > from && entityHits.size() < size) {
                try {
                  entityHits.add(
                      new BrowseResultEntity()
                          .setUrn(Urn.createFromString(row.urn))
                          .setName(displayName(row.document)));
                } catch (Exception ignored) {
                }
              }
            }
          }
        }
      }
      List<BrowseResultGroup> groups =
          groupCounts.entrySet().stream()
              .map(e -> new BrowseResultGroup().setName(e.getKey()).setCount(e.getValue()))
              .collect(Collectors.toList());
      int total = groupCounts.values().stream().mapToInt(Long::intValue).sum() + matched;
      return new BrowseComputation(entityHits, groups, total);
    }

    private static boolean underPath(String prefix, String browsePath) {
      if (prefix.isEmpty()) {
        return true;
      }
      return browsePath.startsWith(prefix)
          && (browsePath.length() == prefix.length() || browsePath.charAt(prefix.length()) == '/');
    }

    private static String nextSegment(String prefix, String browsePath) {
      if (!browsePath.startsWith(prefix)) {
        return null;
      }
      String rest =
          browsePath.length() <= prefix.length()
              ? ""
              : browsePath.substring(prefix.endsWith("/") ? prefix.length() : prefix.length() + 1);
      if (rest.isEmpty()) {
        return null;
      }
      int slash = rest.indexOf('/');
      return slash < 0 ? rest : rest.substring(0, slash);
    }

    private static String displayName(JsonNode doc) {
      JsonNode dp = doc.path("_aspects").path("datasetProperties").path("name");
      if (dp.isMissingNode() || dp.isNull()) {
        return "";
      }
      return dp.asText("");
    }
  }

  private static final class BrowseComputationV2 {
    final List<BrowseResultEntity> entities;
    final List<BrowseResultGroupV2> groups;
    final int totalEntities;

    BrowseComputationV2(
        List<BrowseResultEntity> entities, List<BrowseResultGroupV2> groups, int totalEntities) {
      this.entities = entities;
      this.groups = groups;
      this.totalEntities = totalEntities;
    }

    static BrowseComputationV2 compute(String path, List<JsonRow> rows, int start, int count) {
      BrowseComputation bc = BrowseComputation.compute(path, rows, start, count);
      List<BrowseResultGroupV2> g2 =
          bc.groups.stream()
              .map(g -> new BrowseResultGroupV2().setName(g.getName()).setCount(g.getCount()))
              .collect(Collectors.toList());
      return new BrowseComputationV2(bc.entities, g2, bc.totalEntities);
    }
  }

  /**
   * Opaque scroll ids: {@linkplain #encodeKeysetV1 keyset} for scroll (aligned with OpenSearch
   * {@code search_after} / {@link
   * com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper}); legacy {@code o}
   * offset encoding remains for tests only.
   */
  public static final class PostgresScrollId {
    private static final String TYPE_KEYSET_V1 = "k1";

    /** Cursor after the last hit of the previous page (sort-aligned). */
    public static final class KeysetCursor {
      private final double lastRank;
      private final String lastUrn;

      public KeysetCursor(double lastRank, String lastUrn) {
        this.lastRank = lastRank;
        this.lastUrn = lastUrn;
      }

      public double getLastRank() {
        return lastRank;
      }

      public String getLastUrn() {
        return lastUrn;
      }
    }

    private PostgresScrollId() {}

    /**
     * Keyset continuation: last row's score and urn match {@link #executeSearchHits} sort order.
     */
    public static String encodeKeysetV1(double lastRank, String lastUrn) {
      try {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(
                OBJECT_MAPPER.writeValueAsBytes(
                    Map.of("t", TYPE_KEYSET_V1, "r", lastRank, "u", lastUrn)));
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(e);
      }
    }

    public static Optional<KeysetCursor> decodeKeyset(@Nullable String scrollId) {
      if (scrollId == null || scrollId.isBlank()) {
        return Optional.empty();
      }
      try {
        byte[] raw = Base64.getUrlDecoder().decode(scrollId);
        JsonNode n = OBJECT_MAPPER.readTree(raw);
        if (TYPE_KEYSET_V1.equals(n.path("t").asText()) && n.has("r") && n.has("u")) {
          return Optional.of(new KeysetCursor(n.get("r").asDouble(), n.get("u").asText()));
        }
      } catch (Exception ignored) {
      }
      return Optional.empty();
    }

    /** Unit tests only — paginated {@link #search} still uses OFFSET when {@code from > 0}. */
    public static String encodeOffset(int offset) {
      try {
        return Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(OBJECT_MAPPER.writeValueAsBytes(Map.of("o", offset)));
      } catch (JsonProcessingException e) {
        throw new IllegalStateException(e);
      }
    }

    public static int decodeOffset(@Nullable String scrollId) {
      if (scrollId == null || scrollId.isBlank()) {
        return 0;
      }
      try {
        byte[] raw = Base64.getUrlDecoder().decode(scrollId);
        JsonNode n = OBJECT_MAPPER.readTree(raw);
        if (n.has("o")) {
          return n.get("o").asInt(0);
        }
      } catch (Exception ignored) {
      }
      return 0;
    }
  }
}
