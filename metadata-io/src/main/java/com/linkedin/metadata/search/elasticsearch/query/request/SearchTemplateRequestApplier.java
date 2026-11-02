package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.HighlightCarry;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.MatchEvidence;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.MatchedFields;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchHighlightSelection;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchHighlightSelection.FieldSources;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMatchReport;
import com.linkedin.metadata.search.elasticsearch.query.v3.RenderedSearchTemplate;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortOrder;

/** Template aggregations, rescore, sort, and matched fields. */
public final class SearchTemplateRequestApplier {
  private final AggregationQueryBuilder aggregationQueryBuilder;
  private final int maxTermBucketSize;
  private final List<EntitySpec> entitySpecs;
  @Nullable private final SearchFieldResolver v3SearchFieldResolver;
  private final int v3MinWordLength;
  @Nullable private final V3SearchQueryBuilder v3SearchQueryBuilder;

  public SearchTemplateRequestApplier(
      @Nonnull AggregationQueryBuilder aggregationQueryBuilder,
      int maxTermBucketSize,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nullable SearchFieldResolver v3SearchFieldResolver,
      int v3MinWordLength,
      @Nullable V3SearchQueryBuilder v3SearchQueryBuilder) {
    this.aggregationQueryBuilder = aggregationQueryBuilder;
    this.maxTermBucketSize = maxTermBucketSize;
    this.entitySpecs = entitySpecs;
    this.v3SearchFieldResolver = v3SearchFieldResolver;
    this.v3MinWordLength = v3MinWordLength;
    this.v3SearchQueryBuilder = v3SearchQueryBuilder;
  }

  public void apply(
      @Nonnull OperationContext opContext,
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nonnull SearchFlags searchFlags,
      @Nullable List<SortCriterion> sortCriteria,
      @Nonnull List<String> facets,
      @Nullable RenderedSearchTemplate template,
      boolean scroll,
      @Nullable Integer size,
      @Nonnull String input) {
    // A count has no hits to rescore or suggest. The lexical query and facets still run,
    // including a ladder that rejects a zero page and continues to the full query. Search V3
    // leaves highlighting off; skipHighlighting does not decide it. Matched fields still need
    // the queried _source paths.
    boolean countOnly = size != null && size == 0;
    String[] includes =
        Stream.concat(
                SearchDocFieldFetchConfig.resolve(
                    SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SCROLL, searchFlags)
                    .stream(),
                countOnly ? Stream.empty() : matchedFieldSources(opContext, template).stream())
            .distinct()
            .toArray(String[]::new);
    searchSourceBuilder.fetchSource(includes, null);
    if (Boolean.FALSE.equals(searchFlags.isSkipAggregates())) {
      aggregations(opContext, searchFlags, facets, template)
          .forEach(searchSourceBuilder::aggregation);
    }
    if (template != null
        && template.rescore() != null
        && relevanceSort(sortCriteria)
        && !scroll
        && !countOnly) {
      searchSourceBuilder.addRescorer(template.rescore());
      // Rescore requires score order. The URN tie-break keeps equal scores on one page.
      searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
      searchSourceBuilder.sort(new FieldSortBuilder("urn").order(SortOrder.ASC));
    } else {
      sort(opContext, searchSourceBuilder, sortCriteria, template);
    }
    if (!countOnly && !scroll && Boolean.TRUE.equals(searchFlags.isGetSuggestions())) {
      ESUtils.buildNameSuggestions(searchSourceBuilder, input, "name");
    }
  }

  @Nonnull
  private List<AggregationBuilder> aggregations(
      @Nonnull OperationContext opContext,
      @Nonnull SearchFlags searchFlags,
      @Nonnull List<String> facets,
      @Nullable RenderedSearchTemplate template) {
    if (template == null) {
      return aggregationQueryBuilder.getAggregations(opContext, facets);
    }
    if (!Boolean.TRUE.equals(searchFlags.isIncludeDefaultFacets())) {
      return aggregationQueryBuilder.getCallerAggregations(opContext, facets);
    }
    if (template.aggregations() == null || template.aggregations().isEmpty()) {
      return aggregationQueryBuilder.getAggregations(opContext, facets);
    }
    // Entity type and the caller's facets are mandatory. A template may add an aggregation, and a
    // reserved name keeps the Java one. Terms sizes are capped again here so a stored body cannot
    // exceed maxAggValues or maxTermBucketSize.
    Map<String, AggregationBuilder> byName = new LinkedHashMap<>();
    Set<String> reserved = new LinkedHashSet<>();
    for (AggregationBuilder aggregation :
        aggregationQueryBuilder.getEntityTypeAggregations(opContext)) {
      byName.put(aggregation.getName(), aggregation);
      reserved.add(aggregation.getName());
    }
    for (AggregationBuilder aggregation :
        aggregationQueryBuilder.getCallerAggregations(opContext, facets)) {
      if (reserved.add(aggregation.getName())) {
        byName.put(aggregation.getName(), aggregation);
      }
    }
    int requested =
        searchFlags.hasMaxAggValues() && searchFlags.getMaxAggValues() != null
            ? searchFlags.getMaxAggValues()
            : maxTermBucketSize;
    int cap = Math.min(requested, maxTermBucketSize);
    for (AggregationBuilder aggregation : template.aggregations()) {
      if (reserved.contains(aggregation.getName())) {
        continue;
      }
      SearchTemplateJson.clampAggregationSize(aggregation, cap);
      byName.put(aggregation.getName(), aggregation);
    }
    return new ArrayList<>(byName.values());
  }

  private void sort(
      @Nonnull OperationContext opContext,
      @Nonnull SearchSourceBuilder searchSourceBuilder,
      @Nullable List<SortCriterion> sortCriteria,
      @Nullable RenderedSearchTemplate template) {
    if (template != null
        && (sortCriteria == null || sortCriteria.isEmpty())
        && template.sorts() != null
        && !template.sorts().isEmpty()) {
      boolean namesUrn = false;
      for (SortBuilder<?> sort : template.sorts()) {
        searchSourceBuilder.sort(sort);
        if (sort instanceof FieldSortBuilder field && "urn".equals(field.getFieldName())) {
          namesUrn = true;
        }
      }
      if (!namesUrn) {
        searchSourceBuilder.sort(new FieldSortBuilder("urn").order(SortOrder.ASC));
      }
      return;
    }
    ESUtils.buildSortOrder(
        searchSourceBuilder,
        sortCriteria,
        entitySpecs,
        v3SearchFieldResolver,
        opContext,
        opContext.getAspectRetriever());
  }

  /**
   * Matched fields of each hit of one response. The fetched paths and their labels depend only on
   * the request, so they are resolved once rather than for every hit.
   */
  @Nonnull
  public Function<SearchHit, SearchHitMatches> matchedFieldsOf(
      @Nonnull OperationContext opContext, @Nullable RenderedSearchTemplate template) {
    if (template == null) {
      return hit -> SearchHitMatches.EMPTY;
    }
    String query = template.originalQuery() == null ? "" : template.originalQuery();
    MatchedFields matcher = new MatchedFields(query, v3MinWordLength);
    // A query with no words, such as *, matches no field value
    FieldSources queriedFields =
        matcher.hasQueryWords() && template.matchedFields()
            ? queriedFieldSources(opContext, template)
            : FieldSources.NONE;
    FieldSources highlightFields =
        matcher.hasQueryWords() ? highlightFieldSources(opContext, template) : FieldSources.NONE;
    Set<String> callerOptionalFilterFields =
        opContext.getSearchContext().getCallerOptionalFilterFields() == null
            ? Set.of()
            : opContext.getSearchContext().getCallerOptionalFilterFields();
    boolean explain = explains(opContext);
    boolean deferred = HighlightCarry.isDeferred();
    return hit -> {
      if (!deferred) {
        return matchFields(
            hit,
            template,
            query,
            matcher,
            queriedFields,
            highlightFields,
            callerOptionalFilterFields,
            explain);
      }
      Map<String, Object> source = hit.getSourceAsMap();
      Object urn = source == null ? null : source.get("urn");
      if (urn != null) {
        String[] matchedQueries =
            hit.getMatchedQueries() != null ? hit.getMatchedQueries() : new String[0];
        HighlightCarry.stash(
            urn.toString(),
            requested ->
                matchFields(
                    source,
                    matchedQueries,
                    template,
                    query,
                    matcher,
                    queriedFields,
                    highlightFields,
                    callerOptionalFilterFields,
                    requested));
      }
      return SearchHitMatches.EMPTY;
    };
  }

  private static boolean explains(@Nonnull OperationContext opContext) {
    SearchFlags flags = opContext.getSearchContext().getSearchFlags();
    return flags != null && Boolean.TRUE.equals(flags.isIncludeExplain());
  }

  @Nonnull
  private static SearchHitMatches matchFields(
      @Nonnull SearchHit hit,
      @Nonnull RenderedSearchTemplate template,
      @Nonnull String query,
      @Nonnull MatchedFields matcher,
      @Nonnull FieldSources queriedFields,
      @Nonnull FieldSources highlightFields,
      @Nonnull Set<String> callerOptionalFilterFields,
      boolean explain) {
    String[] matchedQueries =
        hit.getMatchedQueries() != null ? hit.getMatchedQueries() : new String[0];
    return matchFields(
        hit.getSourceAsMap(),
        matchedQueries,
        template,
        query,
        matcher,
        queriedFields,
        highlightFields,
        callerOptionalFilterFields,
        explain);
  }

  @Nonnull
  private static SearchHitMatches matchFields(
      @Nullable Map<String, Object> source,
      @Nonnull String[] matchedQueries,
      @Nonnull RenderedSearchTemplate template,
      @Nonnull String query,
      @Nonnull MatchedFields matcher,
      @Nonnull FieldSources queriedFields,
      @Nonnull FieldSources highlightFields,
      @Nonnull Set<String> callerOptionalFilterFields,
      boolean explain) {
    Map<String, Set<String>> matched = new LinkedHashMap<>();
    List<MatchEvidence> ordinary = new ArrayList<>();
    if (source != null) {
      scanFields(source, matcher, queriedFields, matched, ordinary, explain);
      scanFields(source, matcher, highlightFields, matched, ordinary, explain);
    }
    if (template.matchedFields()) {
      for (String queryName : matchedQueries) {
        if ((queryName.startsWith(SearchTemplateMatchReport.MATCHED_FIELD_PREFIX)
                || callerOptionalFilterFields.contains(queryName))
            && !SearchTemplateMatchReport.labelAlreadyMatched(matched, queryName)) {
          matched.put(queryName, Set.of(""));
        }
      }
    }
    Set<String> covered =
        template.namedClauses().values().stream()
            .map(RenderedSearchTemplate.NamedClause::path)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    List<MatchEvidence> explained = new ArrayList<>();
    for (MatchEvidence evidence : ordinary) {
      if (!covers(evidence.field(), covered)) {
        explained.add(evidence);
      }
    }
    if (explain) {
      explained.addAll(
          SearchTemplateMatchReport.explain(matched, source, query, template.namedClauses()));
    }
    return new SearchHitMatches(
        SearchTemplateMatchReport.fields(matched, source, query, template.namedClauses()),
        explained);
  }

  /**
   * Scans label keys first, then stored paths already fetched for labels that are not stored on the
   * label. {@link #addFirst} keeps one value per field, so a label hit does not hide a different
   * label's stored source and does not report the same label twice.
   */
  private static void scanFields(
      @Nonnull Map<String, Object> source,
      @Nonnull MatchedFields matcher,
      @Nonnull FieldSources fields,
      @Nonnull Map<String, Set<String>> matched,
      @Nonnull List<MatchEvidence> ordinary,
      boolean explain) {
    if (!fields.labelKeys().isEmpty()) {
      addFirst(matched, ordinary, matcher.matches(source, fields.labelKeys(), Map.of(), explain));
    }
    if (!fields.fallbackPaths().isEmpty()) {
      addFirst(
          matched,
          ordinary,
          matcher.matches(source, fields.fallbackPaths(), fields.fallbackLabels(), explain));
    }
    if (!fields.direct().isEmpty()) {
      addFirst(matched, ordinary, matcher.matches(source, fields.direct(), Map.of(), explain));
    }
  }

  /** One reported value per field name, in the order the scan found them. */
  private static void addFirst(
      @Nonnull Map<String, Set<String>> matched,
      @Nonnull List<MatchEvidence> ordinary,
      @Nonnull SearchHitMatches scanned) {
    Set<String> added = new LinkedHashSet<>();
    Set<String> reported = new LinkedHashSet<>();
    for (MatchedField field : scanned.matched()) {
      if (added.add(field.getName()) && !matched.containsKey(field.getName())) {
        matched
            .computeIfAbsent(field.getName(), ignored -> new LinkedHashSet<>())
            .add(field.getValue());
        reported.add(field.getName());
      }
    }
    Set<String> explained = new LinkedHashSet<>();
    for (MatchEvidence evidence : scanned.explain()) {
      if (reported.contains(evidence.field()) && explained.add(evidence.field())) {
        ordinary.add(evidence);
      }
    }
  }

  private static boolean covers(@Nonnull String name, @Nonnull Set<String> paths) {
    for (String path : paths) {
      if (name.equals(path) || name.startsWith(path + ".")) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private List<String> matchedFieldSources(
      @Nonnull OperationContext opContext, @Nullable RenderedSearchTemplate template) {
    if (template == null) {
      return List.of();
    }
    // The scan ignores a query with no words, such as *. Do not fetch paths it will not read.
    // Highlighting off still fetches queried paths when the query has words: that source is how a
    // part match is reported.
    String query = template.originalQuery() == null ? "" : template.originalQuery();
    if (!new MatchedFields(query, v3MinWordLength).hasQueryWords()) {
      return List.of();
    }
    LinkedHashSet<String> sources =
        new LinkedHashSet<>(queriedFieldSources(opContext, template).fetchPaths());
    sources.addAll(highlightFieldSources(opContext, template).fetchPaths());
    return List.copyOf(sources);
  }

  @Nonnull
  private FieldSources queriedFieldSources(
      @Nonnull OperationContext opContext, @Nonnull RenderedSearchTemplate template) {
    if (v3SearchQueryBuilder == null || !template.matchedFields()) {
      return FieldSources.NONE;
    }
    return SearchHighlightSelection.fields(
        template.queriedFields(),
        template.queriedFieldSources(),
        template.queriedFields(),
        v3SearchQueryBuilder.copiedStructuredPropertyFields(opContext, entitySpecs));
  }

  @Nonnull
  private FieldSources highlightFieldSources(
      @Nonnull OperationContext opContext, @Nonnull RenderedSearchTemplate template) {
    if (v3SearchQueryBuilder == null || template.highlights().isEmpty()) {
      return FieldSources.NONE;
    }
    return SearchHighlightSelection.fields(
        template.highlights(),
        template.queriedFieldSources(),
        template.queriedFields(),
        v3SearchQueryBuilder.copiedStructuredPropertyFields(opContext, entitySpecs));
  }

  /** No sort, or only descending {@code _score} / {@code score}, keeps rescore. */
  private static boolean relevanceSort(@Nullable List<SortCriterion> sortCriteria) {
    if (sortCriteria == null || sortCriteria.isEmpty()) {
      return true;
    }
    return sortCriteria.stream()
        .allMatch(
            criterion -> {
              String field = criterion.getField();
              return ("_score".equals(field) || "score".equals(field))
                  && criterion.getOrder() != com.linkedin.metadata.query.filter.SortOrder.ASCENDING;
            });
  }
}
