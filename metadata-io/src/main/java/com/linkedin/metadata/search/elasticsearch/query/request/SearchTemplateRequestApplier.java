package com.linkedin.metadata.search.elasticsearch.query.request;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.MatchedField;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFieldModel;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.HighlightCarry;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.MatchEvidence;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.MatchedFields;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchHighlightSelection;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMatchReport;
import com.linkedin.metadata.search.elasticsearch.query.template.StructuredPropertySubfields;
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
    List<String> sources =
        matcher.hasQueryWords() ? matchedFieldSources(opContext, template) : List.of();
    Map<String, String> labels = V3SearchFieldModel.of(entitySpecs).storedParsedNameLabels();
    boolean explain = explains(opContext);
    boolean deferred = HighlightCarry.isDeferred();
    return hit -> {
      if (!deferred) {
        return matchFields(hit, template, query, matcher, sources, labels, explain);
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
                    source, matchedQueries, template, query, matcher, sources, labels, requested));
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
      @Nonnull List<String> sources,
      @Nonnull Map<String, String> labels,
      boolean explain) {
    String[] matchedQueries =
        hit.getMatchedQueries() != null ? hit.getMatchedQueries() : new String[0];
    return matchFields(
        hit.getSourceAsMap(), matchedQueries, template, query, matcher, sources, labels, explain);
  }

  @Nonnull
  private static SearchHitMatches matchFields(
      @Nullable Map<String, Object> source,
      @Nonnull String[] matchedQueries,
      @Nonnull RenderedSearchTemplate template,
      @Nonnull String query,
      @Nonnull MatchedFields matcher,
      @Nonnull List<String> sources,
      @Nonnull Map<String, String> labels,
      boolean explain) {
    Map<String, Set<String>> matched = new LinkedHashMap<>();
    List<MatchEvidence> ordinary = List.of();
    if (source != null && !sources.isEmpty()) {
      SearchHitMatches scanned = matcher.matches(source, sources, labels, explain);
      for (MatchedField field : scanned.matched()) {
        matched
            .computeIfAbsent(field.getName(), ignored -> new LinkedHashSet<>())
            .add(field.getValue());
      }
      ordinary = scanned.explain();
    }
    for (String queryName : matchedQueries) {
      matched.putIfAbsent(queryName, Set.of(""));
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
    if (template == null || v3SearchQueryBuilder == null) {
      return List.of();
    }
    if (template.highlights().isEmpty()) {
      return List.of();
    }
    Map<String, List<String>> searched =
        v3SearchQueryBuilder.searchedFields(opContext, entitySpecs);
    List<String> queried = new ArrayList<>();
    for (String field : template.queriedFields()) {
      queried.add(baseQueryField(field));
    }
    return SearchHighlightSelection.sourcePaths(
        template.highlights(),
        searched,
        queried,
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

  @Nonnull
  private static String baseQueryField(@Nonnull String field) {
    String base = field;
    int boost = base.lastIndexOf('^');
    if (boost > 0) {
      base = base.substring(0, boost);
    }
    return StructuredPropertySubfields.strip(base);
  }
}
