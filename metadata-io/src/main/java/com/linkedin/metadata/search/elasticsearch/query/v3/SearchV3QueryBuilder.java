package com.linkedin.metadata.search.elasticsearch.query.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;

import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchTemplateConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFieldModel;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchClauseBudget;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.SearchHighlightSelection;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.StructuredPropertySearchCatalog;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.StructuredQueryRouting;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateContextVariables;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateDialects;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFieldPruner;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFields;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateHoles;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateJson;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMatchReport;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMustache;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateNormalizationPolicy;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateQueryShape;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateSynonyms;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateViewer;
import com.linkedin.metadata.search.elasticsearch.query.template.StructuredPropertyClauses;
import com.linkedin.metadata.search.elasticsearch.query.template.StructuredPropertyReferences;
import com.linkedin.metadata.search.elasticsearch.query.template.StructuredPropertySubfields;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyOperator;
import com.linkedin.search.template.StructuredPropertyQuery;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilder;

/** Builds the Elasticsearch request for a resolved Search V3 template. */
public class SearchV3QueryBuilder {
  private final SearchTemplateLoader loader;
  private final SearchConfiguration searchConfiguration;
  private final V3SearchQueryBuilder keywordQueryBuilder;
  private final StructuredPropertySearchCatalog structuredPropertySearchCatalog;

  public SearchV3QueryBuilder(
      @Nonnull SearchTemplateLoader loader,
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this.loader = loader;
    SearchConfiguration local = SearchConfiguration.withSectionDefaults(searchConfiguration);
    this.searchConfiguration = local;
    SearchTemplateSynonyms.installFromClasspath();
    this.keywordQueryBuilder = new V3SearchQueryBuilder(local, customSearchConfiguration);
    this.structuredPropertySearchCatalog =
        new StructuredPropertySearchCatalog(local.getStructuredProperties());
  }

  @Nonnull
  public QueryBuilder render(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    return render(opContext, entitySpecs, query, fulltext, null, false).query();
  }

  @Nonnull
  public String structuredPropertyCatalogStamp(@Nonnull OperationContext opContext) {
    return structuredPropertySearchCatalog.catalogStamp(opContext);
  }

  @Nonnull
  public RenderedSearchTemplate render(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      @Nullable String renderUrn,
      boolean scroll) {
    try {
      return renderUnchecked(opContext, entitySpecs, query, fulltext, renderUrn, scroll);
    } catch (RuntimeException e) {
      SearchTemplateMetrics.renderFailure(opContext, "render");
      throw e;
    }
  }

  @Nonnull
  private RenderedSearchTemplate renderUnchecked(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      @Nullable String renderUrn,
      boolean scroll) {
    ResolvedSearchTemplate resolved = opContext.getSearchContext().getResolvedSearchTemplate();
    if (resolved == null) {
      throw new SearchTemplateException(
          "Search templates are enabled and no template was resolved for this query");
    }
    MergedSearchTemplate template =
        loader.load(opContext, renderUrn == null ? resolved.getUrn() : renderUrn);
    if (!SearchTemplateDialects.matches(
        template.getDialect(), SearchTemplateDialect.ELASTICSEARCH.name())) {
      throw new SearchTemplateException(
          "Search template " + template.getUrn() + " is dialect " + template.getDialect());
    }

    SearchFlags flags = opContext.getSearchContext().getSearchFlags();

    List<SearchFieldConfig> base =
        keywordQueryBuilder.templateBaseFieldConfigs(opContext, entitySpecs).stream()
            .sorted(
                Comparator.comparing(SearchFieldConfig::fieldName)
                    .thenComparing(field -> field.analyzer() == null ? "" : field.analyzer())
                    .thenComparing(SearchFieldConfig::boost))
            .toList();
    boolean bridge = template.isClasspathBridge();
    // Query-by-default properties come from the request catalog. Classpath mode still omits
    // template-authored property references, which would read stored definitions.
    List<StructuredPropertySearchCatalog.IndexedProperty> properties =
        structuredPropertySearchCatalog.indexed(opContext, entitySpecs);
    StructuredPropertyReferences.Resolver resolver =
        bridge
            ? StructuredPropertyReferences.omitting()
            : StructuredPropertyReferences.resolver(opContext, opContext.getAspectRetriever());
    List<SearchFieldConfig> layer2 =
        SearchTemplateFields.applyTemplate(base, template.getFields(), resolver);
    Set<SearchFieldConfig> queried = new LinkedHashSet<>(layer2);

    List<String> highlights =
        SearchHighlightSelection.resolve(
            SearchTemplateFields.applyHighlight(Set.of(), template.getHighlightFields()), flags);
    List<String> queriedFields = searchedFields(queried);

    TemplateContext builtContext = context(opContext, entitySpecs, query, template, queried);
    Map<String, Object> context = builtContext.values();
    int estimatedClauses = builtContext.estimatedClauses();
    context.putAll(renderFragments(template, context, opContext));
    String body = rewrite(template.getQuery(), opContext, template);
    String renderedQuery = body == null ? null : SearchTemplateMustache.render(body, context);
    SearchTemplateJson.rejectForbidden(renderedQuery, "query");
    QueryBuilder built =
        renderedQuery == null || renderedQuery.isBlank()
            ? QueryBuilders.matchAllQuery()
            : SearchTemplateJson.parseQuery(renderedQuery);
    built =
        propertyClauses(
            built, queried, properties, query, template.getStructuredPropertyQuery(), bridge);
    if (template.getClauses() != null) {
      String renderedClauses =
          SearchTemplateMustache.render(
              rewrite(template.getClauses(), opContext, template), context);
      SearchTemplateJson.rejectForbidden(renderedClauses, "clauses");
      built = SearchTemplateJson.mergeClauses(built, renderedClauses);
    }
    built = SearchTemplateFieldPruner.prune(built, removedFields(base, queried));
    String functionScore =
        template.getFunctionScore() == null
            ? null
            : SearchTemplateMustache.render(
                rewrite(template.getFunctionScore(), opContext, template), context);
    SearchTemplateJson.rejectForbidden(functionScore, "functionScore");
    List<SearchTemplateJson.TermsBoost> boosts =
        template.isClasspathBridge()
            ? List.of()
            : renderBoosts(template.getBoosts(), opContext, resolver);
    SearchTemplateJson.FunctionScoreTail tail =
        template.isClasspathBridge()
            ? SearchTemplateJson.FunctionScoreTail.none()
            : TemplateConfiguredScores.functionScoreTail(opContext, template);
    if (functionScore != null || !boosts.isEmpty() || tail.applies()) {
      built = SearchTemplateJson.wrapFunctionScore(built, functionScore, boosts, tail);
    }

    String renderedRescore = null;
    if (template.getRescore() != null && !scroll) {
      renderedRescore =
          SearchTemplateMustache.render(
              rewrite(template.getRescore(), opContext, template), context);
      SearchTemplateJson.rejectForbidden(renderedRescore, "rescore");
    }
    QueryRescorerBuilder rescore =
        renderedRescore == null ? null : SearchTemplateJson.parseRescore(renderedRescore);
    List<AggregationBuilder> aggregations = null;
    if (template.getAggregations() != null) {
      String renderedAggregations =
          SearchTemplateMustache.render(
              rewrite(template.getAggregations(), opContext, template), context);
      SearchTemplateJson.rejectForbidden(renderedAggregations, "aggregations");
      aggregations = SearchTemplateJson.parseAggregations(renderedAggregations);
      Integer requested = flags == null ? null : flags.getMaxAggValues();
      int cap =
          Math.min(requested == null ? 20 : requested, searchConfiguration.getMaxTermBucketSize());
      if (aggregations != null) {
        aggregations.forEach(
            aggregation -> SearchTemplateJson.clampAggregationSize(aggregation, cap));
      }
      aggregations = aggregations == null ? List.of() : aggregations;
    }
    List<SortBuilder<?>> sorts = null;
    if (template.getSort() != null) {
      String renderedSort =
          SearchTemplateMustache.render(rewrite(template.getSort(), opContext, template), context);
      SearchTemplateJson.rejectForbidden(renderedSort, "sort");
      sorts = SearchTemplateJson.parseSort(renderedSort);
    }
    if (template.getKnn() != null) {
      SearchTemplateJson.rejectForbidden(
          SearchTemplateMustache.render(rewrite(template.getKnn(), opContext, template), context),
          "knn");
    }
    rejectLeafCap(built, renderedRescore);
    Map<String, RenderedSearchTemplate.NamedClause> named =
        SearchTemplateMatchReport.namedClauses(built, properties);
    built = TemplateConfiguredScores.apply(opContext, template, entitySpecs, built);
    return new RenderedSearchTemplate(
        built,
        rescore,
        aggregations,
        sorts,
        highlights,
        queriedFields,
        query,
        named,
        estimatedClauses);
  }

  private record TemplateContext(@Nonnull Map<String, Object> values, int estimatedClauses) {}

  /**
   * The write-time sample cannot see every score function on the request schema. Count the DSL that
   * will be sent.
   */
  private void rejectLeafCap(@Nonnull QueryBuilder built, @Nullable String rescore) {
    if (searchConfiguration.getV3() == null || searchConfiguration.getV3().getTemplates() == null) {
      return;
    }
    SearchTemplateConfiguration templates = searchConfiguration.getV3().getTemplates();
    // A plain SearchConfiguration leaves the cap at 0 and templates disabled. That is not a
    // configured limit. An enabled template set with a non-positive cap still rejects the render.
    if (!templates.isEnabled() && templates.getMaxLeafClauses() <= 0) {
      return;
    }
    int cap = templates.getMaxLeafClauses();
    int leaves = SearchTemplateJson.countLeaves(built.toString());
    leaves += SearchTemplateJson.rescoreLeaves(rescore);
    if (leaves > cap) {
      throw new SearchTemplateException(
          "Search template has " + leaves + " leaf clauses and the limit is " + cap);
    }
  }

  @Nonnull
  private TemplateContext context(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      @Nonnull MergedSearchTemplate template,
      @Nonnull Set<SearchFieldConfig> queried) {
    String text = query == null ? "" : query;
    boolean structured = structuredTemplate(template);
    if (structured && text.startsWith(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX)) {
      text = text.substring(SearchQueryBuilder.STRUCTURED_QUERY_PREFIX.length());
    }
    text = text.replaceFirst("^:+", "");
    ExactMatchConfiguration exact =
        searchConfiguration.getExactMatch() == null
            ? new ExactMatchConfiguration()
            : searchConfiguration.getExactMatch();
    WordGramConfiguration grams =
        searchConfiguration.getWordGram() == null
            ? new WordGramConfiguration()
            : searchConfiguration.getWordGram();
    PartialConfiguration partial =
        searchConfiguration.getPartial() == null
            ? new PartialConfiguration()
            : searchConfiguration.getPartial();
    String unquoted = CustomizedQueryHandler.unquote(text);
    SearchClauseBudget.FieldCounts fieldCounts =
        clauseBudgetCounts(opContext, entitySpecs, template);
    String budgeted = SearchClauseBudget.firstTermsWithinBudget(text, fieldCounts);
    String operatorEscaped = V3SearchQueryBuilder.escapeSimpleQueryStringOperators(budgeted);
    SearchTemplateNormalizationPolicy.Resolved requestedNormalization =
        SearchTemplateNormalizationPolicy.resolve(template.getNormalization());
    SearchTemplateNormalizationPolicy.Resolved normalization =
        SearchTemplateNormalizationPolicy.budgetClamp(
            requestedNormalization, operatorEscaped, fieldCounts);
    SearchTemplateQueryShape shape =
        SearchTemplateQueryShape.of(unquoted, normalization.wildcardMinimumLength());
    String sanitized = V3SearchQueryBuilder.splitAlphanumericTokens(operatorEscaped);
    String fuzzyCandidate =
        V3SearchQueryBuilder.makeFuzzyQuery(
            sanitized,
            normalization.fuzzyTokenThreshold(),
            normalization.mediumFuzzyTokenThreshold(),
            normalization.shortFuzzyEdits(),
            normalization.mediumFuzzyEdits());
    int fuzzyMaxExpansions =
        SearchClauseBudget.fuzzyExpansions(sanitized, operatorEscaped, fieldCounts, fuzzyCandidate);
    String fuzzy = fuzzyMaxExpansions > 0 ? fuzzyCandidate : sanitized;
    // A spaceless id is a lookup. Edit distance would turn orderz_20240101 into orderz~1.
    if (SearchTemplateQueryShape.stopOnEmptyLight(shape.raw)) {
      fuzzy = sanitized;
    }
    String broadQuery =
        SearchTemplateNormalizationPolicy.firstTokens(
            operatorEscaped, normalization.broadQueryTokenCap());
    if (!budgeted.equals(text)) {
      SearchTemplateMetrics.clauseBudget(opContext, "terms");
    }
    if (!CustomizedQueryHandler.isQuoted(sanitized)
        && fuzzyCandidate.indexOf('~') >= 0
        && fuzzyMaxExpansions < SearchClauseBudget.MAX_FUZZY_EXPANSIONS) {
      SearchTemplateMetrics.clauseBudget(opContext, "fuzzy");
    }
    if (normalization.synonymCap() < requestedNormalization.synonymCap()) {
      SearchTemplateMetrics.clauseBudget(opContext, "synonyms");
    }
    int estimatedClauses = SearchClauseBudget.estimatedTermClauses(operatorEscaped, fieldCounts);
    List<SearchFieldConfig> queriedFields = withoutStructuredProperties(queried);
    List<String> analyzed = defaultQueryFields(queried);
    List<String> identityFields = SearchTemplateFields.identityAnalyzedFields(queriedFields);
    List<String> lightFields = shape.nameOnly ? identityFields : analyzed;
    // A spaceless path scores qualifiedName only through the bounded clause. Leaving it in this
    // best_fields list lets one earlier token on a shorter name outrank that segment match.
    if (shape.spacelessDotted) {
      lightFields = SearchTemplateFields.withoutLabel(lightFields, "qualifiedName");
    }
    List<String> nameFields =
        SearchTemplateFields.labelAnalyzedFields(
            queriedFields, SearchTemplateFields.ENTITY_NAME_FIELD);
    List<String> qualifiedNameFields =
        SearchTemplateFields.labelAnalyzedFields(queriedFields, "qualifiedName");
    List<String> defaultWithoutQualifiedName =
        SearchTemplateFields.withoutLabel(analyzed, "qualifiedName");
    SearchFieldResolver fieldResolver =
        V3SearchFieldModel.forSearchableEntities(opContext.getEntityRegistry()).resolver();
    Map<String, Object> context = new LinkedHashMap<>();
    context.put(SearchTemplateContextVariables.DEFAULT_QUERY_FIELDS, analyzed);
    context.put(
        SearchTemplateContextVariables.DEFAULT_QUERY_FIELDS_JSON,
        SearchTemplateMustache.json(analyzed));
    context.put(
        SearchTemplateContextVariables.DEFAULT_FIELDS_WITHOUT_QUALIFIED_NAME_JSON,
        SearchTemplateMustache.json(defaultWithoutQualifiedName));
    context.put(
        SearchTemplateContextVariables.IDENTITY_QUERY_FIELDS_JSON,
        SearchTemplateMustache.json(identityFields));
    context.put(
        SearchTemplateContextVariables.NAME_FIELDS_JSON, SearchTemplateMustache.json(nameFields));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_FIELDS_JSON,
        SearchTemplateMustache.json(qualifiedNameFields));
    if (!qualifiedNameFields.isEmpty()) {
      context.put(SearchTemplateContextVariables.QUALIFIED_NAME_FIELDS, Boolean.TRUE);
    }
    context.put(
        SearchTemplateContextVariables.LIGHT_FIELDS_JSON, SearchTemplateMustache.json(lightFields));
    context.put(
        SearchTemplateContextVariables.LIGHT_OPERATOR,
        shape.fullyQualifiedName || shape.longExactName ? "AND" : "OR");
    putWildcardFields(context, queriedFields);
    putPrefixFields(context, queriedFields, exact);
    context.put(
        SearchTemplateContextVariables.ENTITY_NAME_FIELD,
        exactPath(fieldResolver, "name", SearchFieldResolver.Intent.EXACT, "name"));
    context.put(
        SearchTemplateContextVariables.DESCRIPTION_FIELD,
        analyzedPath(fieldResolver, "description"));
    context.put(
        SearchTemplateContextVariables.PLATFORM_FIELD,
        exactPath(fieldResolver, "platform", SearchFieldResolver.Intent.FILTER, "platform_urn"));
    context.put(
        SearchTemplateContextVariables.COLUMN_NAME_FIELD,
        exactPath(fieldResolver, "fieldPaths", SearchFieldResolver.Intent.EXACT, "name_1"));
    String urnExact = exactPath(fieldResolver, "urn", SearchFieldResolver.Intent.EXACT, "urn");
    context.put(SearchTemplateContextVariables.URN_FIELD, urnExact);
    context.put(SearchTemplateContextVariables.URN_EXACT_FIELD, urnExact);
    context.put(
        SearchTemplateContextVariables.URN_FOLDED_FIELD, fieldResolver.lowercaseFullUrnPath("urn"));
    context.put(
        SearchTemplateContextVariables.URN_PARSED_FIELD,
        fieldResolver.lowercaseFullUrnPath("urn_name"));
    context.put(
        SearchTemplateContextVariables.NORMALIZED_KEYWORD, ESUtils.keywordNormalize(unquoted));
    context.put(
        SearchTemplateContextVariables.NAME_KEYWORD,
        exactPath(fieldResolver, "name", SearchFieldResolver.Intent.EXACT, "name"));
    context.put(SearchTemplateContextVariables.NAME_TEXT, analyzedPath(fieldResolver, "name"));
    context.put(SearchTemplateContextVariables.PREFIX_FIELD, analyzedPath(fieldResolver, "name"));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_KEYWORD,
        exactPath(
            fieldResolver, "qualifiedName", SearchFieldResolver.Intent.EXACT, "qualifiedName"));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_TEXT,
        analyzedPath(fieldResolver, "qualifiedName"));
    context.put(
        SearchTemplateContextVariables.QUALIFIED_NAME_IDENTITY,
        analyzedPath(fieldResolver, "qualifiedName"));
    context.put(
        SearchTemplateContextVariables.DESCRIPTION_TEXT,
        analyzedPath(fieldResolver, "description"));
    context.put(
        SearchTemplateContextVariables.ID_KEYWORD,
        exactPath(fieldResolver, "id", SearchFieldResolver.Intent.EXACT, "id"));
    context.put(SearchTemplateContextVariables.URN_KEYWORD, urnExact);
    context.put(SearchTemplateContextVariables.ENTITY_TYPES, entityTypeNames(entitySpecs));
    putScoreFunctions(context, entitySpecs);
    context.put(
        SearchTemplateContextVariables.QUERY,
        !structured && !shape.normalized.isEmpty() ? shape.normalized : text);
    context.put(SearchTemplateContextVariables.RAW, shape.raw);
    context.put(SearchTemplateContextVariables.ESCAPED, shape.escaped);
    context.put(SearchTemplateContextVariables.NORMALIZED, shape.unsplit);
    context.put(SearchTemplateContextVariables.ALPHANUMERIC_SPLIT, shape.normalized);
    context.put(SearchTemplateContextVariables.UNQUOTED_QUERY, unquoted);
    context.put(SearchTemplateContextVariables.UNQUOTED, unquoted);
    context.put(SearchTemplateContextVariables.FUZZY_QUERY, fuzzy);
    context.put(SearchTemplateContextVariables.FUZZY, fuzzy);
    context.put(SearchTemplateContextVariables.FUZZY_MAX_EXPANSIONS, fuzzyMaxExpansions);
    context.put(SearchTemplateContextVariables.BROAD_QUERY, broadQuery);
    if (structured) {
      context.put(
          SearchTemplateContextVariables.STRUCTURED_QUERY,
          structuredFragment(opContext, entitySpecs, text, new ArrayList<>(queried)));
    }
    context.put(
        SearchTemplateContextVariables.PREFIX_MAX_EXPANSIONS,
        SearchClauseBudget.prefixExpansions(operatorEscaped));
    context.put(SearchTemplateContextVariables.LIGHT_QUERY_TEXT, shape.lightQueryText);
    context.put(SearchTemplateContextVariables.SHORTENED, shape.shortened);
    context.put(SearchTemplateContextVariables.LAST_SEGMENT, shape.lastSegment);
    context.put(
        SearchTemplateContextVariables.NORMALIZED_LAST_SEGMENT,
        ESUtils.keywordNormalize(shape.lastSegment));
    context.put(SearchTemplateContextVariables.PREFIX_FACTOR, exactNumber(exact.getPrefixFactor()));
    context.put(SearchTemplateContextVariables.EXACT_FACTOR, exactNumber(exact.getExactFactor()));
    context.put(
        SearchTemplateContextVariables.CASE_SENSITIVITY_FACTOR,
        exactNumber(exact.getCaseSensitivityFactor()));
    context.put(
        SearchTemplateContextVariables.TWO_GRAM_FACTOR, exactNumber(grams.getTwoGramFactor()));
    context.put(
        SearchTemplateContextVariables.THREE_GRAM_FACTOR, exactNumber(grams.getThreeGramFactor()));
    context.put(
        SearchTemplateContextVariables.FOUR_GRAM_FACTOR, exactNumber(grams.getFourGramFactor()));
    context.put(SearchTemplateContextVariables.URN_FACTOR, exactNumber(partial.getUrnFactor()));
    context.put(SearchTemplateContextVariables.PARTIAL_FACTOR, exactNumber(partial.getFactor()));
    putQueryShape(context, shape, normalization.synonymCap());
    context.putAll(template.getParameters());
    String blob =
        String.join(
            "\n",
            template.getQuery() == null ? "" : template.getQuery(),
            template.getClauses() == null ? "" : template.getClauses(),
            template.getFunctionScore() == null ? "" : template.getFunctionScore(),
            String.join("\n", template.getQueryFragments().values()));
    if (template.isClasspathBridge()) {
      context.put(SearchTemplateViewer.URN, SearchTemplateViewer.urn(opContext));
      context.put(SearchTemplateViewer.GROUPS, "[]");
      context.put(SearchTemplateViewer.DOMAINS, "[]");
      context.put(SearchTemplateViewer.DOMAIN_LIST, "[]");
    } else {
      context.put(SearchTemplateViewer.URN, SearchTemplateViewer.urn(opContext));
      context.put(
          SearchTemplateViewer.GROUPS,
          blob.contains(SearchTemplateViewer.GROUPS)
              ? SearchTemplateViewer.groupsJson(opContext)
              : "[]");
      context.put(
          SearchTemplateViewer.DOMAINS,
          blob.contains(SearchTemplateViewer.DOMAINS)
              ? SearchTemplateViewer.domainsJson(opContext)
              : "[]");
      context.put(
          SearchTemplateViewer.DOMAIN_LIST,
          blob.contains(SearchTemplateViewer.DOMAIN_LIST)
              ? SearchTemplateViewer.domainListJson(opContext)
              : "[]");
    }
    Map<String, Object> queryFlags = new LinkedHashMap<>();
    queryFlags.put(SearchTemplateContextVariables.ALPHANUMERIC_SPLIT, shape.alphanumericSplit);
    queryFlags.put(
        SearchTemplateContextVariables.EXACT_NAME_LETTER_DIGIT, shape.exactNameLetterDigit());
    context.put(SearchTemplateContextVariables.FLAGS, queryFlags);
    SearchTemplateContextVariables.requireProduced(
        context.keySet(), template.getParameters().keySet());
    return new TemplateContext(context, estimatedClauses);
  }

  private static void putScoreFunctions(
      @Nonnull Map<String, Object> context, @Nonnull List<EntitySpec> entitySpecs) {
    // Supply schema data to Mustache; the template remains the source of the function-score DSL.
    List<Map<String, Object>> valueFunctions = new ArrayList<>();
    Set<String> weightedFields = new HashSet<>();
    entitySpecs.stream()
        .flatMap(spec -> spec.getSearchableFieldSpecs().stream())
        .map(spec -> spec.getSearchableAnnotation())
        .filter(annotation -> weightedFields.add(annotation.getFieldName()))
        .forEach(
            annotation ->
                annotation
                    .getWeightsPerFieldValue()
                    .forEach(
                        (value, weight) -> {
                          Map<String, Object> function = new LinkedHashMap<>();
                          function.put(
                              SearchTemplateContextVariables.FIELD, annotation.getFieldName());
                          function.put(
                              SearchTemplateContextVariables.VALUE_JSON,
                              SearchTemplateMustache.json(value));
                          function.put(
                              SearchTemplateContextVariables.WEIGHT,
                              exactNumber(weight.floatValue()));
                          valueFunctions.add(function);
                        }));
    List<Map<String, Object>> fieldFunctions = new ArrayList<>();
    Set<String> scoreFields = new HashSet<>();
    entitySpecs.stream()
        .flatMap(spec -> spec.getSearchScoreFieldSpecs().stream())
        .map(spec -> spec.getSearchScoreAnnotation())
        .filter(annotation -> scoreFields.add(annotation.getFieldName()))
        .forEach(
            annotation -> {
              Map<String, Object> function = new LinkedHashMap<>();
              function.put(SearchTemplateContextVariables.FIELD, annotation.getFieldName());
              function.put(
                  SearchTemplateContextVariables.FACTOR,
                  exactNumber((float) annotation.getWeight()));
              function.put(
                  SearchTemplateContextVariables.MISSING,
                  exactNumber((float) annotation.getDefaultValue()));
              annotation
                  .getModifier()
                  .ifPresent(
                      modifier ->
                          function.put(
                              SearchTemplateContextVariables.MODIFIER, scoreModifier(modifier)));
              fieldFunctions.add(function);
            });
    context.put(SearchTemplateContextVariables.VALUE_SCORE_FUNCTIONS, valueFunctions);
    context.put(SearchTemplateContextVariables.FIELD_SCORE_FUNCTIONS, fieldFunctions);
  }

  @Nonnull
  private static String scoreModifier(@Nonnull SearchScoreAnnotation.Modifier modifier) {
    return switch (modifier) {
      case LOG -> "log1p";
      case LN -> "ln1p";
      case SQRT -> "sqrt";
      case SQUARE -> "square";
      case RECIPROCAL -> "reciprocal";
    };
  }

  @Nonnull
  private static Map<String, String> renderFragments(
      @Nonnull MergedSearchTemplate template,
      @Nonnull Map<String, Object> context,
      @Nonnull OperationContext opContext) {
    Map<String, String> filled = new LinkedHashMap<>();
    template
        .getQueryFragments()
        .forEach(
            (name, value) -> {
              if (value != null && !value.isEmpty()) {
                filled.put(name, value);
              }
            });
    SearchTemplateHoles.requirePresent(template.getQuery(), filled.keySet());
    Map<String, String> rendered = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : filled.entrySet()) {
      String value =
          SearchTemplateMustache.render(rewrite(entry.getValue(), opContext, template), context);
      if (!value.isBlank()) {
        rendered.put(entry.getKey(), value.trim());
      }
    }
    return rendered;
  }

  @Nullable
  private static String rewrite(
      @Nullable String text,
      @Nonnull OperationContext opContext,
      @Nonnull MergedSearchTemplate template) {
    if (template.isClasspathBridge()) {
      return StructuredPropertyReferences.omitStored(text);
    }
    return StructuredPropertyReferences.rewrite(text, opContext, opContext.getAspectRetriever());
  }

  @Nonnull
  private static List<SearchTemplateJson.TermsBoost> renderBoosts(
      @Nullable SearchTemplateBoostArray boosts,
      @Nonnull OperationContext opContext,
      @Nonnull StructuredPropertyReferences.Resolver resolver) {
    if (boosts == null || boosts.isEmpty()) {
      return List.of();
    }
    List<SearchTemplateJson.TermsBoost> rendered = new ArrayList<>();
    for (SearchTemplateBoost boost : boosts) {
      if (!SearchTemplateViewer.INJECTABLES.contains(boost.getValues())) {
        throw new SearchTemplateException(
            "Search template boost values are not valid: " + boost.getValues());
      }
      String field =
          resolver.resolve(boost.getField(), boost.hasSubfield() ? boost.getSubfield() : null);
      List<String> values = SearchTemplateViewer.list(opContext, boost.getValues());
      if (!values.isEmpty()) {
        rendered.add(new SearchTemplateJson.TermsBoost(field, values, boost.getWeight()));
      }
    }
    return rendered;
  }

  /**
   * The widest index in the request, after this template's add, remove, and replace. A {@code
   * fieldConfiguration} search flag is not applied. Structured-property fields stay in the count:
   * they are queried beside the template body.
   */
  @Nonnull
  private SearchClauseBudget.FieldCounts clauseBudgetCounts(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull MergedSearchTemplate template) {
    boolean bridge = template.isClasspathBridge();
    StructuredPropertyReferences.Resolver resolver =
        bridge
            ? StructuredPropertyReferences.omitting()
            : StructuredPropertyReferences.resolver(opContext, opContext.getAspectRetriever());
    int fields = 0;
    int wordGramFields = 0;
    for (List<EntitySpec> indexSpecs :
        entitySpecs.stream().collect(Collectors.groupingBy(V3IndexKeys::resolve)).values()) {
      List<SearchFieldConfig> applied =
          SearchTemplateFields.applyTemplate(
              keywordQueryBuilder.templateBaseFieldConfigs(opContext, indexSpecs),
              template.getFields(),
              resolver);
      Set<List<String>> plain = new HashSet<>();
      Set<List<String>> grams = new HashSet<>();
      for (SearchFieldConfig field : applied) {
        if (!field.isQueryByDefault()) {
          continue;
        }
        String analyzer = field.analyzer() == null ? "" : field.analyzer();
        if (analyzer.contains("word_gram")) {
          grams.add(List.of(analyzer, field.fieldName()));
        } else {
          plain.add(List.of(analyzer, field.fieldName()));
        }
      }
      fields = Math.max(fields, plain.size());
      wordGramFields = Math.max(wordGramFields, grams.size());
    }
    return new SearchClauseBudget.FieldCounts(fields, wordGramFields);
  }

  private static boolean structuredTemplate(@Nonnull MergedSearchTemplate template) {
    String urn = template.getUrn();
    return "urn:li:searchTemplate:structured".equals(urn)
        || "urn:li:searchTemplate:structuredExact".equals(urn);
  }

  private static void putQueryShape(
      @Nonnull Map<String, Object> context,
      @Nonnull SearchTemplateQueryShape shape,
      int synonymCap) {
    List<String> synonyms = shape.synonyms;
    if (synonymCap < synonyms.size()) {
      synonyms = synonymCap <= 0 ? List.of() : synonyms.subList(0, synonymCap);
    }
    if (!synonyms.isEmpty()) {
      context.put(SearchTemplateContextVariables.SYNONYM_QUERIES, synonyms);
      context.put(
          SearchTemplateContextVariables.SYNONYMS,
          SearchTemplateMustache.synonymElements(synonyms));
    }
    if (shape.wildcardTerm != null) {
      context.put(SearchTemplateContextVariables.WILDCARD_TERM, shape.wildcardTerm);
    }
    if (shape.multiWord) {
      context.put(SearchTemplateContextVariables.MULTI_WORD, Boolean.TRUE);
    }
    if (shape.longQuery) {
      context.put(SearchTemplateContextVariables.LONG_QUERY, Boolean.TRUE);
      context.put(SearchTemplateContextVariables.LONG_PHRASE, Boolean.TRUE);
    }
    if (shape.dotted) {
      context.put(SearchTemplateContextVariables.DOTTED_QUERY, Boolean.TRUE);
      context.put(SearchTemplateContextVariables.DOTTED, Boolean.TRUE);
    }
    if (shape.fullyQualifiedName) {
      context.put(SearchTemplateContextVariables.FULLY_QUALIFIED_NAME, Boolean.TRUE);
    }
    if (shape.deepPath) {
      context.put(SearchTemplateContextVariables.DEEP_PATH, Boolean.TRUE);
    }
    if (shape.longExactName) {
      context.put(SearchTemplateContextVariables.LONG_EXACT_NAME, Boolean.TRUE);
    }
    if (shape.operatorEscaped) {
      context.put(SearchTemplateContextVariables.OPERATOR_ESCAPED, Boolean.TRUE);
    }
    if (SearchTemplateQueryShape.stopOnEmptyLight(shape.raw)) {
      context.put(SearchTemplateContextVariables.SPACELESS_IDENTIFIER, Boolean.TRUE);
    }
    if (shape.wholeIdentifier != null) {
      context.put(SearchTemplateContextVariables.WHOLE_IDENTIFIER, shape.wholeIdentifier);
    }
    if (shape.letterDigitQuery != null) {
      context.put(SearchTemplateContextVariables.LETTER_DIGIT_PARTS, Boolean.TRUE);
      context.put(SearchTemplateContextVariables.LETTER_DIGIT_QUERY, shape.letterDigitQuery);
    }
    if (shape.fqnQuery != null) {
      context.put(SearchTemplateContextVariables.FQN_PARTS, Boolean.TRUE);
      context.put(SearchTemplateContextVariables.FQN_QUERY, shape.fqnQuery);
    }
    putSpacelessDotted(context, shape);
  }

  /**
   * Completeness tiers for a spaceless dotted query. The full path and the qualifier prefix are
   * each one bounded keyword match, so a deeper name does not add clauses. Patterns treat {@code
   * *}, {@code ?}, and {@code \} as literals. The name whole-identifier clause uses the full-path
   * patterns as a filter: the dataset key is indexed on {@code name}, and a shorter key that merely
   * contains the same tokens would otherwise outrank the bounded path. Patterns keep the written
   * case. A keyword-root wildcard cannot set {@code case_insensitive}: OpenSearch 3 fails that
   * shard. {@link SearchTemplateJson#parseQuery} drops the flag and adds the lowercase pattern.
   */
  private static void putSpacelessDotted(
      @Nonnull Map<String, Object> context, @Nonnull SearchTemplateQueryShape shape) {
    if (!shape.spacelessDotted) {
      return;
    }
    String terminal = shape.terminalSegment;
    List<String> qualifiers = new ArrayList<>(shape.qualifierSegments);
    List<String> segments = new ArrayList<>(qualifiers);
    segments.add(terminal);
    context.put(SearchTemplateContextVariables.SPACELESS_DOTTED, Boolean.TRUE);
    context.put(SearchTemplateContextVariables.TERMINAL_SEGMENT, terminal);
    putBoundedPath(
        context,
        String.join(".", segments),
        SearchTemplateContextVariables.DOTTED_PATH,
        SearchTemplateContextVariables.DOTTED_PATH_PREFIX,
        SearchTemplateContextVariables.DOTTED_PATH_SUFFIX,
        SearchTemplateContextVariables.DOTTED_PATH_CONTAINS);
    // One joined prefix, not one clause set per segment. The query size stays fixed as the path
    // gets deeper, and a single shared segment is not enough context.
    putBoundedPath(
        context,
        String.join(".", qualifiers),
        SearchTemplateContextVariables.QUALIFIER_PATH,
        SearchTemplateContextVariables.QUALIFIER_PATH_PREFIX,
        SearchTemplateContextVariables.QUALIFIER_PATH_SUFFIX,
        SearchTemplateContextVariables.QUALIFIER_PATH_CONTAINS);
  }

  private static void putBoundedPath(
      @Nonnull Map<String, Object> context,
      @Nonnull String path,
      @Nonnull String exact,
      @Nonnull String prefix,
      @Nonnull String suffix,
      @Nonnull String contains) {
    String literal = SearchTemplateQueryShape.wildcardLiteral(path);
    context.put(exact, path);
    context.put(prefix, literal + ".*");
    context.put(suffix, "*." + literal);
    context.put(contains, "*." + literal + ".*");
  }

  @Nonnull
  private static List<String> entityTypeNames(@Nonnull List<EntitySpec> entitySpecs) {
    List<String> names = new ArrayList<>();
    for (EntitySpec spec : entitySpecs) {
      if (!names.contains(spec.getName())) {
        names.add(spec.getName());
      }
    }
    return names;
  }

  /**
   * {@code (double) float} prints binary noise ({@code 1.1f} becomes {@code 1.100000023841858}).
   */
  private static double exactNumber(float value) {
    return Double.parseDouble(Float.toString(value));
  }

  /**
   * Same fields as {@code V3SearchQueryBuilder#getWildcardContainsQuery}: analyzed {@code name} and
   * {@code title} fields, each boosted by {@code fieldBoost * 0.3}. The pattern is already
   * lowercased, so the clause does not set {@code case_insensitive}.
   */
  private static final float WILDCARD_BOOST_FACTOR = 0.3f;

  /** Matches {@code V3SearchQueryBuilder}'s {@code EXACT_MATCH_BOOST_MULTIPLIER}. */
  private static final float PREFIX_BOOST_MULTIPLIER = 6.0f;

  private static void putWildcardFields(
      @Nonnull Map<String, Object> context, @Nonnull List<SearchFieldConfig> fields) {
    Map<String, Float> boosts = new LinkedHashMap<>();
    for (SearchFieldConfig field : fields) {
      if (!field.isDelimitedSubfield()
          || (!"name".equals(field.shortName()) && !"title".equals(field.shortName()))) {
        continue;
      }
      boosts.merge(field.fieldName(), field.boost(), Math::max);
    }
    if (boosts.isEmpty()) {
      return;
    }
    List<Map<String, Object>> clauses = new ArrayList<>();
    boosts.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              Map<String, Object> clause = new LinkedHashMap<>();
              clause.put(SearchTemplateContextVariables.FIELD, entry.getKey());
              clause.put(
                  SearchTemplateContextVariables.BOOST,
                  exactNumber(entry.getValue() * WILDCARD_BOOST_FACTOR));
              clauses.add(clause);
            });
    context.put(SearchTemplateContextVariables.WILDCARD_FIELDS, clauses);
  }

  /**
   * Analyzed {@code name} and {@code qualifiedName} {@code .text} and {@code .stemmed} fields. Each
   * boost is {@code fieldBoost * prefixFactor * caseFactor * 6}. Omitted when prefix matching is
   * off or no field qualifies. {@code exact} and {@code structured} do not read this list.
   */
  private static void putPrefixFields(
      @Nonnull Map<String, Object> context,
      @Nonnull List<SearchFieldConfig> fields,
      @Nonnull ExactMatchConfiguration exact) {
    if (!exact.isWithPrefix()) {
      return;
    }
    float caseFactor =
        exact.getCaseSensitivityFactor() > 0.0f ? exact.getCaseSensitivityFactor() : 1.0f;
    float scale = exact.getPrefixFactor() * caseFactor * PREFIX_BOOST_MULTIPLIER;
    Map<String, Float> boosts = new LinkedHashMap<>();
    for (SearchFieldConfig field : fields) {
      if (field.isWordGramSubfield()) {
        continue;
      }
      String shortName = field.shortName();
      if (!"name".equals(shortName) && !"qualifiedName".equals(shortName)) {
        continue;
      }
      String fieldName = field.fieldName();
      if (!fieldName.endsWith(V3SearchFields.TEXT_SUFFIX)
          && !fieldName.endsWith(V3SearchFields.STEMMED_SUFFIX)) {
        continue;
      }
      boosts.merge(fieldName, field.boost() * scale, Math::max);
    }
    if (boosts.isEmpty()) {
      return;
    }
    List<Map<String, Object>> clauses = new ArrayList<>();
    boosts.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              Map<String, Object> clause = new LinkedHashMap<>();
              clause.put(SearchTemplateContextVariables.FIELD, entry.getKey());
              clause.put(SearchTemplateContextVariables.BOOST, exactNumber(entry.getValue()));
              clauses.add(clause);
            });
    context.put(SearchTemplateContextVariables.PREFIX_FIELDS, clauses);
  }

  @Nonnull
  private static String exactPath(
      @Nonnull SearchFieldResolver resolver,
      @Nonnull String field,
      @Nonnull SearchFieldResolver.Intent intent,
      @Nonnull String fallback) {
    return resolver.supports(field, intent) ? resolver.exactPath(field, intent) : fallback;
  }

  @Nonnull
  private static String analyzedPath(@Nonnull SearchFieldResolver resolver, @Nonnull String field) {
    // A field without text or stemmed subfields has no .text mapping.
    if (!resolver.supports(field, SearchFieldResolver.Intent.FULL_TEXT)) {
      return exactPath(resolver, field, SearchFieldResolver.Intent.EXACT, field);
    }
    SearchFieldResolver.Field resolved =
        resolver.resolve(field, SearchFieldResolver.Intent.FULL_TEXT);
    if (resolved.getSubfields().contains(V3SearchFields.TEXT)) {
      return resolved.getPath() + V3SearchFields.TEXT_SUFFIX;
    }
    if (resolved.getSubfields().contains(V3SearchFields.STEMMED)) {
      return resolved.getPath() + V3SearchFields.STEMMED_SUFFIX;
    }
    return resolved.getPath();
  }

  /**
   * Property clauses the template enabled. Java resolves field paths from the current definitions
   * and does not add a clause the template left unset. A classpath bridge still runs analyzed and
   * exact clauses, and skips name-and-value because that clause reads stored display names.
   */
  @Nonnull
  private static QueryBuilder propertyClauses(
      @Nonnull QueryBuilder built,
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull List<StructuredPropertySearchCatalog.IndexedProperty> properties,
      @Nonnull String query,
      @Nullable StructuredPropertyQuery structuredPropertyQuery,
      boolean classpathBridge) {
    List<QueryBuilder> extras = new ArrayList<>();
    if (structuredPropertyQuery != null
        && StructuredPropertyClauses.runs(structuredPropertyQuery.getAnalyzedValue())) {
      extras.addAll(
          analyzedPropertyQueries(
              queried,
              properties,
              query,
              operator(structuredPropertyQuery.getAnalyzedValue()),
              StructuredPropertyClauses.weight(structuredPropertyQuery.getAnalyzedValue())));
    }
    if (structuredPropertyQuery != null
        && StructuredPropertyClauses.runs(structuredPropertyQuery.getExactValue())) {
      extras.addAll(
          valueTerms(
              queried,
              properties,
              query,
              StructuredPropertyClauses.weight(structuredPropertyQuery.getExactValue())));
    }
    if (!classpathBridge) {
      QueryBuilder nameAndValue = nameAndValue(queried, properties, query, structuredPropertyQuery);
      if (nameAndValue != null) {
        extras.add(nameAndValue);
      }
    }
    if (extras.isEmpty()) {
      return built;
    }
    BoolQueryBuilder combined = QueryBuilders.boolQuery().minimumShouldMatch(1);
    combined.should(built);
    extras.forEach(combined::should);
    return combined;
  }

  /**
   * The structured {@code /q} clause, built by {@link StructuredQueryRouting}. The template splices
   * this JSON. It is the user query only: authorization and system filters stay outside the body.
   */
  @Nonnull
  private static String structuredFragment(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      @Nonnull List<SearchFieldConfig> queried) {
    List<SearchFieldConfig> shared = new ArrayList<>();
    List<SearchFieldConfig> properties = new ArrayList<>();
    for (SearchFieldConfig field : queried) {
      if (StructuredPropertyUtils.isStructuredPropertyField(field.fieldName())) {
        properties.add(field);
      } else {
        shared.add(field);
      }
    }
    SearchFieldResolver resolver =
        V3SearchFieldModel.forSearchableEntities(opContext.getEntityRegistry()).resolver();
    Optional<QueryBuilder> built =
        StructuredQueryRouting.build(query, shared, properties, resolver);
    return built.map(QueryBuilder::toString).orElse("{\"match_none\":{}}");
  }

  @Nonnull
  private static Operator operator(@Nullable StructuredPropertyClause clause) {
    if (clause != null
        && clause.hasOperator()
        && clause.getOperator() == StructuredPropertyOperator.AND) {
      return Operator.AND;
    }
    return Operator.OR;
  }

  @Nonnull
  private static List<QueryBuilder> analyzedPropertyQueries(
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull List<StructuredPropertySearchCatalog.IndexedProperty> properties,
      @Nonnull String query,
      @Nonnull Operator operator,
      float weight) {
    Map<String, List<SearchFieldConfig>> byAnalyzer =
        queried.stream()
            .filter(field -> property(properties, field.fieldName()) != null)
            .filter(
                field ->
                    field.fieldName().endsWith(V3SearchFields.TEXT_SUFFIX)
                        || field.fieldName().endsWith(V3SearchFields.STEMMED_SUFFIX))
            .filter(field -> field.analyzer() != null)
            .collect(java.util.stream.Collectors.groupingBy(SearchFieldConfig::analyzer));
    List<QueryBuilder> clauses = new ArrayList<>();
    byAnalyzer.forEach(
        (analyzer, fields) -> {
          SimpleQueryStringBuilder clause =
              QueryBuilders.simpleQueryStringQuery(query)
                  .analyzer(analyzer)
                  .defaultOperator(operator)
                  .lenient(true);
          fields.forEach(
              field ->
                  clause.field(
                      field.fieldName(), (field.boost() == null ? 1f : field.boost()) * weight));
          clauses.add(clause);
        });
    return clauses;
  }

  @Nonnull
  private static List<QueryBuilder> valueTerms(
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull List<StructuredPropertySearchCatalog.IndexedProperty> properties,
      @Nonnull String query,
      float weight) {
    String text = CustomizedQueryHandler.unquote(query);
    SearchTemplateQueryShape shape = SearchTemplateQueryShape.of(text);
    String termValue = shape.nameOnly ? shape.raw : query.trim();
    if (termValue.isEmpty()) {
      return List.of();
    }
    List<QueryBuilder> terms = new ArrayList<>();
    Map<StructuredPropertySearchCatalog.IndexedProperty, Float> selected = new LinkedHashMap<>();
    for (SearchFieldConfig field : queried) {
      StructuredPropertySearchCatalog.IndexedProperty property =
          property(properties, field.fieldName());
      if (property != null) {
        float boost = (field.boost() == null ? 1f : field.boost()) * weight;
        selected.merge(property, boost, Math::max);
      }
    }
    selected.forEach(
        (property, boost) -> {
          terms.add(QueryBuilders.termQuery(property.fieldPath(), termValue).boost(boost));
        });
    return terms;
  }

  @Nullable
  private static QueryBuilder nameAndValue(
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull List<StructuredPropertySearchCatalog.IndexedProperty> properties,
      @Nonnull String query,
      @Nullable StructuredPropertyQuery structuredPropertyQuery) {
    if (structuredPropertyQuery == null
        || !StructuredPropertyClauses.runs(structuredPropertyQuery.getNameAndValue())) {
      return null;
    }
    StructuredPropertyClause clause = structuredPropertyQuery.getNameAndValue();
    if (clause == null || !clause.hasWeight() || clause.getWeight() == null) {
      return null;
    }
    float weight = clause.getWeight();
    if (!Float.isFinite(weight) || !(weight > 0f)) {
      return null;
    }
    return StructuredPropertyNameValue.disMax(query, queried, properties, weight);
  }

  @Nonnull
  private static List<String> searchedFields(@Nonnull Set<SearchFieldConfig> queried) {
    List<String> names = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (SearchFieldConfig field : queried) {
      if (seen.add(field.fieldName())) {
        names.add(field.fieldName());
      }
    }
    return names;
  }

  @Nonnull
  private static List<SearchFieldConfig> withoutStructuredProperties(
      @Nonnull Set<SearchFieldConfig> queried) {
    List<SearchFieldConfig> fields = new ArrayList<>();
    for (SearchFieldConfig field : queried) {
      if (propertyBase(field.fieldName()) == null) {
        fields.add(field);
      }
    }
    return fields;
  }

  @Nullable
  private static String propertyBase(@Nonnull String fieldName) {
    if (!fieldName.startsWith(STRUCTURED_PROPERTY_MAPPING_FIELD + ".")) {
      return null;
    }
    return StructuredPropertySubfields.strip(fieldName);
  }

  @Nullable
  private static StructuredPropertySearchCatalog.IndexedProperty property(
      @Nonnull List<StructuredPropertySearchCatalog.IndexedProperty> properties,
      @Nonnull String fieldName) {
    for (StructuredPropertySearchCatalog.IndexedProperty property : properties) {
      if (fieldName.equals(property.fieldPath())
          || fieldName.startsWith(property.fieldPath() + ".")
          || fieldName.equals(property.fullTextPath())
          || fieldName.startsWith(property.fullTextPath() + ".")) {
        return property;
      }
    }
    return null;
  }

  @Nonnull
  private static List<String> defaultQueryFields(@Nonnull Set<SearchFieldConfig> queried) {
    return SearchTemplateFields.analyzedFields(withoutStructuredProperties(queried));
  }

  @Nonnull
  private static Set<String> removedFields(
      @Nonnull List<SearchFieldConfig> before, @Nonnull Set<SearchFieldConfig> after) {
    Set<String> kept = new HashSet<>();
    for (SearchFieldConfig field : after) {
      kept.add(indexedBase(field.fieldName()));
    }
    Set<String> removed = new LinkedHashSet<>();
    for (SearchFieldConfig field : before) {
      String name = indexedBase(field.fieldName());
      if (!kept.contains(name)) {
        removed.add(name);
      }
    }
    return removed;
  }

  @Nonnull
  private static String indexedBase(@Nonnull String fieldName) {
    String base = propertyBase(fieldName);
    return base == null ? fieldName : base;
  }
}
