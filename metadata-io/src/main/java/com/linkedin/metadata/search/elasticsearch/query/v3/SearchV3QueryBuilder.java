package com.linkedin.metadata.search.elasticsearch.query.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;

import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.V3SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.template.MergedSearchTemplate;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateDialects;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFields;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateHoles;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateLoader;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateQueryShape;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateViewer;
import com.linkedin.metadata.search.elasticsearch.query.template.StructuredPropertyReferences;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateBoostArray;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.StructuredPropertyQuery;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.SortBuilder;

/** Builds the Elasticsearch request for a resolved Search V3 template. */
public class SearchV3QueryBuilder {
  private final SearchTemplateLoader loader;
  private final SearchConfiguration searchConfiguration;
  private final V3SearchQueryBuilder keywordQueryBuilder;

  public SearchV3QueryBuilder(
      @Nonnull SearchTemplateLoader loader,
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this.loader = loader;
    this.searchConfiguration = searchConfiguration;
    this.keywordQueryBuilder =
        new V3SearchQueryBuilder(searchConfiguration, customSearchConfiguration);
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
  public RenderedSearchTemplate render(
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
        new ArrayList<>(keywordQueryBuilder.searchedFieldConfigs(opContext, entitySpecs));
    boolean bridge = template.isClasspathBridge();
    if (!bridge) {
      for (SearchFieldConfig property :
          StructuredPropertyQueryFields.fieldsFor(entityTypeNames(entitySpecs))) {
        if (base.stream().noneMatch(field -> field.fieldName().equals(property.fieldName()))) {
          base.add(property);
        }
      }
    }
    StructuredPropertyReferences.Resolver resolver =
        bridge
            ? StructuredPropertyReferences.omitting()
            : StructuredPropertyReferences.resolver(opContext, opContext.getAspectRetriever());
    List<SearchFieldConfig> layer2 =
        SearchTemplateFields.applyTemplate(base, template.getFields(), resolver);
    Set<SearchFieldConfig> queried = new LinkedHashSet<>(layer2);

    List<String> highlights =
        SearchTemplateFields.applyHighlight(Set.of(), template.getHighlightFields());
    if (flags != null && flags.hasCustomHighlightingFields()) {
      highlights = new ArrayList<>(flags.getCustomHighlightingFields());
    }
    if (flags != null && Boolean.TRUE.equals(flags.isSkipHighlighting())) {
      highlights = List.of();
    }
    List<String> queriedFields = searchedFields(queried);

    Map<String, Object> context = context(opContext, entitySpecs, query, template, queried);
    context.putAll(renderFragments(template, context, opContext));
    String body = rewrite(template.getQuery(), opContext, template);
    String renderedQuery = body == null ? null : SearchTemplateMustache.render(body, context);
    QueryBuilder built =
        renderedQuery == null || renderedQuery.isBlank()
            ? QueryBuilders.matchAllQuery()
            : SearchTemplateJson.parseQuery(renderedQuery);
    if (!template.isClasspathBridge()) {
      built = propertyClauses(built, queried, query, template.getStructuredPropertyQuery());
    }
    if (template.getClauses() != null) {
      built =
          SearchTemplateJson.mergeClauses(
              built,
              SearchTemplateMustache.render(
                  rewrite(template.getClauses(), opContext, template), context));
    }
    built = SearchTemplateFieldPruner.prune(built, removedFields(base, queried));
    String functionScore =
        template.getFunctionScore() == null
            ? null
            : SearchTemplateMustache.render(
                rewrite(template.getFunctionScore(), opContext, template), context);
    List<SearchTemplateJson.TermsBoost> boosts =
        template.isClasspathBridge()
            ? List.of()
            : renderBoosts(template.getBoosts(), opContext, resolver);
    if (functionScore != null || !boosts.isEmpty()) {
      built = SearchTemplateJson.wrapFunctionScore(built, functionScore, boosts);
    }

    QueryRescorerBuilder rescore =
        template.getRescore() != null && !scroll
            ? SearchTemplateJson.parseRescore(
                SearchTemplateMustache.render(
                    rewrite(template.getRescore(), opContext, template), context))
            : null;
    List<AggregationBuilder> aggregations = null;
    if (template.getAggregations() != null) {
      aggregations =
          SearchTemplateJson.parseAggregations(
              SearchTemplateMustache.render(
                  rewrite(template.getAggregations(), opContext, template), context));
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
      sorts =
          SearchTemplateJson.parseSort(
              SearchTemplateMustache.render(
                  rewrite(template.getSort(), opContext, template), context));
    }
    return new RenderedSearchTemplate(
        built,
        rescore,
        aggregations,
        sorts,
        highlights,
        queriedFields,
        query,
        SearchTemplateMatchReport.namedClauses(built));
  }

  @Nonnull
  private Map<String, Object> context(
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
    SearchTemplateQueryShape shape = SearchTemplateQueryShape.of(unquoted);
    List<SearchFieldConfig> queriedFields = withoutStructuredProperties(queried);
    List<String> analyzed = defaultQueryFields(queried);
    List<String> identityFields = SearchTemplateFields.identityAnalyzedFields(queriedFields);
    List<String> lightFields = shape.nameOnly ? identityFields : analyzed;
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("defaultQueryFields", analyzed);
    context.put("defaultQueryFieldsJson", SearchTemplateMustache.json(analyzed));
    context.put("identityQueryFieldsJson", SearchTemplateMustache.json(identityFields));
    context.put("lightFieldsJson", SearchTemplateMustache.json(lightFields));
    context.put("lightOperator", shape.deepPath || shape.longExactName ? "AND" : "OR");
    context.put("entityNameField", SearchTemplateFields.keywordPath("name"));
    context.put("descriptionField", SearchTemplateFields.textPath("description"));
    context.put("platformField", SearchTemplateFields.PLATFORM_FIELD);
    context.put("columnNameField", SearchTemplateFields.keywordPath("columns"));
    context.put("urnField", SearchTemplateFields.URN_FIELD);
    context.put("nameKeyword", SearchTemplateFields.keywordPath("name"));
    context.put("nameText", SearchTemplateFields.textPath("name"));
    context.put("qualifiedNameKeyword", SearchTemplateFields.keywordPath("qualifiedName"));
    context.put("qualifiedNameText", SearchTemplateFields.textPath("qualifiedName"));
    context.put("qualifiedNameIdentity", "qualifiedName");
    context.put("descriptionText", SearchTemplateFields.textPath("description"));
    context.put("idKeyword", "id.keyword");
    context.put("urnKeyword", SearchTemplateFields.URN_FIELD);
    context.put("entityTypes", entityTypeNames(entitySpecs));
    putScoreFunctions(context, entitySpecs);
    context.put("query", !structured && !shape.normalized.isEmpty() ? shape.normalized : text);
    context.put("raw", shape.raw);
    context.put("escaped", shape.escaped);
    context.put("normalized", shape.unsplit);
    context.put("alphanumericSplit", shape.normalized);
    context.put("unquotedQuery", unquoted);
    context.put("unquoted", unquoted);
    context.put("fuzzyQuery", limitFuzzy(shape));
    context.put("fuzzy", limitFuzzy(shape));
    context.put("lightQueryText", shape.lightQueryText);
    context.put("shortened", shape.shortened);
    context.put("lastSegment", shape.lastSegment);
    context.put("prefixFactor", exactNumber(exact.getPrefixFactor()));
    context.put("exactFactor", exactNumber(exact.getExactFactor()));
    context.put("caseSensitivityFactor", exactNumber(exact.getCaseSensitivityFactor()));
    context.put("twoGramFactor", exactNumber(grams.getTwoGramFactor()));
    context.put("threeGramFactor", exactNumber(grams.getThreeGramFactor()));
    context.put("fourGramFactor", exactNumber(grams.getFourGramFactor()));
    context.put("urnFactor", exactNumber(partial.getUrnFactor()));
    context.put("partialFactor", exactNumber(partial.getFactor()));
    putQueryShape(context, shape);
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
    queryFlags.put("alphanumericSplit", shape.alphanumericSplit);
    context.put("flags", queryFlags);
    return context;
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
                          function.put("field", annotation.getFieldName());
                          function.put("valueJson", SearchTemplateMustache.json(value));
                          function.put("weight", exactNumber(weight.floatValue()));
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
              function.put("field", annotation.getFieldName());
              function.put("factor", exactNumber((float) annotation.getWeight()));
              function.put("missing", exactNumber((float) annotation.getDefaultValue()));
              annotation
                  .getModifier()
                  .ifPresent(modifier -> function.put("modifier", scoreModifier(modifier)));
              fieldFunctions.add(function);
            });
    context.put("valueScoreFunctions", valueFunctions);
    context.put("fieldScoreFunctions", fieldFunctions);
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

  /** Drops fuzzy expansions once the token list no longer fits the per-request clause budget. */
  @Nonnull
  private static String limitFuzzy(@Nonnull SearchTemplateQueryShape shape) {
    if (shape.normalized.isEmpty()) {
      return shape.fuzzyQuery;
    }
    int tokens = shape.normalized.split(" ").length;
    if (tokens > 32) {
      return shape.normalized;
    }
    return shape.fuzzyQuery;
  }

  private static boolean structuredTemplate(@Nonnull MergedSearchTemplate template) {
    String urn = template.getUrn();
    return "urn:li:searchTemplate:structured".equals(urn)
        || "urn:li:searchTemplate:structuredExact".equals(urn);
  }

  private static void putQueryShape(
      @Nonnull Map<String, Object> context, @Nonnull SearchTemplateQueryShape shape) {
    List<String> synonyms = shape.synonyms;
    if (synonyms.size() > 8) {
      synonyms = synonyms.subList(0, 8);
    }
    if (!synonyms.isEmpty()) {
      context.put("synonymQueries", synonyms);
      context.put("synonyms", synonyms);
    }
    if (shape.wildcardTerm != null) {
      context.put("wildcardTerm", shape.wildcardTerm);
    }
    if (shape.multiWord) {
      context.put("multiWord", Boolean.TRUE);
    }
    if (shape.longQuery) {
      context.put("longQuery", Boolean.TRUE);
      context.put("longPhrase", Boolean.TRUE);
    }
    if (shape.dotted) {
      context.put("dottedQuery", Boolean.TRUE);
      context.put("dotted", Boolean.TRUE);
    }
    if (shape.fullyQualifiedName) {
      context.put("fullyQualifiedName", Boolean.TRUE);
    }
    if (shape.deepPath) {
      context.put("deepPath", Boolean.TRUE);
    }
    if (shape.longExactName) {
      context.put("longExactName", Boolean.TRUE);
    }
    if (shape.operatorEscaped) {
      context.put("operatorEscaped", Boolean.TRUE);
    }
    if (SearchTemplateQueryShape.stopOnEmptyLight(shape.raw)) {
      context.put("spacelessIdentifier", Boolean.TRUE);
    }
    if (shape.wholeIdentifier != null) {
      context.put("wholeIdentifier", shape.wholeIdentifier);
    }
    if (shape.letterDigitQuery != null) {
      context.put("letterDigitParts", Boolean.TRUE);
      context.put("letterDigitQuery", shape.letterDigitQuery);
    }
    if (shape.fqnQuery != null) {
      context.put("fqnParts", Boolean.TRUE);
      context.put("fqnQuery", shape.fqnQuery);
    }
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
   * String, rich-text, and URN properties match as one whole term. Name-and-value matching adds one
   * {@code dis_max} for string and rich-text properties still in the queried set. Both join the
   * rendered body before {@code clauses} are merged around that query. A date or number stays out
   * of both.
   */
  @Nonnull
  private static QueryBuilder propertyClauses(
      @Nonnull QueryBuilder built,
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull String query,
      @Nullable StructuredPropertyQuery structuredPropertyQuery) {
    List<QueryBuilder> extras = new ArrayList<>(valueTerms(queried, query));
    QueryBuilder nameAndValue = nameAndValue(queried, query, structuredPropertyQuery);
    if (nameAndValue != null) {
      extras.add(nameAndValue);
    }
    if (extras.isEmpty()) {
      return built;
    }
    BoolQueryBuilder combined = QueryBuilders.boolQuery().minimumShouldMatch(1);
    combined.should(built);
    extras.forEach(combined::should);
    return combined;
  }

  @Nonnull
  private static List<QueryBuilder> valueTerms(
      @Nonnull Set<SearchFieldConfig> queried, @Nonnull String query) {
    String text = CustomizedQueryHandler.unquote(query);
    SearchTemplateQueryShape shape = SearchTemplateQueryShape.of(text);
    String termValue = shape.nameOnly ? shape.raw : query.trim();
    if (termValue.isEmpty()) {
      return List.of();
    }
    List<QueryBuilder> terms = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (SearchFieldConfig field : queried) {
      String base = propertyBase(field.fieldName());
      if (base == null || !seen.add(base) || StructuredPropertyQueryFields.omitted(base)) {
        continue;
      }
      Optional<LogicalValueType> type = StructuredPropertyQueryFields.valueType(base);
      if (type.isEmpty() || !wholeValue(type.get())) {
        continue;
      }
      String termField =
          StructuredPropertyUtils.usesKeywordSubfield(type.get()) ? base + ".keyword" : base;
      float boost = field.boost() == null ? 1f : field.boost();
      terms.add(QueryBuilders.termQuery(termField, termValue).boost(boost));
    }
    return terms;
  }

  @Nullable
  private static QueryBuilder nameAndValue(
      @Nonnull Set<SearchFieldConfig> queried,
      @Nonnull String query,
      @Nullable StructuredPropertyQuery structuredPropertyQuery) {
    if (structuredPropertyQuery == null || !structuredPropertyQuery.hasNameAndValueWeight()) {
      return null;
    }
    Float weight = structuredPropertyQuery.getNameAndValueWeight();
    if (weight == null) {
      return null;
    }
    return StructuredPropertyNameValue.disMax(query, queried, weight);
  }

  @Nonnull
  private static List<String> searchedFields(@Nonnull Set<SearchFieldConfig> queried) {
    List<String> names = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (SearchFieldConfig field : queried) {
      String base = propertyBase(field.fieldName());
      if (base != null) {
        Optional<LogicalValueType> type = StructuredPropertyQueryFields.valueType(base);
        if (type.isPresent() && wholeValue(type.get()) && seen.add(base)) {
          names.add(base);
        }
        continue;
      }
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
    if (fieldName.endsWith(".keyword")) {
      return fieldName.substring(0, fieldName.length() - ".keyword".length());
    }
    if (fieldName.endsWith(".text")) {
      return fieldName.substring(0, fieldName.length() - ".text".length());
    }
    return fieldName;
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

  private static boolean wholeValue(@Nonnull LogicalValueType type) {
    return type == LogicalValueType.STRING
        || type == LogicalValueType.RICH_TEXT
        || type == LogicalValueType.URN;
  }
}
