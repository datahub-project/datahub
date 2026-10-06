package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.isQuoted;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.unquote;

import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchValidationConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.utils.ESUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;

/**
 * Full-text query of the Search V3 entity indices: one template over the shared {@code _search}
 * fields (see {@link V3SearchFields}), filled in with the query at request time. It reads which
 * shared fields the searched entities have, never their individual searchable fields, so a new
 * entity or aspect changes the mapping, not this query.
 *
 * <ul>
 *   <li>a simple query string over each shared field's {@code text} subfield, and its {@code
 *       stemmed} subfield at half that weight;
 *   <li>a phrase prefix on each {@code text} subfield, which also matches phrases, replacing the V2
 *       word-gram clauses;
 *   <li>an exact match on the keywords of the shared fields that name the entity, and on the urn,
 *       with a match in the stored casing counting more, as on V2.
 * </ul>
 *
 * <p>Each shared field weighs what {@link V3SearchFields#weight} says, scaled by the same exact,
 * prefix and partial factors as the V2 query; {@code @Searchable} boost scores are not applied.
 * Query configurations and score functions apply as on V2. A field configuration names the shared
 * fields to search, either directly or by a field that feeds one.
 */
@Slf4j
public class V3SearchQueryBuilder extends SearchQueryBuilder {

  // A stemmed match is a looser one, so it counts for less than the same word unstemmed
  private static final float STEMMED_FACTOR = 0.5f;

  private final ExactMatchConfiguration exactMatchConfiguration;
  private final PartialConfiguration partialConfiguration;
  private final SearchValidationConfiguration searchValidationConfiguration;
  private final CustomizedQueryHandler customizedQueryHandler;
  // The shared fields of each searched entity list, derived once from the same annotations that
  // build the mapping
  private final Map<List<String>, Map<String, List<String>>> fullTextFieldsByEntities =
      new ConcurrentHashMap<>();

  public V3SearchQueryBuilder(
      @Nonnull final SearchConfiguration searchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration) {
    super(searchConfiguration, customSearchConfiguration);
    this.exactMatchConfiguration = searchConfiguration.getExactMatch();
    this.partialConfiguration = searchConfiguration.getPartial();
    this.searchValidationConfiguration =
        searchConfiguration.getValidation() != null
            ? searchConfiguration.getValidation()
            : new SearchValidationConfiguration();
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(searchConfiguration.getCustom(), customSearchConfiguration)
            .build();
  }

  @Override
  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    final QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(query).orElse(null);
    if (searchValidationConfiguration.isEnabled()) {
      validateSearchQuery(query);
    }
    final String sanitizedQuery = query.replaceFirst("^:+", "");
    final BoolQueryBuilder finalQuery =
        Optional.ofNullable(customQueryConfig)
            .flatMap(
                cqc ->
                    CustomizedQueryHandler.boolQueryBuilder(
                        opContext.getObjectMapper(), cqc, sanitizedQuery))
            .orElse(QueryBuilders.boolQuery());
    final Set<String> fields = searchedFields(opContext, entitySpecs).keySet();

    if (fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX)) {
      getSimpleQuery(customQueryConfig, fields, sanitizedQuery).ifPresent(finalQuery::should);
      getPrefixAndExactMatchQuery(customQueryConfig, fields, sanitizedQuery)
          .ifPresent(finalQuery::should);
    } else {
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;
      getStructuredQuery(customQueryConfig, fields, withoutQueryPrefix)
          .ifPresent(finalQuery::should);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(customQueryConfig, fields, withoutQueryPrefix)
            .ifPresent(finalQuery::should);
      }
    }

    if (!finalQuery.should().isEmpty()) {
      finalQuery.minimumShouldMatch(1);
    }
    return buildScoreFunctions(opContext, customQueryConfig, entitySpecs, query, finalQuery);
  }

  /**
   * The shared full-text fields a query of these entities reads, after the request's field
   * configuration, each with the root fields that feed it, as the indices they are searched in map
   * them.
   */
  @Nonnull
  public Map<String, List<String>> searchedFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Map<String, List<String>> fields =
        fullTextFieldsByEntities.computeIfAbsent(
            entitySpecs.stream().map(EntitySpec::getName).collect(Collectors.toList()),
            k ->
                V3SearchFields.fullTextFields(
                    V3SearchFields.indexGroupSpecs(opContext.getEntityRegistry(), entitySpecs)));
    final String fieldConfigLabel =
        customizedQueryHandler.resolveFieldConfiguration(
            opContext.getSearchContext().getSearchFlags(),
            CustomConfiguration::getSearchFieldConfigDefault);
    final SearchFields searchFields = getSearchFields(fieldConfigLabel);
    if (searchFields == null) {
      return fields;
    }
    if (!searchFields.isValid()) {
      log.error(
          "Invalid field configuration for label: {}. Replace cannot be used with add/remove.",
          fieldConfigLabel);
      return fields;
    }
    if (!searchFields.getReplace().isEmpty()) {
      final Map<String, List<String>> replaced = named(fields, searchFields.getReplace());
      if (replaced.isEmpty()) {
        log.warn(
            "Field configuration replace resulted in no valid fields for label: {}. "
                + "Using base fields instead.",
            fieldConfigLabel);
        return fields;
      }
      return replaced;
    }
    final Map<String, List<String>> result = new LinkedHashMap<>(fields);
    named(fields, searchFields.getRemove()).keySet().forEach(result::remove);
    // Only a removed shared field can be added back: a field that feeds none is not copied into
    // the shared fields, so no query can reach it
    named(fields, searchFields.getAdd()).forEach(result::put);
    if (result.isEmpty()) {
      // A query over no fields would fall back to the engine's default fields
      log.warn(
          "Field configuration removed every searchable field for label: {}. "
              + "Using base fields instead.",
          fieldConfigLabel);
      return fields;
    }
    return result;
  }

  @Nullable
  private SearchFields getSearchFields(@Nullable final String fieldConfigLabel) {
    final CustomSearchConfiguration customSearchConfiguration =
        customizedQueryHandler.getCustomSearchConfiguration();
    if (fieldConfigLabel == null
        || customSearchConfiguration == null
        || customSearchConfiguration.getFieldConfigurations() == null) {
      return null;
    }
    final FieldConfiguration fieldConfiguration =
        customSearchConfiguration.getFieldConfigurations().get(fieldConfigLabel);
    return fieldConfiguration == null ? null : fieldConfiguration.getSearchFields();
  }

  /**
   * The shared fields that configured names select: a name is a shared field (with or without
   * {@code _search.}) or a field that feeds one, and a subfield or {@code .*} stands for its field,
   * as V2 field configurations write them. The catch-all {@code other} is only selected by its own
   * name: a long-tail field does not stand for every other field queried by default.
   */
  @Nonnull
  private static Map<String, List<String>> named(
      @Nonnull final Map<String, List<String>> fields, @Nonnull final List<String> names) {
    final Set<String> baseNames =
        names.stream().map(V3SearchQueryBuilder::baseName).collect(Collectors.toSet());
    return fields.entrySet().stream()
        .filter(
            field ->
                baseNames.contains(field.getKey())
                    || (!V3SearchFields.OTHER.equals(field.getKey())
                        && field.getValue().stream().anyMatch(baseNames::contains)))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
  }

  @Nonnull
  private static String baseName(@Nonnull final String configuredName) {
    final String prefix = V3SearchFields.path("");
    final String name =
        configuredName.startsWith(prefix)
            ? configuredName.substring(prefix.length())
            : configuredName;
    final int subfield = name.indexOf('.');
    return subfield > 0 ? name.substring(0, subfield) : name;
  }

  private Optional<QueryBuilder> getSimpleQuery(
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull Set<String> fields,
      @Nonnull String sanitizedQuery) {
    final boolean executeSimpleQuery =
        customQueryConfig != null
            ? customQueryConfig.isSimpleQuery()
            : !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    if (!executeSimpleQuery || fields.isEmpty()) {
      return Optional.empty();
    }
    // No analyzer: each subfield applies its own search analyzer
    final SimpleQueryStringBuilder simpleBuilder =
        QueryBuilders.simpleQueryStringQuery(sanitizedQuery).defaultOperator(Operator.AND);
    addAnalyzedFields(fields, partialConfiguration.getFactor(), simpleBuilder::field);
    // Grouped like V2's per-analyzer queries: the search config export reads this shape
    return Optional.of(QueryBuilders.boolQuery().should(simpleBuilder).minimumShouldMatch(1));
  }

  private Optional<QueryBuilder> getPrefixAndExactMatchQuery(
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull Set<String> fields,
      @Nonnull String query) {
    final boolean isPrefixQuery =
        customQueryConfig == null
            ? exactMatchConfiguration.isWithPrefix()
            : customQueryConfig.isPrefixMatchQuery();
    final boolean isExactQuery = customQueryConfig == null || customQueryConfig.isExactMatchQuery();
    final boolean caseSensitivityEnabled =
        exactMatchConfiguration.getCaseSensitivityFactor() > 0.0f;
    final float caseSensitivityFactor =
        caseSensitivityEnabled ? exactMatchConfiguration.getCaseSensitivityFactor() : 1.0f;
    final String unquotedQuery = unquote(query);

    final BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    for (String field : fields) {
      final float weight = V3SearchFields.weight(field);
      if (isPrefixQuery) {
        finalQuery.should(
            QueryBuilders.matchPhrasePrefixQuery(textField(field), query)
                .boost(weight * exactMatchConfiguration.getPrefixFactor() * caseSensitivityFactor));
      }
      if (isExactQuery && V3SearchFields.isIdentity(field)) {
        // As on V2, a match in the stored casing counts on top of the one that ignores case
        if (caseSensitivityEnabled) {
          finalQuery.should(
              QueryBuilders.termQuery(
                      V3SearchFields.path(field) + "." + ESUtils.KEYWORD, unquotedQuery)
                  .boost(weight * exactMatchConfiguration.getExactFactor()));
        }
        // The field itself is normalized, so this term matches ignoring case
        finalQuery.should(
            QueryBuilders.termQuery(V3SearchFields.path(field), unquotedQuery)
                .boost(weight * exactMatchConfiguration.getExactFactor() * caseSensitivityFactor));
      }
    }
    if (isExactQuery) {
      final float urnBoost =
          Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"))
              * exactMatchConfiguration.getExactFactor();
      if (caseSensitivityEnabled) {
        finalQuery.should(
            QueryBuilders.termQuery("urn", unquotedQuery).caseInsensitive(false).boost(urnBoost));
      }
      finalQuery.should(
          QueryBuilders.termQuery("urn", unquotedQuery)
              .caseInsensitive(true)
              .boost(urnBoost * caseSensitivityFactor));
    }
    return finalQuery.should().isEmpty()
        ? Optional.empty()
        : Optional.of(finalQuery.minimumShouldMatch(1));
  }

  private Optional<QueryBuilder> getStructuredQuery(
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull Set<String> fields,
      @Nonnull String sanitizedQuery) {
    if (customQueryConfig != null && !customQueryConfig.isStructuredQuery()) {
      return Optional.empty();
    }
    // Fields a structured query names itself are read as named; these are the default fields
    final QueryStringQueryBuilder queryBuilder =
        QueryBuilders.queryStringQuery(sanitizedQuery).defaultOperator(Operator.AND);
    addAnalyzedFields(fields, 1.0f, queryBuilder::field);
    return Optional.of(queryBuilder);
  }

  /**
   * Adds the analyzed subfields of these shared fields at their weight times {@code factor}: each
   * {@code text} subfield, and its {@code stemmed} one at half that.
   */
  private static void addAnalyzedFields(
      @Nonnull final Set<String> fields,
      final float factor,
      @Nonnull final BiConsumer<String, Float> addField) {
    for (String field : fields) {
      final float boost = V3SearchFields.weight(field) * factor;
      addField.accept(textField(field), boost);
      addField.accept(stemmedField(field), boost * STEMMED_FACTOR);
    }
  }

  @Nonnull
  private static String textField(@Nonnull final String field) {
    return V3SearchFields.path(field) + "." + V3SearchFields.TEXT;
  }

  @Nonnull
  private static String stemmedField(@Nonnull final String field) {
    return V3SearchFields.path(field) + "." + V3SearchFields.STEMMED;
  }
}
