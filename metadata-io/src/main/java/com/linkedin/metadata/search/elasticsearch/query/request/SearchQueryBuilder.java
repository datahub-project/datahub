package com.linkedin.metadata.search.elasticsearch.query.request;

import static com.linkedin.metadata.Constants.SKIP_REFERENCE_ASPECT;
import static com.linkedin.metadata.models.SearchableFieldSpecExtractor.PRIMARY_URN_SEARCH_PROPERTIES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.*;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.CUSTOM_FULL_TEXT_SEARCH_FIELDS;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.isQuoted;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.unquote;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ExactMatchConfiguration;
import com.linkedin.metadata.config.search.PartialConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SearchValidationConfiguration;
import com.linkedin.metadata.config.search.WordGramConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.entity.validation.ValidationException;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchScoreFieldSpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.SearchableRefFieldSpec;
import com.linkedin.metadata.models.annotation.SearchScoreAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableRefAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.IdentityQueryStrategy;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.QueryIntent;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.QueryStrategy;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.QueryUnderstanding;
import com.linkedin.metadata.search.elasticsearch.query.request.understanding.SynonymMapLoader;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.V3IndexKeys;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.lucene.search.FuzzyQuery;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;

/**
 * Builds the keyword search query. V2 entity indices get the V2 query: per-analyzer simple query
 * strings with AND semantics plus exact and prefix matches, summed under a bool query. Search V3
 * keyword reads get the Stage 1 query, a port of DataHub Cloud's Search V2.5 Stage 1: OR retrieval
 * with fuzzy, synonym and wildcard recall under a DisMax root, so the best matching clause sets the
 * score. The Stage 1 methods keep DataHub Cloud's {@code V2_5} names so the code stays easy to
 * merge.
 */
@Slf4j
public class SearchQueryBuilder {
  public static final String STRUCTURED_QUERY_PREFIX = "\\/q ";

  private static final int WILDCARD_MIN_LENGTH = 5;
  private static final float WILDCARD_BOOST_FACTOR = 0.3f;
  private static final float EXACT_PREFIX_DISMAX_TIE_BREAKER = 0.01f;

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  private static final Pattern LETTER_DIGIT_BOUNDARY = Pattern.compile("(?<=[a-zA-Z])(?=\\d)");
  private static final Pattern DIGIT_LETTER_BOUNDARY = Pattern.compile("(?<=\\d)(?=[a-zA-Z])");
  private static final Pattern FUZZY_SPLIT_PATTERN = Pattern.compile("[\\s_\\-]+");

  /** What the search analyzers split terms at: anything but letters and digits. */
  private static final Pattern NON_ALPHANUMERIC_PATTERN = Pattern.compile("[^\\p{L}\\p{N}]+");

  /** A query in quotes, the rule the quoted query configuration of search_config.yaml uses. */
  private static final Pattern FULLY_QUOTED_PATTERN = Pattern.compile("^[\"'].+[\"']$");

  private static final Pattern SQS_OPERATOR_PATTERN = Pattern.compile("[+|\\-~()\\\\*]");
  private static final Pattern WORD_GRAM_SUFFIX_PATTERN = Pattern.compile("\\.wordGrams\\d+$");

  /**
   * Tie-breaker for simple-query inner DisMax across analyzer groups (avoids entity-type bias from
   * summing groups).
   */
  private static final float SIMPLE_QUERY_DISMAX_TIE_BREAKER = 0.01f;

  /**
   * Stage 1: IDF-independent constant score for exact name/title matches. Ensures entities with
   * exact name matches rank first regardless of cross-entity IDF disparities that otherwise bury
   * chart/dashboard exact matches under datasets.
   *
   * <p>The value is deliberately large relative to typical BM25 scores (~5–30) so that an exact
   * name hit always outranks partial matches.
   */
  private static final float EXACT_NAME_CONSTANT_BOOST = 1000.0f;

  /** Fields eligible for the exact-name constant boost (only primary name fields). */
  private static final Set<String> EXACT_NAME_BOOST_FIELDS = Set.of("name", "title");

  /**
   * Stage 1: fields the {@code *word*} contains-wildcard searches. A leading wildcard is costly,
   * and a URN is a different fact from a name.
   */
  private static final Set<String> WILDCARD_CONTAINS_FIELDS = Set.of("name", "title");

  /**
   * Stage 1: BM25-scored boost for FQN matches on qualifiedName. Ensures the target dataset ranks
   * above entities sharing only individual tokens; kept moderate to avoid overwhelming BM25 IDF.
   */
  private static final float FQN_MATCH_BOOST = 50.0f;

  /**
   * Stage 1: Boost multiplier for the all-terms-match bonus clause. For multi-word queries,
   * entities whose name contains ALL query tokens get this multiplier on top of base field boost.
   * Prevents OR dilution where 1-of-N token matches outrank N-of-N matches.
   */
  private static final float ALL_TERMS_MATCH_BOOST_MULTIPLIER = 3.0f;

  /** Minimum number of query tokens required to activate the all-terms bonus. */
  private static final int ALL_TERMS_MIN_TOKENS = 2;

  /** Name/title fields eligible for the all-terms bonus (primary name fields only). */
  private static final Set<String> ALL_TERMS_BONUS_FIELDS = Set.of("name", "title", "id");

  /**
   * Stage 1: Minimum number of query tokens to activate description phrase match. Long queries (4+
   * words) are likely copy-pasted from descriptions or documentation, so an exact phrase match in
   * the description field should rank highly.
   */
  private static final int DESCRIPTION_PHRASE_MIN_TOKENS = 4;

  /**
   * Stage 1: Boost for exact phrase matches on description fields. Deliberately high so that an
   * entity whose description contains the exact query phrase ranks above entities that merely share
   * individual tokens in their name.
   */
  private static final float DESCRIPTION_PHRASE_MATCH_BOOST = 100.0f;

  /**
   * Stage 1: Delimited subfields excluded from prefix matching. These fields contain identifiers
   * (URNs, UUIDs in customProperties) where prefix matching causes short tokens like "cdc" to match
   * UUID hex substrings like "cdc1", flooding results with irrelevant entities.
   */
  private static final Set<String> PREFIX_MATCH_EXCLUDED_FIELDS =
      Set.of("urn.delimited", "customProperties.delimited");

  /**
   * Stage 1: Core identity fields eligible for exact/prefix match clauses. Restricts exact/prefix
   * matching to name/id fields; other fields still get recall via the SQS analyzed match.
   */
  private static final Set<String> EXACT_MATCH_CORE_FIELDS =
      Set.of("name", "title", "_entityName", "qualifiedName", "id", "urn");

  /**
   * Stage 1: Multiplier on exact and prefix match boosts so an exact or prefix hit outscores a
   * fuzzy-only hit under the DisMax root. DataHub Cloud's default.
   */
  private static final float EXACT_MATCH_BOOST_MULTIPLIER = 6.0f;

  /**
   * Stage 1: Multiplier on field boosts in the synonym-priority query so a synonym match outscores
   * a fuzzy match. DataHub Cloud's default.
   */
  private static final float SYNONYM_BOOST_MULTIPLIER = 1.5f;

  /**
   * Stage 1: clauses the per-term queries of one index may add. Lucene counts every term clause,
   * and every term a fuzzy term expands to, against {@code indices.query.bool.max_clause_count}
   * (1024 by default on OpenSearch, which fails the search with too_many_nested_clauses past it).
   * Each term adds a clause per field to the synonym-priority query, the word gram queries and,
   * when fuzzy, the simple query, so a query keeps the words that fit and its fuzzy terms share
   * what is left. The rest of 1024 covers the exact, prefix and wildcard clauses and the request's
   * filters; a query of several terms expands its phrase prefixes less ({@link
   * #MAX_PREFIX_EXPANSIONS}).
   */
  private static final int CLAUSE_BUDGET = 850;

  /** Stage 1: clauses per term outside the per-field queries (all-terms, description, FQN). */
  private static final int PER_TERM_EXTRA_CLAUSES = 6;

  private static final int MAX_FUZZY_EXPANSIONS = 10;

  /**
   * Fields excluded from the light path multi_match. These fields either have very low search
   * relevance (ldap, flowId, jobId, cluster) or are free text whose broad token overlap causes
   * false positives and latency on the focused light path (glossary term definitions). The full
   * query still searches them when the light query finds nothing. Document bodies ({@code text})
   * stay searched, as DataHub Cloud exempts them.
   */
  private static final Set<String> LIGHT_PATH_EXCLUDED_FIELDS =
      Set.of("ldap", "flowId", "jobId", "cluster", "definition");

  /**
   * Matches queries that are truly a single word — no spaces, underscores, hyphens, dots, or
   * slashes. Used to skip wordGram fields for single-word queries since wordGram shingles
   * (2/3/4-grams) require 2+ tokens to match.
   */
  private static final Pattern SINGLE_WORD_PATTERN = Pattern.compile("[^\\s_\\-./:]+");

  private static final Pattern DELIMITER_PATTERN = Pattern.compile("[\\s_\\-./:]+");

  /**
   * Light path: top-level identity fields whose {@code .delimited} analyzer keeps a hyphenated run
   * (e.g. {@code load_job-0001}) as one token. Used to recover exact identifier matches that {@link
   * #escapeSimpleQueryStringOperators} shreds by replacing the hyphen with a space. Exact field
   * names (not shortName) so nested/reference {@code *.delimited} fields cannot sneak in.
   */
  private static final Set<String> DELIMITED_IDENTITY_FIELDS =
      Set.of(
          "name.delimited",
          "title.delimited",
          "urn.delimited",
          "qualifiedName.delimited",
          "id.delimited",
          "displayName.delimited");

  /**
   * Light path: max query length before truncation at a word boundary. Long S3 paths and URLs
   * produce many tokens; truncation preserves recall while reducing evaluation cost.
   */
  private static final int MAX_LIGHT_QUERY_CHARS = 80;

  /**
   * Fields the light path searches for EXACT_NAME and FQN queries: name/title-related subfields
   * only (~10 fields instead of ~35). The full query searches every field when the light query
   * finds nothing.
   */
  private static final Set<String> EXACT_NAME_FOCUSED_FIELDS =
      Set.of("name", "title", "qualifiedName", "id", "displayName", "urn");

  /**
   * Stage 1: terms a phrase prefix expands to per field when the query has several terms, whose
   * per-term clauses take most of {@link #CLAUSE_BUDGET}. The whole query is one token on the
   * {@code .delimited} subfields. A single term keeps the default of 50: for a word too short for
   * fuzziness and the wildcard, the prefix is its only partial match.
   */
  private static final int MAX_PREFIX_EXPANSIONS = 10;

  /** A character the search analyzers keep in a term. */
  private static final Pattern TERM_CHAR_PATTERN = Pattern.compile("[\\p{L}\\p{N}]");

  private static final QueryStrategy IDENTITY_STRATEGY = new IdentityQueryStrategy();

  private final ExactMatchConfiguration exactMatchConfiguration;
  private final PartialConfiguration partialConfiguration;
  private final WordGramConfiguration wordGramConfiguration;
  private final SearchValidationConfiguration searchValidationConfiguration;
  private final Pattern validationRegex;

  private final CustomizedQueryHandler customizedQueryHandler;

  /** Search V3 keyword reads build the Stage 1 query; V2 reads keep the V2 query. */
  private final boolean v3KeywordReadEnabled;

  /**
   * Lazily loaded synonym map for synonym-aware exact matching. Loaded from the synonym file the
   * search analyzers use, so exact match queries on keyword fields (which no analyzer expands) also
   * try the synonyms of the query.
   */
  private volatile Map<String, Set<String>> synonymMap;

  private Map<String, Set<String>> getSynonymMap() {
    if (synonymMap == null) {
      synchronized (this) {
        if (synonymMap == null) {
          synonymMap = SynonymMapLoader.loadDefault();
        }
      }
    }
    return synonymMap;
  }

  /** V2 queries only; production passes the V3 read decision through the other constructor. */
  @VisibleForTesting
  public SearchQueryBuilder(
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this(searchConfiguration, customSearchConfiguration, false);
  }

  public SearchQueryBuilder(
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration,
      final boolean v3KeywordReadEnabled) {
    this.v3KeywordReadEnabled = v3KeywordReadEnabled;
    this.exactMatchConfiguration = searchConfiguration.getExactMatch();
    this.partialConfiguration = searchConfiguration.getPartial();
    this.wordGramConfiguration = searchConfiguration.getWordGram();
    this.searchValidationConfiguration =
        searchConfiguration.getValidation() != null
            ? searchConfiguration.getValidation()
            : new SearchValidationConfiguration();
    this.validationRegex = Pattern.compile(this.searchValidationConfiguration.getRegex());
    this.customizedQueryHandler =
        CustomizedQueryHandler.builder(searchConfiguration.getCustom(), customSearchConfiguration)
            .build();
  }

  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    return buildQuery(opContext, entitySpecs, query, fulltext, false);
  }

  /**
   * Builds the query, optionally the light Stage 1 query.
   *
   * @param skipExpensiveClauses on Search V3, build the light query: one multi_match instead of the
   *     fuzzy simple queries, and no wildcard, synonym-priority or full exact/prefix clauses.
   *     Callers run the full query (false) when the light query matches nothing. V2 queries ignore
   *     it.
   * @return the query, or null for a light query that a custom configuration leaves without any
   *     clause
   */
  @Nullable
  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      boolean skipExpensiveClauses) {
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(query).orElse(null);

    final QueryBuilder queryBuilder;
    if (v3KeywordReadEnabled) {
      // Validate before the identity dispatch below, which skips buildInternalQueryV2_5
      if (searchValidationConfiguration.isEnabled()) {
        validateSearchQuery(query);
      }
      QueryUnderstanding.Result analysis = QueryUnderstanding.analyze(query, getSynonymMap());
      QueryIntent intent = analysis.intent();
      String normalizedQuery = analysis.normalizedQuery();
      if (log.isDebugEnabled()) {
        log.debug(
            "Stage 1 query intent: {}, normalized: \"{}\"{}",
            intent,
            normalizedQuery,
            normalizedQuery.equals(query) ? "" : " (was: \"" + query + "\")");
      }

      // URN and storage-path lookups get a focused query on the identity fields
      QueryBuilder strategyQuery = buildStrategyQuery(intent, normalizedQuery);
      queryBuilder =
          strategyQuery != null
              ? buildIdentityQuery(
                  opContext, customQueryConfig, entitySpecs, normalizedQuery, strategyQuery)
              : buildInternalQueryV2_5(
                  opContext,
                  customQueryConfig,
                  entitySpecs,
                  normalizedQuery,
                  fulltext,
                  skipExpensiveClauses,
                  intent);
    } else {
      queryBuilder = buildInternalQuery(opContext, customQueryConfig, entitySpecs, query, fulltext);
    }
    if (queryBuilder == null) {
      return null;
    }
    return buildScoreFunctions(opContext, customQueryConfig, entitySpecs, query, queryBuilder);
  }

  /**
   * Constructs the search query.
   *
   * @param customQueryConfig custom configuration
   * @param entitySpecs entities being searched
   * @param query search string
   * @param fulltext use fulltext queries
   * @return query builder
   */
  private QueryBuilder buildInternalQuery(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
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

    if (fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX)) {
      getSimpleQuery(opContext, customQueryConfig, entitySpecs, sanitizedQuery)
          .ifPresent(finalQuery::should);
      getPrefixAndExactMatchQuery(
              opContext,
              opContext.getEntityRegistry(),
              customQueryConfig,
              entitySpecs,
              sanitizedQuery,
              opContext.getAspectRetriever())
          .ifPresent(finalQuery::should);
    } else {
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;
      getStructuredQuery(
              opContext.getEntityRegistry(), customQueryConfig, entitySpecs, withoutQueryPrefix)
          .ifPresent(finalQuery::should);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQuery(
                opContext,
                opContext.getEntityRegistry(),
                customQueryConfig,
                entitySpecs,
                withoutQueryPrefix,
                opContext.getAspectRetriever())
            .ifPresent(finalQuery::should);
      }
    }

    if (!finalQuery.should().isEmpty()) {
      finalQuery.minimumShouldMatch(1);
    }

    return finalQuery;
  }

  /**
   * Constructs the Stage 1 query: fuzzy matching with ~1/~2 edit distance, OR operator for lenient
   * matching, and a DisMax root so the best matching clause sets the score instead of every clause
   * adding up.
   *
   * @param customQueryConfig custom configuration
   * @param entitySpecs entities being searched
   * @param query search string, already validated and synonym-normalized
   * @param fulltext use fulltext queries
   * @param skipExpensiveClauses build the light query
   * @param intent the query understanding intent
   * @return query builder
   */
  private QueryBuilder buildInternalQueryV2_5(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      boolean skipExpensiveClauses,
      @Nonnull QueryIntent intent) {
    final boolean simpleSyntax = fulltext && !query.startsWith(STRUCTURED_QUERY_PREFIX);
    final IndexFieldCounts fieldCounts =
        simpleSyntax ? indexFieldCounts(opContext, entitySpecs) : null;
    final String colonStripped =
        simpleSyntax
            ? firstTermsWithinBudget(query.replaceFirst("^:+", ""), fieldCounts)
            : query.replaceFirst("^:+", "");
    final String operatorEscaped = escapeSimpleQueryStringOperators(colonStripped);
    final String sanitizedQuery = splitAlphanumericTokens(operatorEscaped);

    // Light path: truncate long queries (S3 paths, URLs) to limit evaluation cost.
    final String lightSanitizedQuery =
        skipExpensiveClauses
            ? truncateQueryAtWordBoundary(sanitizedQuery, MAX_LIGHT_QUERY_CHARS)
            : sanitizedQuery;

    // Use dis_max instead of bool/should to prevent score accumulation
    // Takes MAX field score + (tie_breaker × sum_of_other_scores)
    // tie_breaker=0.01 so best-matching clause wins with minimal tie-break from other matches.
    final DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
    disMaxQuery.tieBreaker(0.01f);

    // Check if custom query config provides a bool query wrapper (for filters, must clauses).
    // Use colonStripped (not sanitizedQuery) to match V2 behavior for custom config.
    final BoolQueryBuilder customBoolQuery =
        Optional.ofNullable(customQueryConfig)
            .flatMap(
                cqc ->
                    CustomizedQueryHandler.boolQueryBuilder(
                        opContext.getObjectMapper(), cqc, colonStripped))
            .orElse(null);

    if (simpleSyntax) {
      // A custom query configuration can turn every text match off
      final boolean anyTextMatch =
          customQueryConfig == null
              || customQueryConfig.isSimpleQuery()
              || customQueryConfig.isPrefixMatchQuery()
              || customQueryConfig.isExactMatchQuery();
      // A single word split at letter/digit boundaries ("cargo2017") is a light match only where
      // a name holds it whole (the identity re-query below) or holds all of its parts (the
      // all-terms bonus). The multi_match would also take a name holding only "cargo": the
      // .delimited search analyzers split "cargo 2017" inside one token, where an AND operator
      // does not require every part. A word that also holds "_" or "-" ("fleet_v2") still
      // matches a name holding one of those parts, as in DataHub Cloud: the analyzers emit them
      // at one position.
      final boolean splitWord =
          skipExpensiveClauses
              && intent == QueryIntent.EXACT_NAME
              && splitsLetterDigitRun(operatorEscaped);
      if (!splitWord) {
        getSimpleQueryV2_5(
                opContext,
                customQueryConfig,
                entitySpecs,
                lightSanitizedQuery,
                colonStripped,
                skipExpensiveClauses,
                intent,
                fuzzyExpansions(sanitizedQuery, operatorEscaped, fieldCounts))
            .ifPresent(disMaxQuery::add);
      }
      // Synonym recall: the light multi_match reads the mapping's search analyzers, so add
      // queries for the synonyms the analyzers would not expand, such as multi-word ones. A
      // quoted query names a value and does not expand, and only a synonym that is the same name
      // gets the exact-name terms.
      if (skipExpensiveClauses && !FULLY_QUOTED_PATTERN.matcher(colonStripped.trim()).matches()) {
        Set<String> synonyms = getSynonymMap().get(colonStripped.toLowerCase());
        if (synonyms != null) {
          for (String synonym : synonyms) {
            if (!synonym.equalsIgnoreCase(colonStripped)) {
              getSimpleQueryV2_5(
                      opContext,
                      customQueryConfig,
                      entitySpecs,
                      synonym,
                      isSameName(colonStripped, synonym) ? synonym : null,
                      skipExpensiveClauses,
                      intent,
                      0)
                  .ifPresent(disMaxQuery::add);
            }
          }
        }
      }
      if (skipExpensiveClauses) {
        // Light path: two constant_score clauses (name.keyword, title.keyword) so exact name
        // matches still rank first regardless of cross-entity IDF disparities.
        if (customQueryConfig == null || customQueryConfig.isExactMatchQuery()) {
          getLightweightExactMatchBoost(colonStripped, getSynonymMap()).ifPresent(disMaxQuery::add);
        }
      } else {
        // Exact/prefix match with term queries, phrase prefixes, synonyms and word grams.
        getPrefixAndExactMatchQueryV2_5(
                opContext.getEntityRegistry(),
                customQueryConfig,
                entitySpecs,
                colonStripped,
                opContext.getAspectRetriever(),
                termCount(operatorEscaped) > 1
                    ? MAX_PREFIX_EXPANSIONS
                    : FuzzyQuery.defaultMaxExpansions)
            .ifPresent(disMaxQuery::add);
        // Wildcard contains query for substring matching, except for a quoted query, which asks
        // for the words as they are
        if (anyTextMatch && !FULLY_QUOTED_PATTERN.matcher(colonStripped.trim()).matches()) {
          getWildcardContainsQuery(opContext.getEntityRegistry(), entitySpecs, colonStripped)
              .ifPresent(disMaxQuery::add);
        }
        // The light path skips the synonym-priority query: the light multi_match already finds
        // synonym matches through the search-time synonym filters.
        getSynonymPriorityQuery(opContext, customQueryConfig, entitySpecs, sanitizedQuery)
            .ifPresent(disMaxQuery::add);
        // splitAlphanumericTokens turned "orders2017" into "orders 2017", but the analyzers index
        // such a run as one token, so also match the unsplit query, without fuzziness
        if (splitsLetterDigitRun(operatorEscaped)) {
          getSynonymPriorityQuery(opContext, customQueryConfig, entitySpecs, operatorEscaped)
              .ifPresent(disMaxQuery::add);
        }
      }
      // These conditional clauses provide recall and scoring for specific query patterns.
      // They only fire when the query matches their activation criteria.
      if (anyTextMatch) {
        getAllTermsMatchBonus(opContext.getEntityRegistry(), entitySpecs, lightSanitizedQuery)
            .ifPresent(disMaxQuery::add);
        // FQN match on the light path only for 3+ segment FQNs ("db.schema.table"): the common
        // tokens of short FQNs match thousands of loosely related entities, while long FQNs are
        // specific enough and need the boost to outrank containers matching the first segment.
        if (!skipExpensiveClauses || colonStripped.chars().filter(c -> c == '.').count() >= 2) {
          getFqnMatchQuery(colonStripped).ifPresent(disMaxQuery::add);
        }
        getDescriptionPhraseMatchQuery(lightSanitizedQuery).ifPresent(disMaxQuery::add);
      }
      // On the light path the raw-query exact/prefix/wildcard clauses are skipped, and the
      // escaping and letter/digit splitting above shred identifiers such as "load_job-0001" or
      // "orders2017" that the .delimited analyzer indexes as one token. Re-query the .delimited
      // identity fields with the pre-escape string, all terms required. Fires only when escaping
      // or splitting changed the query, so ordinary queries are untouched.
      if (skipExpensiveClauses && anyTextMatch && !colonStripped.equals(sanitizedQuery)) {
        getDelimitedIdentityQuery(opContext, entitySpecs, colonStripped)
            .ifPresent(disMaxQuery::add);
      }
    } else {
      // Structured query path: uses raw query (no splitAlphanumericTokens) because
      // QueryStringQueryBuilder has its own tokenization via the analyzer chain.
      final String withoutQueryPrefix =
          query.startsWith(STRUCTURED_QUERY_PREFIX)
              ? query.substring(STRUCTURED_QUERY_PREFIX.length())
              : query;
      getStructuredQueryV2_5(opContext, customQueryConfig, entitySpecs, withoutQueryPrefix)
          .ifPresent(disMaxQuery::add);
      if (exactMatchConfiguration.isEnableStructured()) {
        getPrefixAndExactMatchQueryV2_5(
                opContext.getEntityRegistry(),
                customQueryConfig,
                entitySpecs,
                withoutQueryPrefix,
                opContext.getAspectRetriever(),
                FuzzyQuery.defaultMaxExpansions)
            .ifPresent(disMaxQuery::add);
      }
    }

    if (skipExpensiveClauses && disMaxQuery.innerQueries().isEmpty()) {
      // A custom configuration left no light clause, so a light query would match every entity:
      // there is none, and the caller runs the full query
      return null;
    }

    // Check if dis_max has any queries (it requires at least one sub-query)
    boolean hasDisMaxQueries = !disMaxQuery.innerQueries().isEmpty();

    QueryBuilder baseQuery;
    // If custom bool query exists (with any clauses), wrap dis_max inside it
    if (customBoolQuery != null
        && (!customBoolQuery.filter().isEmpty()
            || !customBoolQuery.must().isEmpty()
            || !customBoolQuery.mustNot().isEmpty()
            || !customBoolQuery.should().isEmpty())) {
      if (hasDisMaxQueries) {
        customBoolQuery.must(disMaxQuery);
      }
      baseQuery = customBoolQuery;
    } else if (!hasDisMaxQueries) {
      // If dis_max has no queries, return match_all to avoid malformed query error
      baseQuery = QueryBuilders.matchAllQuery();
    } else {
      baseQuery = disMaxQuery;
    }

    return baseQuery;
  }

  /**
   * URN and storage-path lookups: the focused identity query, OR'ed with V2's all-terms simple
   * query, so a URN that differs in case or is cut short, a path stored without its scheme, and
   * entities that reference the URN still match, as on V2. A custom search configuration's bool
   * query wraps it as it wraps the Stage 1 query.
   */
  private QueryBuilder buildIdentityQuery(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      @Nonnull QueryBuilder strategyQuery) {
    final String colonStripped = query.replaceFirst("^:+", "");
    BoolQueryBuilder identityQuery =
        QueryBuilders.boolQuery().should(strategyQuery).minimumShouldMatch(1);
    getSimpleQuery(opContext, customQueryConfig, entitySpecs, colonStripped)
        .ifPresent(identityQuery::should);
    return Optional.ofNullable(customQueryConfig)
        .flatMap(
            cqc ->
                CustomizedQueryHandler.boolQueryBuilder(
                    opContext.getObjectMapper(), cqc, colonStripped))
        .filter(
            bool ->
                !bool.filter().isEmpty()
                    || !bool.must().isEmpty()
                    || !bool.mustNot().isEmpty()
                    || !bool.should().isEmpty())
        .<QueryBuilder>map(bool -> bool.must(identityQuery))
        .orElse(identityQuery);
  }

  /**
   * Stage 1 query understanding: build a focused query for intents that benefit from field
   * targeting. Returns null for every other intent, which uses the standard Stage 1 query.
   */
  @Nullable
  private QueryBuilder buildStrategyQuery(@Nonnull QueryIntent intent, @Nonnull String query) {
    QueryStrategy strategy;
    switch (intent) {
      case IDENTITY:
        strategy = IDENTITY_STRATEGY;
        break;
      // FQN queries (e.g., "analytics_db.dim_user") need broad field coverage because the user
      // may be searching for a schema container, not just the exact entity. The standard query
      // handles FQN tokenization via .delimited subfields. Short EXACT_NAME queries need broad
      // field coverage (description, tags, etc.) to find all relevant results.
      default:
        return null;
    }
    return strategy.buildQuery(query, getSynonymMap());
  }

  /**
   * Validates search query input to block malicious payloads. Blocks Java deserialization attacks,
   * JNDI injection, and other exploits.
   *
   * @param input The raw search query string
   * @throws ValidationException if the query contains dangerous patterns
   */
  @Nonnull
  public void validateSearchQuery(@Nonnull String input) throws ValidationException {
    // Limit query length
    if (searchValidationConfiguration.isMaxLengthEnabled()
        && input.length() > searchValidationConfiguration.getMaxQueryLength()) {
      log.warn("Blocked excessively long search query: {} characters", input.length());
      throw new ValidationException(
          "Search query exceeds maximum length of "
              + searchValidationConfiguration.getMaxQueryLength()
              + " characters");
    }
    if (validationRegex.matcher(input).matches()) {
      log.warn("Blocked potentially malicious search query.");
      throw new ValidationException("Query rejected due to potentially malicious structure.");
    }
  }

  /**
   * Adds fuzzy matching operators to query terms for typo tolerance.
   *
   * <p>Appends ~N to each term to enable fuzzy matching with up to N character edits. This handles
   * common typos like "notiphication" → "notification" without requiring exact matches.
   *
   * <p>Edit distance scaling (conservative to avoid false positives on short acronyms):
   *
   * <ul>
   *   <li>Terms 1-4 chars: No fuzzy (exact match only). Short acronyms like "nps", "etl", "gmv" are
   *       too easily corrupted by even 1 edit (nps→fps, gmv→gov, etl→etc).
   *   <li>Terms 5-6 chars: ~1 (1 edit). Catches common typos while keeping precision.
   *   <li>Terms 7+ chars: ~2 (2 edits). Longer terms tolerate more edits safely.
   * </ul>
   *
   * @param query The sanitized search query
   * @return Query with fuzzy operators added to terms
   */
  @Nonnull
  public static String makeFuzzyQuery(@Nonnull final String query) {
    // Split on whitespace, underscores, and hyphens to add ~N fuzzy operator to each sub-term,
    // otherwise terms like "page_view" get ~2 on the whole string which performs poorly.
    String[] terms = FUZZY_SPLIT_PATTERN.split(query);
    StringBuilder fuzzyQuery = new StringBuilder();

    for (int i = 0; i < terms.length; i++) {
      String term = terms[i].trim();
      if (term.isEmpty()) {
        continue;
      }

      fuzzyQuery.append(term);

      int termLength = term.length();
      if (termLength >= 7) {
        fuzzyQuery.append("~2"); // Allow 2 edits for longer terms
      } else if (termLength >= 5) {
        fuzzyQuery.append("~1"); // Allow 1 edit for medium terms
      }
      // Terms < 5 chars: no fuzzy — short acronyms (nps, etl, gmv) are too easily corrupted

      if (i < terms.length - 1) {
        fuzzyQuery.append(" ");
      }
    }

    return fuzzyQuery.toString();
  }

  /** The fields one index queries by default, outside and inside the word gram analyzers. */
  private record IndexFieldCounts(int fields, int wordGramFields) {}

  /**
   * The most fields one index queries by default. Lucene counts clauses per index, and a field an
   * index does not map adds none.
   */
  private IndexFieldCounts indexFieldCounts(
      @Nonnull OperationContext opContext, @Nonnull List<EntitySpec> entitySpecs) {
    int fields = 0;
    int wordGramFields = 0;
    for (List<EntitySpec> indexSpecs :
        entitySpecs.stream().collect(Collectors.groupingBy(V3IndexKeys::resolve)).values()) {
      Map<Boolean, Set<List<String>>> analyzedFields =
          applySearchFieldConfiguration(
                  opContext,
                  indexSpecs,
                  getStandardFields(opContext.getEntityRegistry(), indexSpecs),
                  customizedQueryHandler.resolveFieldConfiguration(
                      opContext.getSearchContext().getSearchFlags(),
                      CustomConfiguration::getSearchFieldConfigDefault))
              .stream()
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(
                  Collectors.partitioningBy(
                      cfg -> cfg.analyzer().contains("word_gram"),
                      Collectors.mapping(
                          cfg -> List.of(cfg.analyzer(), cfg.fieldName()), Collectors.toSet())));
      fields = Math.max(fields, analyzedFields.get(false).size());
      wordGramFields = Math.max(wordGramFields, analyzedFields.get(true).size());
    }
    return new IndexFieldCounts(fields, wordGramFields);
  }

  /** The letter and digit runs of {@code text}, the terms the search analyzers produce. */
  private static int runCount(@Nonnull String text) {
    return (int)
        Arrays.stream(NON_ALPHANUMERIC_PATTERN.split(text)).filter(term -> !term.isEmpty()).count();
  }

  /**
   * The terms the per-term queries match in {@code text}, also split at letter/digit boundaries.
   */
  private static int termCount(@Nonnull String text) {
    return runCount(splitAlphanumericTokens(text));
  }

  /**
   * Clauses the per-term queries add without fuzziness: the synonym-priority query, the word gram
   * queries and the bonus clauses per term, plus the unsplit copy of every word when any word holds
   * a letter/digit run.
   */
  private static int termClauses(
      int terms, int unsplitWords, boolean runs, @Nonnull IndexFieldCounts counts) {
    return terms * (counts.fields() + counts.wordGramFields() + PER_TERM_EXTRA_CLAUSES)
        + (runs ? unsplitWords * counts.fields() : 0);
  }

  private static int termClauses(
      @Nonnull String operatorEscaped, @Nonnull IndexFieldCounts counts) {
    return termClauses(
        termCount(operatorEscaped),
        runCount(operatorEscaped),
        splitsLetterDigitRun(operatorEscaped),
        counts);
  }

  /**
   * The leading words of {@code query} whose per-term clauses fit {@link #CLAUSE_BUDGET}, so a
   * pasted paragraph stays under the clause limit. A first word that does not fit alone keeps its
   * leading terms. A fully quoted query keeps its quotes.
   */
  private static String firstTermsWithinBudget(
      @Nonnull String query, @Nonnull IndexFieldCounts counts) {
    String trimmed = query.trim();
    boolean quoted = FULLY_QUOTED_PATTERN.matcher(trimmed).matches();
    String[] words =
        WHITESPACE_PATTERN.split(quoted ? trimmed.substring(1, trimmed.length() - 1) : trimmed);
    int terms = 0;
    int unsplitWords = 0;
    boolean runs = false;
    int kept = 0;
    for (String word : words) {
      String escaped = escapeSimpleQueryStringOperators(word);
      int wordTerms = termCount(escaped);
      int wordUnsplit = runCount(escaped);
      boolean withRuns = runs || splitsLetterDigitRun(escaped);
      if (termClauses(terms + wordTerms, unsplitWords + wordUnsplit, withRuns, counts)
          > CLAUSE_BUDGET) {
        break;
      }
      terms += wordTerms;
      unsplitWords += wordUnsplit;
      runs = withRuns;
      kept++;
    }
    if (kept == words.length) {
      return query;
    }
    log.debug("Kept {} of {} words of the query within the clause budget", kept, words.length);
    String keptWords =
        kept == 0
            ? leadingTermsWithinBudget(words[0], counts)
            : String.join(" ", Arrays.copyOf(words, kept));
    return quoted
        ? trimmed.charAt(0) + keptWords + trimmed.charAt(trimmed.length() - 1)
        : keptWords;
  }

  /**
   * The leading terms of {@code word} whose clauses fit {@link #CLAUSE_BUDGET}, cut where a term
   * ends, at a separator or a letter/digit boundary: a deep dotted path or a long letter/digit run
   * is one word with more terms than a query can match. Keeps at least the first term.
   */
  private static String leadingTermsWithinBudget(
      @Nonnull String word, @Nonnull IndexFieldCounts counts) {
    String kept = null;
    for (int i = word.offsetByCodePoints(0, 1);
        i < word.length();
        i = word.offsetByCodePoints(i, 1)) {
      String previous = Character.toString(word.codePointBefore(i));
      String next = Character.toString(word.codePointAt(i));
      boolean termEnds =
          TERM_CHAR_PATTERN.matcher(previous).matches()
              && (!TERM_CHAR_PATTERN.matcher(next).matches()
                  || splitsLetterDigitRun(previous + next));
      if (termEnds) {
        String prefix = word.substring(0, i);
        if (kept != null
            && termClauses(escapeSimpleQueryStringOperators(prefix), counts) > CLAUSE_BUDGET) {
          break;
        }
        kept = prefix;
      }
    }
    return kept == null ? word : kept;
  }

  /**
   * Expansions per fuzzy term that keep the per-term queries within {@link #CLAUSE_BUDGET}, or 0
   * for no fuzziness: a quoted query, no term long enough to be fuzzy, or no room for one expansion
   * each. A short query keeps every expansion; a long one gets fewer, then none.
   */
  private static int fuzzyExpansions(
      @Nonnull String sanitizedQuery,
      @Nonnull String operatorEscaped,
      @Nonnull IndexFieldCounts counts) {
    // Each ~ operator is a term the fuzzy simple query sends as fuzzy
    long fuzzyTerms = makeFuzzyQuery(sanitizedQuery).chars().filter(c -> c == '~').count();
    if (isQuoted(sanitizedQuery) || fuzzyTerms == 0 || counts.fields() == 0) {
      return 0;
    }
    // The fuzzy simple query adds a clause per term and field, and every expansion past the first
    // one more per fuzzy term and field
    long room =
        CLAUSE_BUDGET
            - termClauses(operatorEscaped, counts)
            - (long) termCount(operatorEscaped) * counts.fields();
    if (room < 0) {
      return 0;
    }
    return (int) Math.min(MAX_FUZZY_EXPANSIONS, 1 + room / (fuzzyTerms * counts.fields()));
  }

  /** Whether {@link #splitAlphanumericTokens} splits a letter/digit run of {@code query}. */
  private static boolean splitsLetterDigitRun(@Nonnull String query) {
    return LETTER_DIGIT_BOUNDARY.matcher(query).find()
        || DIGIT_LETTER_BOUNDARY.matcher(query).find();
  }

  /**
   * Splits mixed alphanumeric tokens into separate words at letter/digit boundaries.
   *
   * <p>Handles queries like "orders2017" → "orders 2017" so each part can be matched independently.
   * Only splits tokens that contain both letters and digits (e.g., won't affect "hello" or "2017"
   * alone). Preserves tokens that are already separated by whitespace.
   *
   * @param query The search query
   * @return Query with alphanumeric tokens split into separate words
   */
  @VisibleForTesting
  public static String splitAlphanumericTokens(@Nonnull String query) {
    String[] tokens = WHITESPACE_PATTERN.split(query);
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < tokens.length; i++) {
      String token = tokens[i];
      boolean hasLetters = false;
      boolean hasDigits = false;
      for (char c : token.toCharArray()) {
        if (Character.isLetter(c)) hasLetters = true;
        if (Character.isDigit(c)) hasDigits = true;
      }
      if (hasLetters && hasDigits) {
        // Insert space at letter↔digit boundaries
        String split =
            DIGIT_LETTER_BOUNDARY
                .matcher(LETTER_DIGIT_BOUNDARY.matcher(token).replaceAll(" "))
                .replaceAll(" ");
        result.append(split);
      } else {
        result.append(token);
      }
      if (i < tokens.length - 1) {
        result.append(" ");
      }
    }
    return result.toString();
  }

  /**
   * Truncates a query to approximately maxChars characters, cutting at the last word boundary
   * (whitespace or common delimiter) before the limit. Used by the light path to limit the cost of
   * long queries (S3 paths, URLs). Returns the original query unchanged if it is already shorter
   * than maxChars.
   */
  @VisibleForTesting
  static String truncateQueryAtWordBoundary(@Nonnull final String query, int maxChars) {
    if (query.length() <= maxChars) {
      return query;
    }
    // Find last word boundary (whitespace or common delimiters) at or before maxChars
    int cutPoint = maxChars;
    while (cutPoint > 0 && !isQueryDelimiter(query.charAt(cutPoint))) {
      cutPoint--;
    }
    // If no delimiter found in first maxChars chars, cut at the limit
    if (cutPoint == 0) {
      cutPoint = maxChars;
    }
    return query.substring(0, cutPoint).trim();
  }

  private static boolean isQueryDelimiter(char c) {
    return Character.isWhitespace(c) || c == '/' || c == '.' || c == ':' || c == '_';
  }

  /**
   * Strips matching surrounding quotes (single or double, bare or escaped) from a query string.
   * Handles both escaped quotes (e.g., {@code \"term\"}) and bare quotes (e.g., {@code "term"}).
   * Returns the original string unchanged if it is not quoted.
   */
  @VisibleForTesting
  static String stripSurroundingQuotes(@Nonnull final String query) {
    if (query.length() > 4
        && ((query.startsWith("\\\"") && query.endsWith("\\\""))
            || (query.startsWith("\\'") && query.endsWith("\\'")))) {
      return query.substring(2, query.length() - 2);
    } else if (query.length() > 2
        && ((query.startsWith("\"") && query.endsWith("\""))
            || (query.startsWith("'") && query.endsWith("'")))) {
      return query.substring(1, query.length() - 1);
    }
    return query;
  }

  /**
   * Replaces SimpleQueryString operator characters with spaces so user input is treated as literal
   * search terms, not query syntax. Without this, hyphens become NOT operators (e.g.,
   * "user-interaction" excludes "interaction"), tildes override fuzzy edit distance, and other
   * characters change query semantics unexpectedly.
   *
   * <p>Preserved: double quotes (phrase matching handled by isQuoted()), bare "*" (used for
   * browse-all). Replaced with space: + | - ~ ( ) \ *
   */
  @VisibleForTesting
  public static String escapeSimpleQueryStringOperators(@Nonnull final String query) {
    // Bare "*" is the browse-all query — don't escape it
    if ("*".equals(query.trim())) {
      return query;
    }
    return SQS_OPERATOR_PATTERN.matcher(query).replaceAll(" ");
  }

  /**
   * Gets searchable fields from all entities in the input collection. De-duplicates fields across
   * entities.
   *
   * @param entitySpecs: Entity specs to extract searchable fields from
   * @return A set of SearchFieldConfigs containing the searchable fields from the input entities.
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull Collection<EntitySpec> entitySpecs) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    // Always present
    final float urnBoost =
        Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));

    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn", urnBoost, SearchableAnnotation.FieldType.URN, true));
    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn.delimited",
            urnBoost * partialConfiguration.getUrnFactor(),
            SearchableAnnotation.FieldType.URN,
            true));

    entitySpecs.stream()
        .map(spec -> getFieldsFromEntitySpec(entityRegistry, spec))
        .flatMap(Set::stream)
        .collect(Collectors.groupingBy(SearchFieldConfig::fieldName))
        .forEach(
            (key, value) ->
                fields.add(
                    new SearchFieldConfig(
                        key,
                        value.get(0).shortName(),
                        (float)
                            value.stream()
                                .mapToDouble(SearchFieldConfig::boost)
                                .average()
                                .getAsDouble(),
                        value.get(0).analyzer(),
                        value.stream().anyMatch(SearchFieldConfig::hasKeywordSubfield),
                        value.stream().anyMatch(SearchFieldConfig::hasDelimitedSubfield),
                        value.stream().anyMatch(SearchFieldConfig::hasWordGramSubfields),
                        true,
                        value.stream().anyMatch(SearchFieldConfig::isDelimitedSubfield),
                        value.stream().anyMatch(SearchFieldConfig::isKeywordSubfield),
                        value.stream().anyMatch(SearchFieldConfig::isWordGramSubfield))));

    return fields;
  }

  /**
   * Return query by default fields
   *
   * @param entityRegistry entity registry with search annotations
   * @param entitySpec the entity spect
   * @return set of queryByDefault field configurations
   */
  @VisibleForTesting
  public Set<SearchFieldConfig> getFieldsFromEntitySpec(
      @Nonnull EntityRegistry entityRegistry, EntitySpec entitySpec) {
    Set<SearchFieldConfig> fields = new HashSet<>();
    List<SearchableFieldSpec> searchableFieldSpecs = entitySpec.getSearchableFieldSpecs();
    for (SearchableFieldSpec fieldSpec : searchableFieldSpecs) {
      if (!fieldSpec.getSearchableAnnotation().isQueryByDefault()) {
        continue;
      }

      SearchFieldConfig searchFieldConfig = SearchFieldConfig.detectSubFieldType(fieldSpec);
      fields.add(searchFieldConfig);

      if (SearchFieldConfig.detectSubFieldType(fieldSpec).hasDelimitedSubfield()) {
        final SearchableAnnotation searchableAnnotation = fieldSpec.getSearchableAnnotation();

        fields.add(
            SearchFieldConfig.detectSubFieldType(
                searchFieldConfig.fieldName() + ".delimited",
                searchFieldConfig.boost() * partialConfiguration.getFactor(),
                searchableAnnotation.getFieldType(),
                searchableAnnotation.isQueryByDefault()));

        if (SearchFieldConfig.detectSubFieldType(fieldSpec).hasWordGramSubfields()) {
          addWordGramSearchConfig(fields, searchFieldConfig);
        }
      }
    }

    List<SearchableRefFieldSpec> searchableRefFieldSpecs = entitySpec.getSearchableRefFieldSpecs();
    for (SearchableRefFieldSpec refFieldSpec : searchableRefFieldSpecs) {
      if (!refFieldSpec.getSearchableRefAnnotation().isQueryByDefault()) {
        continue;
      }

      int depth = refFieldSpec.getSearchableRefAnnotation().getDepth();
      Set<SearchFieldConfig> searchFieldConfigs =
          SearchFieldConfig.detectSubFieldType(refFieldSpec, depth, entityRegistry).stream()
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(Collectors.toSet());
      fields.addAll(searchFieldConfigs);

      Map<String, SearchableAnnotation.FieldType> fieldTypeMap =
          getAllFieldTypeFromSearchableRef(refFieldSpec, depth, entityRegistry, "");
      for (SearchFieldConfig fieldConfig : searchFieldConfigs) {
        if (fieldConfig.hasDelimitedSubfield()) {
          fields.add(
              SearchFieldConfig.detectSubFieldType(
                  fieldConfig.fieldName() + ".delimited",
                  fieldConfig.boost() * partialConfiguration.getFactor(),
                  fieldTypeMap.get(fieldConfig.fieldName()),
                  fieldConfig.isQueryByDefault()));
        }

        if (fieldConfig.hasWordGramSubfields()) {
          addWordGramSearchConfig(fields, fieldConfig);
        }
      }
    }
    return fields;
  }

  private void addWordGramSearchConfig(
      Set<SearchFieldConfig> fields, SearchFieldConfig searchFieldConfig) {
    fields.add(
        SearchFieldConfig.builder()
            .fieldName(searchFieldConfig.fieldName() + ".wordGrams2")
            .boost(searchFieldConfig.boost() * wordGramConfiguration.getTwoGramFactor())
            .analyzer(WORD_GRAM_2_ANALYZER)
            .hasKeywordSubfield(true)
            .hasDelimitedSubfield(true)
            .hasWordGramSubfields(true)
            .isQueryByDefault(true)
            .build());
    fields.add(
        SearchFieldConfig.builder()
            .fieldName(searchFieldConfig.fieldName() + ".wordGrams3")
            .boost(searchFieldConfig.boost() * wordGramConfiguration.getThreeGramFactor())
            .analyzer(WORD_GRAM_3_ANALYZER)
            .hasKeywordSubfield(true)
            .hasDelimitedSubfield(true)
            .hasWordGramSubfields(true)
            .isQueryByDefault(true)
            .build());
    fields.add(
        SearchFieldConfig.builder()
            .fieldName(searchFieldConfig.fieldName() + ".wordGrams4")
            .boost(searchFieldConfig.boost() * wordGramConfiguration.getFourGramFactor())
            .analyzer(WORD_GRAM_4_ANALYZER)
            .hasKeywordSubfield(true)
            .hasDelimitedSubfield(true)
            .hasWordGramSubfields(true)
            .isQueryByDefault(true)
            .build());
  }

  /** The fields one entity searches, with the urn. */
  protected Set<SearchFieldConfig> getStandardFields(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    Set<SearchFieldConfig> fields = new HashSet<>();

    // Always present
    final float urnBoost =
        Float.parseFloat((String) PRIMARY_URN_SEARCH_PROPERTIES.get("boostScore"));

    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn", urnBoost, SearchableAnnotation.FieldType.URN, true));
    fields.add(
        SearchFieldConfig.detectSubFieldType(
            "urn.delimited",
            urnBoost * partialConfiguration.getUrnFactor(),
            SearchableAnnotation.FieldType.URN,
            true));

    fields.addAll(getFieldsFromEntitySpec(entityRegistry, entitySpec));

    return fields;
  }

  /**
   * The fields of these entities that the field configuration {@code label} keeps, or {@code
   * fields} when it names none.
   */
  protected Set<SearchFieldConfig> applySearchFieldConfiguration(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchFieldConfig> fields,
      @Nullable String label) {
    return customizedQueryHandler.applySearchFieldConfiguration(fields, label);
  }

  /** Stage 1: the fields the FQN match reads. */
  protected List<String> fqnMatchFields() {
    // Many entities (e.g., dbt datasets) store the FQN in the id field rather than qualifiedName
    return List.of("qualifiedName.delimited", "id.delimited");
  }

  /** Stage 1: the field the description phrase match reads. */
  protected String descriptionPhraseField() {
    return "description.delimited";
  }

  /** Light path: whether the identity re-query reads this field. */
  protected boolean isDelimitedIdentityField(@Nonnull SearchFieldConfig cfg) {
    return DELIMITED_IDENTITY_FIELDS.contains(cfg.fieldName());
  }

  private Optional<QueryBuilder> getSimpleQuery(
      @Nonnull OperationContext operationContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();
    EntityRegistry entityRegistry = operationContext.getEntityRegistry();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (executeSimpleQuery) {
      BoolQueryBuilder simplePerField = QueryBuilders.boolQuery();

      // Get base fields
      Set<SearchFieldConfig> baseFields =
          entitySpecs.stream()
              .map(spec -> getStandardFields(entityRegistry, spec))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      Set<SearchFieldConfig> configuredFields =
          applySearchFieldConfiguration(
              operationContext,
              entitySpecs,
              baseFields,
              customizedQueryHandler.resolveFieldConfiguration(
                  operationContext.getSearchContext().getSearchFlags(),
                  CustomConfiguration::getSearchFieldConfigDefault));

      /*
       * NOTE: This logic applies the queryByDefault annotations for each entity to ALL entities
       * If we ever have fields that are queryByDefault on some entities and not others, this section will need to be refactored
       * to apply an index filter AND the analyzers added here.
       */
      // Simple query string does not use per field analyzers
      // Group the fields by analyzer
      Map<String, List<SearchFieldConfig>> analyzerGroup =
          configuredFields.stream()
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(Collectors.groupingBy(SearchFieldConfig::analyzer));

      analyzerGroup.keySet().stream()
          .sorted()
          .filter(str -> !str.contains("word_gram"))
          .forEach(
              analyzer -> {
                List<SearchFieldConfig> fieldConfigs = analyzerGroup.get(analyzer);
                SimpleQueryStringBuilder simpleBuilder =
                    QueryBuilders.simpleQueryStringQuery(sanitizedQuery);
                simpleBuilder.analyzer(analyzer);
                simpleBuilder.defaultOperator(Operator.AND);
                Map<String, List<SearchFieldConfig>> fieldAnalyzers =
                    fieldConfigs.stream()
                        .collect(Collectors.groupingBy(SearchFieldConfig::fieldName));
                // De-duplicate fields across different indices
                for (Map.Entry<String, List<SearchFieldConfig>> fieldAnalyzer :
                    fieldAnalyzers.entrySet()) {
                  SearchFieldConfig cfg = fieldAnalyzer.getValue().get(0);
                  simpleBuilder.field(cfg.fieldName(), cfg.boost());
                }
                simplePerField.should(simpleBuilder);
              });

      if (!simplePerField.should().isEmpty()) {
        simplePerField.minimumShouldMatch(1);
      }

      result = Optional.of(simplePerField);
    }

    return result;
  }

  /**
   * Stage 1: Simple query with OR operator and lenient behavior. The full query runs one simple
   * query string per analyzer group under a DisMax, with explicit fuzzy operators (~N): multi_match
   * fuzziness applies AFTER analysis (stemming), so "notiphication" stems to "notiph" which can't
   * fuzzy-match "notif" (stemmed "notification"). SQS applies ~N to raw tokens before analysis,
   * preserving fuzzy recall. The light query runs a single multi_match without fuzziness instead.
   */
  private Optional<QueryBuilder> getSimpleQueryV2_5(
      @Nonnull OperationContext operationContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery,
      @Nullable String typedQuery,
      boolean skipExpensiveClauses,
      @Nonnull QueryIntent intent,
      int maxExpansions) {
    Optional<QueryBuilder> result = Optional.empty();
    EntityRegistry entityRegistry = operationContext.getEntityRegistry();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (executeSimpleQuery) {
      Set<SearchFieldConfig> baseFields =
          entitySpecs.stream()
              .map(spec -> getStandardFields(entityRegistry, spec))
              .flatMap(Set::stream)
              .collect(Collectors.toSet());

      Set<SearchFieldConfig> configuredFields =
          applySearchFieldConfiguration(
              operationContext,
              entitySpecs,
              baseFields,
              customizedQueryHandler.resolveFieldConfiguration(
                  operationContext.getSearchContext().getSearchFlags(),
                  CustomConfiguration::getSearchFieldConfigDefault));

      if (skipExpensiveClauses) {
        return getLightSimpleQuery(
            configuredFields,
            sanitizedQuery,
            typedQuery,
            intent,
            customQueryConfig == null || customQueryConfig.isExactMatchQuery());
      }

      DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
      disMaxQuery.tieBreaker(SIMPLE_QUERY_DISMAX_TIE_BREAKER);

      Map<String, List<SearchFieldConfig>> analyzerGroup =
          configuredFields.stream()
              .filter(SearchFieldConfig::isQueryByDefault)
              .collect(Collectors.groupingBy(SearchFieldConfig::analyzer));

      final String fuzzyQuery = makeFuzzyQuery(sanitizedQuery);
      final boolean fuzzy = maxExpansions > 0;

      analyzerGroup.keySet().stream()
          .sorted()
          .forEach(
              analyzer -> {
                List<SearchFieldConfig> fieldConfigs = analyzerGroup.get(analyzer);
                boolean isWordGram = analyzer.contains("word_gram");
                if (!isWordGram && !fuzzy) {
                  // Without fuzziness this group would repeat the synonym-priority query, which
                  // matches the same words on the same fields at a higher boost, and only add
                  // clauses
                  return;
                }

                String queryToUse = isWordGram ? sanitizedQuery : fuzzyQuery;

                Map<String, SearchFieldConfig> uniqueFields = new LinkedHashMap<>();
                for (SearchFieldConfig cfg : fieldConfigs) {
                  uniqueFields.merge(
                      cfg.fieldName(),
                      cfg,
                      (existing, incoming) ->
                          incoming.boost() > existing.boost() ? incoming : existing);
                }

                SimpleQueryStringBuilder simpleBuilder =
                    QueryBuilders.simpleQueryStringQuery(queryToUse);
                simpleBuilder.analyzer(analyzer);
                simpleBuilder.defaultOperator(Operator.OR);
                if (!isWordGram) {
                  simpleBuilder.fuzzyPrefixLength(0);
                  simpleBuilder.fuzzyMaxExpansions(maxExpansions);
                  simpleBuilder.fuzzyTranspositions(true);
                }
                for (SearchFieldConfig cfg : uniqueFields.values()) {
                  simpleBuilder.field(cfg.fieldName(), cfg.boost());
                }

                disMaxQuery.add(simpleBuilder);
              });

      if (!disMaxQuery.innerQueries().isEmpty()) {
        result = Optional.of(disMaxQuery);
      }
    }

    return result;
  }

  /**
   * Light path: a single multi_match over the {@code .delimited} and {@code .wordGrams} subfields,
   * which apply their mapping's search analyzers, instead of the per-analyzer simple queries. Exact
   * matching is handled separately by {@link #getLightweightExactMatchBoost}.
   */
  private Optional<QueryBuilder> getLightSimpleQuery(
      @Nonnull Set<SearchFieldConfig> configuredFields,
      @Nonnull String sanitizedQuery,
      @Nullable String typedQuery,
      @Nonnull QueryIntent intent,
      boolean exactMatch) {
    // Quoted queries are not searching for the quote characters
    String matchQuery =
        intent == QueryIntent.EXACT_NAME ? stripSurroundingQuotes(sanitizedQuery) : sanitizedQuery;

    MultiMatchQueryBuilder multiMatch =
        QueryBuilders.multiMatchQuery(matchQuery)
            .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .tieBreaker(SIMPLE_QUERY_DISMAX_TIE_BREAKER);

    // AND operator for navigational queries: deep FQNs (2+ dots) and long exact names (5+
    // delimiter tokens). Narrows the candidate set; the full OR query runs when this finds nothing.
    boolean isDeepFqn =
        intent == QueryIntent.FQN && matchQuery.chars().filter(c -> c == '.').count() >= 2;
    boolean isLongExactName =
        intent == QueryIntent.EXACT_NAME
            && Arrays.stream(DELIMITER_PATTERN.split(matchQuery))
                    .filter(part -> !part.isEmpty())
                    .count()
                >= 5;
    if (isDeepFqn || isLongExactName) {
      multiMatch.operator(Operator.AND);
    }

    // EXACT_NAME and FQN queries search the name-related subfields and, as in DataHub Cloud, the
    // structured property values; KEYWORD queries search every field for broader recall. Word
    // grams need 2+ tokens, so single words skip them.
    boolean useNameFocusedFields = intent == QueryIntent.EXACT_NAME || intent == QueryIntent.FQN;
    boolean isTrueSingleWord = SINGLE_WORD_PATTERN.matcher(matchQuery.trim()).matches();
    Map<String, Float> fieldBoosts = new LinkedHashMap<>();
    configuredFields.stream()
        .filter(SearchFieldConfig::isQueryByDefault)
        .filter(cfg -> cfg.isDelimitedSubfield() || (cfg.isWordGramSubfield() && !isTrueSingleWord))
        .filter(cfg -> !LIGHT_PATH_EXCLUDED_FIELDS.contains(cfg.shortName()))
        .filter(
            cfg ->
                !useNameFocusedFields
                    || EXACT_NAME_FOCUSED_FIELDS.contains(cfg.shortName())
                    || CUSTOM_FULL_TEXT_SEARCH_FIELDS.equals(cfg.shortName()))
        .forEach(cfg -> fieldBoosts.merge(cfg.fieldName(), cfg.boost(), Math::max));
    if (fieldBoosts.isEmpty()) {
      return Optional.empty();
    }
    fieldBoosts.forEach(multiMatch::field);

    // No typed name: a synonym that is a different name gets no exact-name terms
    if (!useNameFocusedFields || !exactMatch || typedQuery == null) {
      return Optional.of(multiMatch);
    }
    // Exact name hits score above multi_match results. The name is the query as typed: escaping
    // and splitting would make "load-job" an exact match for a name "Load Job"
    String exactQuery = stripSurroundingQuotes(typedQuery);
    DisMaxQueryBuilder nameBoost = QueryBuilders.disMaxQuery().tieBreaker(0.0f);
    nameBoost.add(
        QueryBuilders.termQuery("name.keyword", exactQuery)
            .caseInsensitive(true)
            .boost(EXACT_NAME_CONSTANT_BOOST));
    nameBoost.add(
        QueryBuilders.termQuery("title.keyword", exactQuery)
            .caseInsensitive(true)
            .boost(EXACT_NAME_CONSTANT_BOOST));
    nameBoost.add(multiMatch);
    return Optional.of(nameBoost);
  }

  /**
   * Light path: minimal exact-match boost using only constant_score clauses on name.keyword and
   * title.keyword. Keeps exact name matches above partial matches without the many clauses of the
   * full exact/prefix query (term queries, phrase prefixes, synonyms and word grams on every core
   * field).
   */
  @VisibleForTesting
  static Optional<QueryBuilder> getLightweightExactMatchBoost(
      @Nonnull final String query, @Nullable Map<String, Set<String>> synonymMap) {
    if (query.trim().isEmpty()) {
      return Optional.empty();
    }

    String unquotedQuery = stripSurroundingQuotes(query);
    // A quoted query names a value, so it does not expand to its synonyms
    boolean quoted = FULLY_QUOTED_PATTERN.matcher(query.trim()).matches();
    DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
    disMaxQuery.tieBreaker(EXACT_PREFIX_DISMAX_TIE_BREAKER);

    for (String field : List.of("name.keyword", "title.keyword")) {
      disMaxQuery.add(
          QueryBuilders.constantScoreQuery(
                  QueryBuilders.termQuery(field, unquotedQuery).caseInsensitive(true))
              .boost(EXACT_NAME_CONSTANT_BOOST));

      // Synonym-expanded exact match for the same name written another way: e.g., "staging" also
      // matches name="stg", but "glue" does not match name="athena"
      if (synonymMap != null && !quoted) {
        Set<String> synonyms = synonymMap.get(unquotedQuery.toLowerCase());
        if (synonyms != null) {
          for (String synonym : synonyms) {
            if (!synonym.equalsIgnoreCase(unquotedQuery) && isSameName(unquotedQuery, synonym)) {
              disMaxQuery.add(
                  QueryBuilders.constantScoreQuery(
                          QueryBuilders.termQuery(field, synonym).caseInsensitive(true))
                      .boost(EXACT_NAME_CONSTANT_BOOST));
            }
          }
        }
      }
    }

    return Optional.of(disMaxQuery);
  }

  /**
   * Multi-match over the {@code .delimited} identity fields ({@code name}, {@code title}, {@code
   * urn}, {@code qualifiedName}, {@code id}, {@code displayName}) queried with the pre-escape
   * string, so an identifier that {@link #escapeSimpleQueryStringOperators} or {@link
   * #splitAlphanumericTokens} broke up ({@code load_job-0001}, {@code orders2017}) still matches
   * the whole token the {@code .delimited} analyzer indexes. Every term is required, but that
   * analyzer also emits the {@code _} and {@code -} separated parts of an identifier at the same
   * position, so for such an identifier a name holding one part matches as well. Empty when none of
   * these fields is queried.
   */
  private Optional<QueryBuilder> getDelimitedIdentityQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String rawQuery) {
    Map<String, Float> identityFields = new LinkedHashMap<>();
    applySearchFieldConfiguration(
            opContext,
            entitySpecs,
            getStandardFields(opContext.getEntityRegistry(), entitySpecs),
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getSearchFieldConfigDefault))
        .stream()
        .filter(SearchFieldConfig::isQueryByDefault)
        .filter(this::isDelimitedIdentityField)
        .forEach(cfg -> identityFields.merge(cfg.fieldName(), cfg.boost(), Math::max));
    if (identityFields.isEmpty()) {
      return Optional.empty();
    }
    MultiMatchQueryBuilder multiMatch =
        QueryBuilders.multiMatchQuery(rawQuery)
            .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .operator(Operator.AND)
            .tieBreaker(SIMPLE_QUERY_DISMAX_TIE_BREAKER);
    identityFields.forEach(multiMatch::field);
    return Optional.of(multiMatch);
  }

  /**
   * Stage 1: Synonym-priority query that matches using analyzer-level synonyms WITHOUT fuzzy
   * matching. This gives synonym-expanded terms a dedicated higher-scoring path through the DisMax,
   * ensuring that synonym matches rank above fuzzy matches (e.g., "staging" matching "stg" via the
   * synonym dictionary vs "stage" via edit distance).
   *
   * <p>Key design choices:
   *
   * <ul>
   *   <li>No fuzzy (~N) suffixes: prevents wrong fuzzy expansions from polluting results
   *   <li>OR operator: lenient threshold so a single synonym match is sufficient
   *   <li>Boost multiplier 1.5x: synonym matches score above fuzzy-only matches in DisMax
   *   <li>Same analyzer groups: leverages existing search-time synonym filters in ES
   * </ul>
   *
   * <p>If no synonyms apply, this query produces no/low results and the DisMax falls back to the
   * fuzzy query. This is safe because DisMax takes MAX(fuzzyScore, synonymScore).
   */
  private Optional<QueryBuilder> getSynonymPriorityQuery(
      @Nonnull OperationContext operationContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    EntityRegistry entityRegistry = operationContext.getEntityRegistry();

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (!executeSimpleQuery) {
      return Optional.empty();
    }

    DisMaxQueryBuilder synonymDisMax = QueryBuilders.disMaxQuery();
    synonymDisMax.tieBreaker(SIMPLE_QUERY_DISMAX_TIE_BREAKER);

    Set<SearchFieldConfig> baseFields =
        entitySpecs.stream()
            .map(spec -> getStandardFields(entityRegistry, spec))
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

    Set<SearchFieldConfig> configuredFields =
        applySearchFieldConfiguration(
            operationContext,
            entitySpecs,
            baseFields,
            customizedQueryHandler.resolveFieldConfiguration(
                operationContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getSearchFieldConfigDefault));

    Map<String, List<SearchFieldConfig>> analyzerGroup =
        configuredFields.stream()
            .filter(SearchFieldConfig::isQueryByDefault)
            .collect(Collectors.groupingBy(SearchFieldConfig::analyzer));

    analyzerGroup.keySet().stream()
        .sorted()
        .filter(str -> !str.contains("word_gram"))
        .forEach(
            analyzer -> {
              List<SearchFieldConfig> fieldConfigs = analyzerGroup.get(analyzer);

              Map<String, List<SearchFieldConfig>> fieldAnalyzers =
                  fieldConfigs.stream()
                      .collect(Collectors.groupingBy(SearchFieldConfig::fieldName));

              SimpleQueryStringBuilder simpleBuilder =
                  QueryBuilders.simpleQueryStringQuery(sanitizedQuery);
              simpleBuilder.analyzer(analyzer);
              simpleBuilder.defaultOperator(Operator.OR);

              for (Map.Entry<String, List<SearchFieldConfig>> fieldAnalyzer :
                  fieldAnalyzers.entrySet()) {
                float boost =
                    (float)
                        fieldAnalyzer.getValue().stream()
                            .mapToDouble(SearchFieldConfig::boost)
                            .max()
                            .orElse(1.0);
                simpleBuilder.field(fieldAnalyzer.getKey(), boost * SYNONYM_BOOST_MULTIPLIER);
              }

              if (!simpleBuilder.fields().isEmpty()) {
                synonymDisMax.add(simpleBuilder);
              }
            });

    return synonymDisMax.innerQueries().isEmpty() ? Optional.empty() : Optional.of(synonymDisMax);
  }

  /**
   * Stage 1: FQN (fully qualified name) matching for dot-delimited queries. When a query contains
   * dots (e.g., "mydb.analytics.dim_user"), it likely targets a specific dataset by its
   * database.schema.table path. Adds a high-boost BM25 match on qualifiedName.delimited so the
   * matching dataset ranks above entities that merely share individual tokens.
   *
   * <p>Uses analyzed match (AND operator) on .delimited subfields instead of leading wildcard
   * (*query). The delimited analyzer splits on dots, underscores, and other delimiters, so
   * "analytics.dim_user" matches "MYDB.ANALYTICS.DIM_USER" because all tokens (analytics, dim,
   * user) are present. This is 10-80x faster than leading wildcard queries and handles partial FQN
   * matches even better — the wildcard requires an exact suffix match while the analyzed match
   * finds documents containing all query tokens regardless of position.
   *
   * <p>Uses BM25 scoring (not constant_score) so IDF naturally weights rare tokens higher than
   * common ones (e.g., "time", "event"). This prevents queries with common tokens from getting
   * spurious high scores on loosely matching entities.
   */
  @VisibleForTesting
  Optional<QueryBuilder> getFqnMatchQuery(@Nonnull final String query) {
    // Only applies to queries containing dots (FQN-like)
    if (!query.contains(".") || query.trim().isEmpty()) {
      return Optional.empty();
    }
    // AND operator: the analyzer splits on dots/underscores/hyphens, so all FQN segments become
    // independent tokens that must all match. BM25 scoring ensures rare tokens contribute more to
    // the score than common ones.
    BoolQueryBuilder fqnBool = QueryBuilders.boolQuery();
    for (String field : fqnMatchFields()) {
      fqnBool.should(
          QueryBuilders.matchQuery(field, query).operator(Operator.AND).boost(FQN_MATCH_BOOST));
    }
    fqnBool.minimumShouldMatch(1);
    return Optional.of(fqnBool);
  }

  /**
   * Stage 1: All-terms match bonus for multi-word queries. Returns a query that scores entities
   * whose name/title contains ALL query tokens higher than entities matching only a subset.
   *
   * <p>For "conversion rate", the OR-based simple query scores "daily_fx_rates" (matches "rate"
   * only) nearly as high as "conversion_rate_dashboard" (matches both). This bonus clause uses AND
   * operator on name/title fields with a boost multiplier, giving full-match entities a dedicated
   * high-scoring path through the outer DisMax.
   *
   * <p>Only activates for queries with {@link #ALL_TERMS_MIN_TOKENS}+ tokens. Single-word queries
   * are unaffected (the simple query already handles them correctly).
   */
  @VisibleForTesting
  Optional<QueryBuilder> getAllTermsMatchBonus(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final List<EntitySpec> entitySpecs,
      @Nonnull final String sanitizedQuery) {
    String[] tokens = WHITESPACE_PATTERN.split(sanitizedQuery.trim());
    if (tokens.length < ALL_TERMS_MIN_TOKENS) {
      return Optional.empty();
    }

    // Collect name/title fields AND their .delimited subfields.
    // The base name field uses keyword/standard analyzer (doesn't split on underscores),
    // so "conversion_rate" is one token. The .delimited subfield splits on underscores,
    // hyphens, and other delimiters, so "conversion_rate" becomes ["conversion", "rate"].
    // We need both: base field catches "Conversion Rate" (space-separated), delimited
    // catches "conversion_rate" (underscore-separated).
    Set<SearchFieldConfig> allFields =
        entitySpecs.stream()
            .map(spec -> getStandardFields(entityRegistry, spec))
            .flatMap(Set::stream)
            .filter(SearchFieldConfig::isQueryByDefault)
            .filter(
                f ->
                    ALL_TERMS_BONUS_FIELDS.contains(f.shortName())
                        && (f.fieldName().equals(f.shortName()) || f.isDelimitedSubfield()))
            .collect(Collectors.toSet());

    if (allFields.isEmpty()) {
      return Optional.empty();
    }

    // Build a SimpleQueryString with AND operator on name/title + delimited fields.
    // No fuzzy — we want exact token matching for the bonus (fuzzy is in the OR clause).
    SimpleQueryStringBuilder allTermsQuery = QueryBuilders.simpleQueryStringQuery(sanitizedQuery);
    allTermsQuery.defaultOperator(Operator.AND);

    for (SearchFieldConfig field : allFields) {
      allTermsQuery.field(field.fieldName(), field.boost() * ALL_TERMS_MATCH_BOOST_MULTIPLIER);
    }

    return Optional.of(allTermsQuery);
  }

  /**
   * Stage 1: Adds an all-tokens-required match on description.delimited for long queries (4+
   * words). When a user pastes a phrase from an entity's description, the entity whose description
   * contains ALL query tokens should rank above entities that merely share a few tokens in their
   * name.
   *
   * <p>Uses match with AND operator on description.delimited (not match_phrase) because the
   * word_delimited analyzer produces overlapping sentence-level and word-level tokens at the same
   * positions, which breaks match_phrase position tracking. AND-match on a 4+ word query is
   * selective enough — few descriptions will contain all tokens by coincidence.
   */
  @VisibleForTesting
  Optional<QueryBuilder> getDescriptionPhraseMatchQuery(@Nonnull final String sanitizedQuery) {
    String[] tokens = WHITESPACE_PATTERN.split(sanitizedQuery.trim());
    if (tokens.length < DESCRIPTION_PHRASE_MIN_TOKENS) {
      return Optional.empty();
    }

    return Optional.of(
        QueryBuilders.matchQuery(descriptionPhraseField(), sanitizedQuery)
            .operator(Operator.AND)
            .boost(DESCRIPTION_PHRASE_MATCH_BOOST));
  }

  /**
   * Stage 1: whether a synonym is the same name as the query written another way, so it shares the
   * exact-name score. That holds when:
   *
   * <ul>
   *   <li>they differ only in spaces ({@code data platform} / {@code dataplatform});
   *   <li>one is a prefix of the other ({@code prod} / {@code production});
   *   <li>they are within two edits ({@code s3} / {@code s_3});
   *   <li>the shorter, of 3 to 5 letters, abbreviates the longer: same first and last letter,
   *       letters in order ({@code stg} / {@code staging}).
   * </ul>
   *
   * A related name ({@code glue} / {@code athena}) only matches through the analyzers' synonyms.
   */
  @VisibleForTesting
  public static boolean isSameName(@Nonnull String query, @Nonnull String synonym) {
    String q = query.trim().toLowerCase(Locale.ROOT);
    String s = synonym.trim().toLowerCase(Locale.ROOT);
    if (q.isEmpty() || s.isEmpty()) {
      return false;
    }
    if (WHITESPACE_PATTERN
        .matcher(q)
        .replaceAll("")
        .equals(WHITESPACE_PATTERN.matcher(s).replaceAll(""))) {
      return true;
    }
    if (WHITESPACE_PATTERN.matcher(q).find() || WHITESPACE_PATTERN.matcher(s).find()) {
      return false;
    }
    if (q.startsWith(s) || s.startsWith(q) || new LevenshteinDistance(2).apply(q, s) >= 0) {
      return true;
    }
    String shorter = q.length() <= s.length() ? q : s;
    String longer = q.length() <= s.length() ? s : q;
    if (shorter.length() < 3
        || shorter.length() > 5
        || shorter.charAt(0) != longer.charAt(0)
        || shorter.charAt(shorter.length() - 1) != longer.charAt(longer.length() - 1)) {
      return false;
    }
    int matched = 0;
    for (int i = 0; i < longer.length() && matched < shorter.length(); i++) {
      if (longer.charAt(i) == shorter.charAt(matched)) {
        matched++;
      }
    }
    return matched == shorter.length();
  }

  private Optional<QueryBuilder> getWildcardContainsQuery(
      @Nonnull EntityRegistry entityRegistry,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query) {
    String[] terms = WHITESPACE_PATTERN.split(query.trim());
    // Only apply to single-word queries >= 5 chars (multi-word wildcards are too expensive)
    if (terms.length != 1 || terms[0].length() < WILDCARD_MIN_LENGTH) {
      return Optional.empty();
    }

    String escaped = terms[0].toLowerCase().replace("*", "\\*").replace("?", "\\?");
    String wildcardPattern = "*" + escaped + "*";
    BoolQueryBuilder wildcardQuery = QueryBuilders.boolQuery();

    getStandardFields(entityRegistry, entitySpecs).stream()
        .filter(SearchFieldConfig::isDelimitedSubfield)
        .filter(cfg -> WILDCARD_CONTAINS_FIELDS.contains(cfg.shortName()))
        .forEach(
            cfg ->
                // No caseInsensitive(true): wildcardPattern is already lower-cased above and every
                // targeted .delimited subfield lower-cases at index time, so case folding is a
                // no-op on all engines. It is also unsafe on OpenSearch 3.x (Lucene 10): a
                // case-insensitive wildcard builds a case-folding automaton whose ByteRunAutomaton
                // can be null, throwing a server-side NPE ("runAutomaton is null") that fails the
                // whole shard.
                wildcardQuery.should(
                    QueryBuilders.wildcardQuery(cfg.fieldName(), wildcardPattern)
                        .boost(cfg.boost() * WILDCARD_BOOST_FACTOR)
                        .queryName("wildcard_" + cfg.shortName())));

    return wildcardQuery.should().isEmpty()
        ? Optional.empty()
        : Optional.of(wildcardQuery.minimumShouldMatch(1));
  }

  private Optional<QueryBuilder> getPrefixAndExactMatchQuery(
      @Nonnull OperationContext opContext,
      @Nonnull EntityRegistry entityRegistry,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
      @Nullable AspectRetriever aspectRetriever) {

    final boolean isPrefixQuery =
        customQueryConfig == null
            ? exactMatchConfiguration.isWithPrefix()
            : customQueryConfig.isPrefixMatchQuery();
    final boolean isExactQuery = customQueryConfig == null || customQueryConfig.isExactMatchQuery();

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    String unquotedQuery = unquote(query);

    getStandardFields(entityRegistry, entitySpecs)
        .forEach(
            searchFieldConfig -> {
              boolean caseSensitivityEnabled =
                  exactMatchConfiguration.getCaseSensitivityFactor() > 0.0f;
              float caseSensitivityFactor =
                  caseSensitivityEnabled
                      ? exactMatchConfiguration.getCaseSensitivityFactor()
                      : 1.0f;

              if (searchFieldConfig.isDelimitedSubfield() && isPrefixQuery) {
                finalQuery.should(
                    QueryBuilders.matchPhrasePrefixQuery(searchFieldConfig.fieldName(), query)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getPrefixFactor()
                                * caseSensitivityFactor)
                        .queryName(searchFieldConfig.shortName())); // less than exact
              }

              if (searchFieldConfig.isKeyword() && isExactQuery) {
                // It is important to use the subfield .keyword (it uses a different normalizer)
                // The non-.keyword field removes case information

                // Exact match case-sensitive
                if (caseSensitivityEnabled) {
                  finalQuery.should(
                      QueryBuilders.termQuery(
                              ESUtils.toKeywordField(
                                  opContext, searchFieldConfig.fieldName(), false, aspectRetriever),
                              unquotedQuery)
                          .caseInsensitive(false)
                          .boost(
                              searchFieldConfig.boost() * exactMatchConfiguration.getExactFactor())
                          .queryName(searchFieldConfig.shortName()));
                }

                // Exact match case-insensitive
                finalQuery.should(
                    QueryBuilders.termQuery(
                            ESUtils.toKeywordField(
                                opContext, searchFieldConfig.fieldName(), false, aspectRetriever),
                            unquotedQuery)
                        .caseInsensitive(true)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getExactFactor()
                                * caseSensitivityFactor)
                        .queryName(searchFieldConfig.fieldName()));
              }

              if (searchFieldConfig.isWordGramSubfield() && isPrefixQuery) {
                finalQuery.should(
                    QueryBuilders.matchPhraseQuery(
                            ESUtils.toKeywordField(
                                opContext, searchFieldConfig.fieldName(), false, aspectRetriever),
                            unquotedQuery)
                        .boost(
                            searchFieldConfig.boost()
                                * getWordGramFactor(searchFieldConfig.fieldName()))
                        .queryName(searchFieldConfig.shortName()));
              }
            });

    return finalQuery.should().size() > 0
        ? Optional.of(finalQuery.minimumShouldMatch(1))
        : Optional.empty();
  }

  /**
   * Stage 1: Prefix and exact match query. Differences from V2:
   *
   * <ul>
   *   <li>Applies a 6x boost multiplier so exact and prefix matches outscore fuzzy matches in the
   *       DisMax
   *   <li>Expands exact match eligibility to fields with keyword subfields (not just keyword
   *       fields)
   *   <li>Restricts exact and prefix clauses to the core identity fields
   *   <li>Expands name/title exact and prefix matches with the query's synonyms
   *   <li>Uses inner DisMax over per-field clauses so the best-matching field wins instead of
   *       summing all fields (avoids entity-type bias where Datasets with name+id+qualifiedName
   *       outrank Charts/Dashboards with only title).
   * </ul>
   */
  private Optional<QueryBuilder> getPrefixAndExactMatchQueryV2_5(
      @Nonnull EntityRegistry entityRegistry,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
      @Nullable AspectRetriever aspectRetriever,
      int prefixExpansions) {

    final boolean isPrefixQuery =
        customQueryConfig == null
            ? exactMatchConfiguration.isWithPrefix()
            : customQueryConfig.isPrefixMatchQuery();
    final boolean isExactQuery = customQueryConfig == null || customQueryConfig.isExactMatchQuery();

    DisMaxQueryBuilder disMaxQuery = QueryBuilders.disMaxQuery();
    disMaxQuery.tieBreaker(EXACT_PREFIX_DISMAX_TIE_BREAKER);
    // A quoted query names a value, so it does not expand to its synonyms
    final boolean quoted = FULLY_QUOTED_PATTERN.matcher(query.trim()).matches();
    String unquotedQuery = unquote(query);
    Map<String, BoolQueryBuilder> wordGramQueries = new HashMap<>();

    Map<String, SearchFieldConfig> uniqueFields = new LinkedHashMap<>();
    for (SearchFieldConfig cfg : getStandardFields(entityRegistry, entitySpecs)) {
      uniqueFields.merge(
          cfg.fieldName(),
          cfg,
          (oldCfg, newCfg) -> oldCfg.boost() > newCfg.boost() ? oldCfg : newCfg);
    }

    // Filter to core identity fields only. Reduces clause count significantly.
    // Non-core fields still get recall via SQS clauses.
    uniqueFields.values().stream()
        .filter(cfg -> EXACT_MATCH_CORE_FIELDS.contains(cfg.shortName()))
        .forEach(
            searchFieldConfig -> {
              boolean caseSensitivityEnabled =
                  exactMatchConfiguration.getCaseSensitivityFactor() > 0.0f;
              float caseSensitivityFactor =
                  caseSensitivityEnabled
                      ? exactMatchConfiguration.getCaseSensitivityFactor()
                      : 1.0f;

              if (searchFieldConfig.isDelimitedSubfield()
                  && !searchFieldConfig.isWordGramSubfield()
                  && isPrefixQuery
                  && !PREFIX_MATCH_EXCLUDED_FIELDS.contains(searchFieldConfig.fieldName())) {
                disMaxQuery.add(
                    QueryBuilders.matchPhrasePrefixQuery(searchFieldConfig.fieldName(), query)
                        .maxExpansions(prefixExpansions)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getPrefixFactor()
                                * caseSensitivityFactor
                                * EXACT_MATCH_BOOST_MULTIPLIER)
                        .queryName(searchFieldConfig.shortName())); // less than exact

                // Synonym-expanded prefix match on name/title fields only.
                // match_phrase_prefix uses the search_quote_analyzer which doesn't apply
                // synonyms, so "stg" won't prefix-match "STAGING_ORDERS".
                // Restrict to name/title to avoid exceeding ES max_clause_count (1024)
                // when queries with many synonyms are searched across all entity types.
                if (!quoted && EXACT_NAME_BOOST_FIELDS.contains(searchFieldConfig.shortName())) {
                  Set<String> prefixSynonyms = getSynonymMap().get(query.toLowerCase());
                  if (prefixSynonyms != null) {
                    for (String synonym : prefixSynonyms) {
                      if (!synonym.equalsIgnoreCase(query) && isSameName(query, synonym)) {
                        disMaxQuery.add(
                            QueryBuilders.matchPhrasePrefixQuery(
                                    searchFieldConfig.fieldName(), synonym)
                                .maxExpansions(prefixExpansions)
                                .boost(
                                    searchFieldConfig.boost()
                                        * exactMatchConfiguration.getPrefixFactor()
                                        * caseSensitivityFactor
                                        * EXACT_MATCH_BOOST_MULTIPLIER)
                                .queryName(searchFieldConfig.shortName()));
                      }
                    }
                  }
                }
              }

              // Exact match on keyword fields AND fields with keyword subfields.
              // Excludes word-gram subfields — they get coverage from the SQS clauses and the
              // word-gram matchQuery block below. Adding term queries on word-gram keyword fields
              // is redundant and adds ~30 extra clauses.
              boolean canExactMatch =
                  !searchFieldConfig.isWordGramSubfield()
                      && (searchFieldConfig.isKeyword() || searchFieldConfig.hasKeywordSubfield());
              if (canExactMatch && isExactQuery) {
                // Use .keyword subfield for non-keyword fields (preserves case info)
                String keywordField =
                    ESUtils.toKeywordField(
                        null, searchFieldConfig.fieldName(), false, aspectRetriever);

                // Exact match case-sensitive via keyword field (highest boost).
                if (caseSensitivityEnabled) {
                  disMaxQuery.add(
                      QueryBuilders.termQuery(keywordField, unquotedQuery)
                          .caseInsensitive(false)
                          .boost(
                              searchFieldConfig.boost()
                                  * exactMatchConfiguration.getExactFactor()
                                  * EXACT_MATCH_BOOST_MULTIPLIER)
                          .queryName(searchFieldConfig.shortName()));
                }

                // Exact match case-insensitive
                disMaxQuery.add(
                    QueryBuilders.termQuery(keywordField, unquotedQuery)
                        .caseInsensitive(true)
                        .boost(
                            searchFieldConfig.boost()
                                * exactMatchConfiguration.getExactFactor()
                                * caseSensitivityFactor
                                * EXACT_MATCH_BOOST_MULTIPLIER)
                        .queryName(searchFieldConfig.fieldName()));

                // IDF-independent constant-score boost for name/title exact matches.
                // Cross-entity searches suffer from IDF disparities: charts/dashboards with
                // exact name matches can score below datasets with prefix matches because
                // dataset documents have different term frequencies. A constant_score keeps an
                // exact name match above every partial match.
                if (EXACT_NAME_BOOST_FIELDS.contains(searchFieldConfig.shortName())) {
                  disMaxQuery.add(
                      QueryBuilders.constantScoreQuery(
                              QueryBuilders.termQuery(keywordField, unquotedQuery)
                                  .caseInsensitive(true))
                          .boost(EXACT_NAME_CONSTANT_BOOST));
                }

                // Synonym-expanded exact match on name/title keyword fields only.
                // Term queries on keyword fields don't go through any analyzer, so synonym
                // pairs (e.g., "stg"/"staging") fail to match. We expand synonyms only for
                // name/title fields to keep the clause count under ES's max_clause_count (1024).
                // Other keyword fields (qualifiedName, id, etc.) get synonym coverage through
                // their analyzed (non-keyword) counterparts.
                if (!quoted && EXACT_NAME_BOOST_FIELDS.contains(searchFieldConfig.shortName())) {
                  Set<String> synonyms = getSynonymMap().get(unquotedQuery.toLowerCase());
                  if (synonyms != null) {
                    for (String synonym : synonyms) {
                      if (!synonym.equalsIgnoreCase(unquotedQuery)
                          && isSameName(unquotedQuery, synonym)) {
                        disMaxQuery.add(
                            QueryBuilders.termQuery(keywordField, synonym)
                                .caseInsensitive(true)
                                .boost(
                                    searchFieldConfig.boost()
                                        * exactMatchConfiguration.getExactFactor()
                                        * caseSensitivityFactor
                                        * EXACT_MATCH_BOOST_MULTIPLIER)
                                .queryName(searchFieldConfig.fieldName()));
                        disMaxQuery.add(
                            QueryBuilders.constantScoreQuery(
                                    QueryBuilders.termQuery(keywordField, synonym)
                                        .caseInsensitive(true))
                                .boost(EXACT_NAME_CONSTANT_BOOST));
                      }
                    }
                  }
                }
              }

              if (searchFieldConfig.isWordGramSubfield() && isPrefixQuery) {
                // Use matchQuery (not matchPhraseQuery) so word-gram n-grams match regardless
                // of token order — e.g. query "user_active" matches bigrams from
                // "active_users_daily". Order sensitivity is handled by the exact/prefix clauses.
                String parentField =
                    WORD_GRAM_SUFFIX_PATTERN.matcher(searchFieldConfig.fieldName()).replaceAll("");
                wordGramQueries
                    .computeIfAbsent(parentField, k -> QueryBuilders.boolQuery())
                    .should(
                        QueryBuilders.matchQuery(
                                ESUtils.toKeywordField(
                                    null, searchFieldConfig.fieldName(), false, aspectRetriever),
                                unquotedQuery)
                            .boost(
                                searchFieldConfig.boost()
                                    * getWordGramFactor(searchFieldConfig.fieldName()))
                            .queryName(searchFieldConfig.shortName()));
              }
            });

    wordGramQueries.values().forEach(disMaxQuery::add);

    // DisMax returns 0 if no sub-query matches, so "at least one field must match" is implicit.
    return disMaxQuery.innerQueries().isEmpty() ? Optional.empty() : Optional.of(disMaxQuery);
  }

  private Optional<QueryBuilder> getStructuredQuery(
      @Nonnull EntityRegistry entityRegistry,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();

    final boolean executeStructuredQuery;
    if (customQueryConfig != null) {
      executeStructuredQuery = customQueryConfig.isStructuredQuery();
    } else {
      executeStructuredQuery = true;
    }

    if (executeStructuredQuery) {
      QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(sanitizedQuery);
      queryBuilder.defaultOperator(Operator.AND);
      getStandardFields(entityRegistry, entitySpecs)
          .forEach(entitySpec -> queryBuilder.field(entitySpec.fieldName(), entitySpec.boost()));
      result = Optional.of(queryBuilder);
    }
    return result;
  }

  /**
   * Stage 1: Structured query. A field configuration applies only when the caller names one in
   * {@code searchFlags}; the server default does not, so structured queries without the flag keep
   * their historical field set.
   */
  private Optional<QueryBuilder> getStructuredQueryV2_5(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      List<EntitySpec> entitySpecs,
      String sanitizedQuery) {
    Optional<QueryBuilder> result = Optional.empty();

    final boolean executeStructuredQuery;
    if (customQueryConfig != null) {
      executeStructuredQuery = customQueryConfig.isStructuredQuery();
    } else {
      executeStructuredQuery = true;
    }

    if (executeStructuredQuery) {
      QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(sanitizedQuery);
      queryBuilder.defaultOperator(Operator.AND);
      Set<SearchFieldConfig> fields = getStandardFields(opContext.getEntityRegistry(), entitySpecs);
      SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
      String requestedConfig = searchFlags != null ? searchFlags.getFieldConfiguration() : null;
      if (requestedConfig != null) {
        fields = applySearchFieldConfiguration(opContext, entitySpecs, fields, requestedConfig);
      }
      fields.forEach(field -> queryBuilder.field(field.fieldName(), field.boost()));
      result = Optional.of(queryBuilder);
    }
    return result;
  }

  static FunctionScoreQueryBuilder buildScoreFunctions(
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
      @Nonnull QueryBuilder queryBuilder) {

    if (customQueryConfig != null) {
      // Prefer configuration function scoring over annotation scoring
      return CustomizedQueryHandler.functionScoreQueryBuilder(
          opContext.getObjectMapper(), customQueryConfig, queryBuilder, query);
    } else {
      return QueryBuilders.functionScoreQuery(
              queryBuilder, buildAnnotationScoreFunctions(entitySpecs))
          .scoreMode(FunctionScoreQuery.ScoreMode.AVG) // Average score functions
          .boostMode(
              CombineFunction.MULTIPLY); // Multiply score function with the score from query;
    }
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder[] buildAnnotationScoreFunctions(
      @Nonnull List<EntitySpec> entitySpecs) {
    List<FunctionScoreQueryBuilder.FilterFunctionBuilder> finalScoreFunctions = new ArrayList<>();

    // Add a default weight of 1.0 to make sure the score function is larger than 1
    finalScoreFunctions.add(
        new FunctionScoreQueryBuilder.FilterFunctionBuilder(
            ScoreFunctionBuilders.weightFactorFunction(1.0f)));

    Map<String, SearchableAnnotation> annotations =
        entitySpecs.stream()
            .map(EntitySpec::getSearchableFieldSpecs)
            .flatMap(List::stream)
            .map(SearchableFieldSpec::getSearchableAnnotation)
            .collect(
                Collectors.toMap(
                    SearchableAnnotation::getFieldName,
                    annotation -> annotation,
                    (annotation1, annotation2) -> annotation1));

    for (Map.Entry<String, SearchableAnnotation> annotationEntry : annotations.entrySet()) {
      SearchableAnnotation annotation = annotationEntry.getValue();
      annotation.getWeightsPerFieldValue().entrySet().stream()
          .map(
              entry ->
                  buildWeightFactorFunction(
                      annotation.getFieldName(), entry.getKey(), entry.getValue()))
          .forEach(finalScoreFunctions::add);
    }

    Map<String, SearchScoreAnnotation> searchScoreAnnotationMap =
        entitySpecs.stream()
            .map(EntitySpec::getSearchScoreFieldSpecs)
            .flatMap(List::stream)
            .map(SearchScoreFieldSpec::getSearchScoreAnnotation)
            .collect(
                Collectors.toMap(
                    SearchScoreAnnotation::getFieldName,
                    annotation -> annotation,
                    (annotation1, annotation2) -> annotation1));
    for (Map.Entry<String, SearchScoreAnnotation> searchScoreAnnotationEntry :
        searchScoreAnnotationMap.entrySet()) {
      SearchScoreAnnotation annotation = searchScoreAnnotationEntry.getValue();
      finalScoreFunctions.add(buildScoreFunctionFromSearchScoreAnnotation(annotation));
    }

    return finalScoreFunctions.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]);
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder buildWeightFactorFunction(
      @Nonnull String fieldName, @Nonnull Object fieldValue, double weight) {
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(
        QueryBuilders.termQuery(fieldName, fieldValue),
        ScoreFunctionBuilders.weightFactorFunction((float) weight));
  }

  private static FunctionScoreQueryBuilder.FilterFunctionBuilder
      buildScoreFunctionFromSearchScoreAnnotation(@Nonnull SearchScoreAnnotation annotation) {
    FieldValueFactorFunctionBuilder scoreFunction =
        ScoreFunctionBuilders.fieldValueFactorFunction(annotation.getFieldName());
    scoreFunction.factor((float) annotation.getWeight());
    scoreFunction.missing(annotation.getDefaultValue());
    annotation.getModifier().ifPresent(modifier -> scoreFunction.modifier(mapModifier(modifier)));
    return new FunctionScoreQueryBuilder.FilterFunctionBuilder(scoreFunction);
  }

  private static FieldValueFactorFunction.Modifier mapModifier(
      SearchScoreAnnotation.Modifier modifier) {
    switch (modifier) {
      case LOG:
        return FieldValueFactorFunction.Modifier.LOG1P;
      case LN:
        return FieldValueFactorFunction.Modifier.LN1P;
      case SQRT:
        return FieldValueFactorFunction.Modifier.SQRT;
      case SQUARE:
        return FieldValueFactorFunction.Modifier.SQUARE;
      case RECIPROCAL:
        return FieldValueFactorFunction.Modifier.RECIPROCAL;
      default:
        return FieldValueFactorFunction.Modifier.NONE;
    }
  }

  public float getWordGramFactor(String fieldName) {
    if (fieldName.endsWith("Grams2")) {
      return wordGramConfiguration.getTwoGramFactor();
    } else if (fieldName.endsWith("Grams3")) {
      return wordGramConfiguration.getThreeGramFactor();
    } else if (fieldName.endsWith("Grams4")) {
      return wordGramConfiguration.getFourGramFactor();
    }
    throw new IllegalArgumentException(fieldName + " does not end with Grams[2-4]");
  }

  // visible for unit test
  public Map<String, SearchableAnnotation.FieldType> getAllFieldTypeFromSearchableRef(
      SearchableRefFieldSpec refFieldSpec,
      int depth,
      EntityRegistry entityRegistry,
      String prefixField) {
    final SearchableRefAnnotation searchableRefAnnotation =
        refFieldSpec.getSearchableRefAnnotation();
    // contains fieldName as key and SearchableAnnotation as value
    Map<String, SearchableAnnotation.FieldType> fieldNameMap = new HashMap<>();
    EntitySpec refEntitySpec = entityRegistry.getEntitySpec(searchableRefAnnotation.getRefType());
    String fieldName = searchableRefAnnotation.getFieldName();
    final SearchableAnnotation.FieldType fieldType = searchableRefAnnotation.getFieldType();
    if (!prefixField.isEmpty()) {
      fieldName = prefixField + "." + fieldName;
    }

    if (depth == 0) {
      // at depth 0 only URN is present then add and return
      fieldNameMap.put(fieldName, fieldType);
      return fieldNameMap;
    }
    String urnFieldName = fieldName + ".urn";
    fieldNameMap.put(urnFieldName, SearchableAnnotation.FieldType.URN);
    List<AspectSpec> aspectSpecs = refEntitySpec.getAspectSpecs();
    for (AspectSpec aspectSpec : aspectSpecs) {
      if (!SKIP_REFERENCE_ASPECT.contains(aspectSpec.getName())) {
        for (SearchableFieldSpec searchableFieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          String refFieldName = searchableFieldSpec.getSearchableAnnotation().getFieldName();
          refFieldName = fieldName + "." + refFieldName;
          final SearchableAnnotation searchableAnnotation =
              searchableFieldSpec.getSearchableAnnotation();
          final SearchableAnnotation.FieldType refFieldType = searchableAnnotation.getFieldType();
          fieldNameMap.put(refFieldName, refFieldType);
        }

        for (SearchableRefFieldSpec searchableRefFieldSpec :
            aspectSpec.getSearchableRefFieldSpecs()) {
          String refFieldName = searchableRefFieldSpec.getSearchableRefAnnotation().getFieldName();
          refFieldName = fieldName + "." + refFieldName;
          int newDepth =
              Math.min(depth - 1, searchableRefFieldSpec.getSearchableRefAnnotation().getDepth());
          fieldNameMap.putAll(
              getAllFieldTypeFromSearchableRef(
                  searchableRefFieldSpec, newDepth, entityRegistry, refFieldName));
        }
      }
    }
    return fieldNameMap;
  }
}
