package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.URN_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.DESCRIPTION;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.ENTITY_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.OTHER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.QUALIFIED_NAME;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.STEMMED;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.STEMMED_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.STEMMED_SUFFIX;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.TEXT;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.TEXT_SEARCH_ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.TEXT_SUFFIX;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.path;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields.weight;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.isQuoted;
import static com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler.unquote;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.config.search.custom.FieldConfiguration;
import com.linkedin.metadata.config.search.custom.QueryConfiguration;
import com.linkedin.metadata.config.search.custom.SearchFields;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver.Intent;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3IndexKeys;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFieldModel;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.request.CustomizedQueryHandler;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchQueryBuilder;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.IdentityQueryStrategy;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryIntent;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryStrategy;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.QueryUnderstanding;
import com.linkedin.metadata.search.elasticsearch.query.request.v3.understanding.SynonymMapLoader;
import com.linkedin.metadata.search.utils.ESUtils;
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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.SimpleQueryStringBuilder;

/**
 * Keyword query of the Search V3 entity indices. This is the Stage 1 query ported from DataHub
 * Cloud's Search V2.5: OR retrieval with fuzzy, synonym and wildcard recall under a DisMax root, so
 * the best matching clause sets the score. The Stage 1 methods keep DataHub Cloud's {@code V2_5}
 * names so the code stays easy to merge.
 *
 * <p>This is the Search V3 Java fallback. Templates own new fields, boosts, lexical clauses, and
 * ranking, and this builder serves the same V2.5 query when templates are off, in shadow, or
 * outside a canary. {@code SearchTemplateRetirement.JAVA_LEXICAL_BUILDER_RETIRED} stays false until
 * that operational window has elapsed. {@link #FROZEN_AGAINST_NEW_RELEVANCE_POLICY} keeps new
 * relevance work on templates without removing this path.
 *
 * <p>Analyzed clauses read each shared root field's {@code text} subfield, and its {@code stemmed}
 * subfield at half that weight (see {@link
 * com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields}). Exact matches read
 * the keywords of the root fields and the urn, as on V2. A field configuration names the shared
 * fields to search, either directly or by a field that feeds one. A structured property is named by
 * {@code structuredProperties} or by its full field path, and is not a shared field.
 */
@Slf4j
public class V3SearchQueryBuilder extends SearchQueryBuilder {

  /**
   * Relevance policy for this builder is frozen. The fallback still calls {@link #buildQuery}. New
   * ranking changes go to templates.
   */
  public static final boolean FROZEN_AGAINST_NEW_RELEVANCE_POLICY = true;

  private static final int WILDCARD_MIN_LENGTH = 5;
  private static final float WILDCARD_BOOST_FACTOR = 0.3f;
  private static final float EXACT_PREFIX_DISMAX_TIE_BREAKER = 0.01f;

  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  private static final Pattern LETTER_DIGIT_BOUNDARY = Pattern.compile("(?<=[a-zA-Z])(?=\\d)");
  private static final Pattern DIGIT_LETTER_BOUNDARY = Pattern.compile("(?<=\\d)(?=[a-zA-Z])");
  private static final Pattern FUZZY_SPLIT_PATTERN = Pattern.compile("[\\s_\\-]+");

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

  private static final QueryStrategy IDENTITY_STRATEGY = new IdentityQueryStrategy();

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
    final SearchClauseBudget.FieldCounts fieldCounts =
        simpleSyntax ? indexFieldCounts(opContext, entitySpecs) : null;
    final String colonStripped =
        simpleSyntax
            ? SearchClauseBudget.firstTermsWithinBudget(query.replaceFirst("^:+", ""), fieldCounts)
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
                SearchClauseBudget.fuzzyExpansions(sanitizedQuery, operatorEscaped, fieldCounts))
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
        // Light path: two constant_score clauses (name, title) so exact name
        // matches still rank first regardless of cross-entity IDF disparities.
        if (customQueryConfig == null || customQueryConfig.isExactMatchQuery()) {
          getLightweightExactMatchBoost(colonStripped, getSynonymMap()).ifPresent(disMaxQuery::add);
        }
      } else {
        // Exact/prefix match with term queries, phrase prefixes, synonyms and word grams.
        getPrefixAndExactMatchQueryV2_5(
                opContext,
                customQueryConfig,
                entitySpecs,
                colonStripped,
                SearchClauseBudget.prefixExpansions(operatorEscaped))
            .ifPresent(disMaxQuery::add);
        // Wildcard contains query for substring matching, except for a quoted query, which asks
        // for the words as they are
        if (anyTextMatch && !FULLY_QUOTED_PATTERN.matcher(colonStripped.trim()).matches()) {
          getWildcardContainsQuery(opContext, entitySpecs, colonStripped)
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
        getAllTermsMatchBonus(opContext, entitySpecs, lightSanitizedQuery)
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
                opContext,
                customQueryConfig,
                entitySpecs,
                withoutQueryPrefix,
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
    return makeFuzzyQuery(query, 5, 7, 1, 2);
  }

  /**
   * Adds {@code ~edits} using the template's token threshold and short and medium edit counts. The
   * no-argument form keeps the historical 5 / 7 character bands and 1 / 2 edits.
   */
  @Nonnull
  public static String makeFuzzyQuery(
      @Nonnull final String query,
      int fuzzyTokenThreshold,
      int mediumFuzzyTokenThreshold,
      int shortFuzzyEdits,
      int mediumFuzzyEdits) {
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
      if (termLength >= mediumFuzzyTokenThreshold && mediumFuzzyEdits > 0) {
        fuzzyQuery.append('~').append(mediumFuzzyEdits);
      } else if (termLength >= fuzzyTokenThreshold && shortFuzzyEdits > 0) {
        fuzzyQuery.append('~').append(shortFuzzyEdits);
      }

      if (i < terms.length - 1) {
        fuzzyQuery.append(" ");
      }
    }

    return fuzzyQuery.toString();
  }

  /**
   * The most fields one index queries by default. Lucene counts clauses per index, and a field an
   * index does not map adds none.
   */
  private SearchClauseBudget.FieldCounts indexFieldCounts(
      @Nonnull OperationContext opContext, @Nonnull List<EntitySpec> entitySpecs) {
    int fields = 0;
    int wordGramFields = 0;
    for (List<EntitySpec> indexSpecs :
        entitySpecs.stream().collect(Collectors.groupingBy(V3IndexKeys::resolve)).values()) {
      Map<Boolean, Set<List<String>>> analyzedFields =
          applySearchFieldConfiguration(
                  opContext,
                  indexSpecs,
                  getStandardFields(opContext, indexSpecs),
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
    return new SearchClauseBudget.FieldCounts(fields, wordGramFields);
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

    final boolean executeSimpleQuery;
    if (customQueryConfig != null) {
      executeSimpleQuery = customQueryConfig.isSimpleQuery();
    } else {
      executeSimpleQuery = !(isQuoted(sanitizedQuery) && exactMatchConfiguration.isExclusive());
    }

    if (executeSimpleQuery) {
      Set<SearchFieldConfig> baseFields =
          entitySpecs.stream()
              .map(spec -> getStandardFields(operationContext, spec))
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
      configuredFields = supportedFields(configuredFields, entitySpecs);

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

                addSimpleQuery(
                    disMaxQuery,
                    queryToUse,
                    analyzer,
                    uniqueFields.values(),
                    1.0f,
                    !isWordGram,
                    maxExpansions);
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

    // EXACT_NAME and FQN queries search the name-related subfields. A structured property is named
    // by its own path, so a name-focused light query does not search them. KEYWORD queries search
    // every field queried by default. Word grams need 2+ tokens, so single words skip them.
    boolean useNameFocusedFields = intent == QueryIntent.EXACT_NAME || intent == QueryIntent.FQN;
    boolean isTrueSingleWord = SINGLE_WORD_PATTERN.matcher(matchQuery.trim()).matches();
    Map<String, Float> fieldBoosts = new LinkedHashMap<>();
    boolean[] lenient = {false};
    configuredFields.stream()
        .filter(SearchFieldConfig::isQueryByDefault)
        .filter(cfg -> cfg.isDelimitedSubfield() || (cfg.isWordGramSubfield() && !isTrueSingleWord))
        .filter(cfg -> !LIGHT_PATH_EXCLUDED_FIELDS.contains(cfg.shortName()))
        .filter(cfg -> !useNameFocusedFields || EXACT_NAME_FOCUSED_FIELDS.contains(cfg.shortName()))
        .forEach(
            cfg -> {
              fieldBoosts.merge(cfg.fieldName(), cfg.boost(), Math::max);
              if (StructuredPropertyUtils.isStructuredPropertyField(cfg.fieldName())) {
                lenient[0] = true;
              }
            });
    if (fieldBoosts.isEmpty()) {
      return Optional.empty();
    }
    if (lenient[0]) {
      // A property field may be mapped on only some indices of a multi-index search. Lenient
      // applies to the whole multi_match; an unmapped shared field would already fail the query.
      multiMatch.lenient(true);
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
        QueryBuilders.termQuery("name", exactQuery)
            .caseInsensitive(true)
            .boost(EXACT_NAME_CONSTANT_BOOST));
    nameBoost.add(
        QueryBuilders.termQuery("title", exactQuery)
            .caseInsensitive(true)
            .boost(EXACT_NAME_CONSTANT_BOOST));
    nameBoost.add(multiMatch);
    return Optional.of(nameBoost);
  }

  /**
   * Light path: minimal exact-match boost using only constant_score clauses on name and title.
   * Keeps exact name matches above partial matches without the many clauses of the full
   * exact/prefix query (term queries, phrase prefixes, synonyms and word grams on every core
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

    for (String field : List.of("name", "title")) {
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
            getStandardFields(opContext, entitySpecs),
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
            .map(spec -> getStandardFields(operationContext, spec))
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
    configuredFields = supportedFields(configuredFields, entitySpecs);

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

              Map<String, SearchFieldConfig> uniqueFields = new LinkedHashMap<>();
              for (Map.Entry<String, List<SearchFieldConfig>> fieldAnalyzer :
                  fieldAnalyzers.entrySet()) {
                float boost =
                    (float)
                        fieldAnalyzer.getValue().stream()
                            .mapToDouble(SearchFieldConfig::boost)
                            .max()
                            .orElse(1.0);
                SearchFieldConfig representative = fieldAnalyzer.getValue().get(0);
                uniqueFields.put(
                    fieldAnalyzer.getKey(), representative.toBuilder().boost(boost).build());
              }
              addSimpleQuery(
                  synonymDisMax,
                  sanitizedQuery,
                  analyzer,
                  uniqueFields.values(),
                  SYNONYM_BOOST_MULTIPLIER,
                  false,
                  0);
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
      @Nonnull final OperationContext opContext,
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
            .map(spec -> getStandardFields(opContext, spec))
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
      @Nonnull OperationContext opContext,
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

    getStandardFields(opContext, entitySpecs).stream()
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
      @Nonnull OperationContext opContext,
      @Nullable QueryConfiguration customQueryConfig,
      @Nonnull List<EntitySpec> entitySpecs,
      String query,
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
    for (SearchFieldConfig cfg : getStandardFields(opContext, entitySpecs)) {
      uniqueFields.merge(
          cfg.fieldName(),
          cfg,
          (oldCfg, newCfg) -> oldCfg.boost() > newCfg.boost() ? oldCfg : newCfg);
    }

    // Filter to core identity fields only. Reduces clause count significantly.
    // Non-core fields still get recall via SQS clauses.
    SearchFieldResolver v3FieldResolver = resolver(entitySpecs);
    uniqueFields.values().stream()
        .filter(
            cfg ->
                EXACT_MATCH_CORE_FIELDS.contains(cfg.shortName())
                    || cfg.fieldName().contains(V3SearchFields.PARSED_URN_SUFFIX)
                    || cfg.fieldName().contains(StructuredPropertyUrnIdentity.FIELD_SUFFIX)
                    || !v3FieldResolver.parsedKeywordPaths(baseName(cfg.fieldName())).isEmpty())
        .filter(
            cfg ->
                !v3FieldResolver.contains(cfg.fieldName())
                    || v3FieldResolver.supports(cfg.fieldName(), Intent.EXACT))
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
                  && !PREFIX_MATCH_EXCLUDED_FIELDS.contains(searchFieldConfig.fieldName())
                  && (searchFieldConfig.fieldName().startsWith("name.")
                      || searchFieldConfig.fieldName().startsWith("qualifiedName."))) {
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
              if (isExactQuery) {
                List<String> keywordFields =
                    exactKeywordFields(
                        v3FieldResolver,
                        searchFieldConfig,
                        canExactMatch,
                        opContext.getAspectRetriever());
                for (String keywordField : keywordFields) {
                  // A term query does not run keyword_normalizer. Fold values for fields that store
                  // the normalized token; caseInsensitive does not ASCII-fold.
                  final String termValue =
                      exactTermValue(v3FieldResolver, keywordField, unquotedQuery);
                  // Exact match case-sensitive via keyword field (highest boost).
                  if (caseSensitivityEnabled) {
                    disMaxQuery.add(
                        QueryBuilders.termQuery(keywordField, termValue)
                            .caseInsensitive(false)
                            .boost(
                                searchFieldConfig.boost()
                                    * exactMatchConfiguration.getExactFactor()
                                    * EXACT_MATCH_BOOST_MULTIPLIER)
                            .queryName(searchFieldConfig.shortName()));
                  }

                  // Exact match case-insensitive
                  disMaxQuery.add(
                      QueryBuilders.termQuery(keywordField, termValue)
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
                  if (EXACT_NAME_BOOST_FIELDS.contains(searchFieldConfig.shortName())
                      && keywordField.equals(keywordFields.get(0))) {
                    disMaxQuery.add(
                        QueryBuilders.constantScoreQuery(
                                QueryBuilders.termQuery(keywordField, termValue)
                                    .caseInsensitive(true))
                            .boost(EXACT_NAME_CONSTANT_BOOST));
                  }
                }
                if (!keywordFields.isEmpty()) {
                  String keywordField = keywordFields.get(0);

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
                                ESUtils.toV3KeywordField(
                                    null,
                                    searchFieldConfig.fieldName(),
                                    opContext.getAspectRetriever()),
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
      Set<SearchFieldConfig> fields = getStandardFields(opContext, entitySpecs);
      SearchFlags searchFlags = opContext.getSearchContext().getSearchFlags();
      String requestedConfig = searchFlags != null ? searchFlags.getFieldConfiguration() : null;
      if (requestedConfig != null) {
        fields = applySearchFieldConfiguration(opContext, entitySpecs, fields, requestedConfig);
      }
      fields = supportedFields(fields, entitySpecs);
      List<SearchFieldConfig> shared = new ArrayList<>();
      List<SearchFieldConfig> properties = new ArrayList<>();
      for (SearchFieldConfig field : fields) {
        if (StructuredPropertyUtils.isStructuredPropertyField(field.fieldName())) {
          // An unqualified /q matches only properties included by default. A fielded query still
          // names its field whether or not it is listed here.
          if (field.isQueryByDefault()) {
            properties.add(field);
          }
        } else {
          shared.add(field);
        }
      }
      // query_string ignores fields() when the query names a field, so the same fielded text must
      // not run on both clauses. Property fields stay lenient; every other field stays strict.
      result =
          StructuredQueryRouting.build(sanitizedQuery, shared, properties, resolver(entitySpecs));
    }
    return result;
  }

  @Override
  @Nullable
  public QueryBuilder buildQuery(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext,
      boolean skipExpensiveClauses) {
    QueryConfiguration customQueryConfig =
        customizedQueryHandler.lookupQueryConfig(query).orElse(null);

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
    final QueryBuilder queryBuilder =
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
    if (queryBuilder == null) {
      return null;
    }
    return buildScoreFunctions(opContext, customQueryConfig, entitySpecs, query, queryBuilder);
  }

  // A stemmed match is a looser one, so it counts for less than the same word unstemmed
  private static final float STEMMED_FACTOR = 0.5f;

  // The Stage 1 field a shared field stands for, where its name differs
  private static final Map<String, String> STAGE_1_NAMES = Map.of(ENTITY_NAME, "name");

  private static final Set<String> IDENTITY_TEXT_FIELDS =
      Set.of(textField(ENTITY_NAME), textField(QUALIFIED_NAME));

  private final StructuredPropertySearchCatalog structuredPropertySearchCatalog;
  @Nullable private final SearchFieldResolver v3SearchFieldResolver;

  public V3SearchQueryBuilder(
      @Nonnull final SearchConfiguration searchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration) {
    this(searchConfiguration, customSearchConfiguration, null);
  }

  public V3SearchQueryBuilder(
      @Nonnull final SearchConfiguration searchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration,
      @Nullable final SearchFieldResolver v3SearchFieldResolver) {
    super(searchConfiguration, customSearchConfiguration);
    this.v3SearchFieldResolver = v3SearchFieldResolver;
    this.structuredPropertySearchCatalog =
        new StructuredPropertySearchCatalog(searchConfiguration.getStructuredProperties());
  }

  /**
   * Structured-property fields whose {@code copyTo} writes into a shared field this search reads.
   * Matched fields read these values, because {@code copy_to} does not put them on the destination
   * in {@code _source}.
   */
  @Nonnull
  public List<String> copiedStructuredPropertyFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Set<String> searched = searchedFields(opContext, entitySpecs).keySet();
    final List<String> fields = new ArrayList<>();
    final V3SearchFieldModel model = V3SearchFieldModel.of(entitySpecs);
    for (StructuredPropertySearchCatalog.IndexedProperty property :
        structuredPropertySearchCatalog.indexed(opContext, entitySpecs)) {
      if (model.copyDestinations(property.copyTo()).stream().anyMatch(searched::contains)) {
        fields.add(property.fullTextPath());
      }
    }
    return fields;
  }

  /**
   * The shared full-text fields a query of these entities reads, after the request's field
   * configuration, each with the root fields that feed it, as the indices they are searched in map
   * them.
   */
  @Nonnull
  public Map<String, List<String>> searchedFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Map<String, List<String>> fields = cachedFullTextFields(opContext.getEntityRegistry());
    final Map<String, List<String>> configured =
        configuredFields(
            fields,
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getSearchFieldConfigDefault));
    return configured != null ? configured : fields;
  }

  /**
   * Fields the Java fallback queries. A field-configuration label from the custom search file
   * narrows them. An active template does not use this; it starts from {@link
   * #templateBaseFieldConfigs} and applies its own add, remove, and replace.
   */
  @Nonnull
  public List<SearchFieldConfig> searchedFieldConfigs(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Collection<EntitySpec> searchable =
        V3SearchFields.searchableEntitySpecs(opContext.getEntityRegistry());
    return applySearchFieldConfiguration(
            opContext,
            searchable,
            getStandardFields(opContext, searchable),
            customizedQueryHandler.resolveFieldConfiguration(
                opContext.getSearchContext().getSearchFlags(),
                CustomConfiguration::getSearchFieldConfigDefault))
        .stream()
        .filter(SearchFieldConfig::isQueryByDefault)
        .toList();
  }

  /**
   * Stage 1 {@code queryByDefault} fields a template starts from. The template's add, remove, and
   * replace choose the rest. A {@code fieldConfiguration} search flag is not applied.
   */
  @Nonnull
  public List<SearchFieldConfig> templateBaseFieldConfigs(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    final Collection<EntitySpec> searchable =
        V3SearchFields.searchableEntitySpecs(opContext.getEntityRegistry());
    return getStandardFields(opContext, searchable).stream()
        .filter(SearchFieldConfig::isQueryByDefault)
        .toList();
  }

  /** Search V3 queries the resolved annotation. A {@code v3} override does not change Search V2. */
  @Override
  @Nonnull
  protected Optional<SearchableAnnotation> unqualifiedQueryAnnotation(
      @Nonnull SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation resolved = fieldSpec.getSearchableAnnotation().resolvedForV3();
    if (!resolved.isV3Searchable() || !resolved.isQueryByDefault()) {
      return Optional.empty();
    }
    return Optional.of(resolved);
  }

  @Override
  public Set<SearchFieldConfig> getStandardFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    return supportedFields(
        withSharedFields(
            super.getStandardFields(opContext, entitySpecs),
            cachedFullTextFields(opContext.getEntityRegistry()),
            cachedAnalyzerSubfields(opContext.getEntityRegistry()),
            opContext,
            entitySpecs),
        entitySpecs);
  }

  @Override
  protected Set<SearchFieldConfig> getStandardFields(
      @Nonnull OperationContext opContext, @Nonnull EntitySpec entitySpec) {
    return supportedFields(
        withSharedFields(
            super.getStandardFields(opContext, entitySpec),
            cachedFullTextFields(opContext.getEntityRegistry()),
            cachedAnalyzerSubfields(opContext.getEntityRegistry()),
            opContext,
            List.of(entitySpec)),
        List.of(entitySpec));
  }

  /**
   * Keeps the subfields of the shared fields the configuration selects, and the root fields that
   * feed one of those. A root field that feeds no listed shared field, such as the urn or a field
   * of a referenced entity, lands in {@code other}.
   */
  @Override
  protected Set<SearchFieldConfig> applySearchFieldConfiguration(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nonnull Set<SearchFieldConfig> fields,
      @Nullable String label) {
    final Map<String, List<String>> fullTextFields =
        cachedFullTextFields(opContext.getEntityRegistry());
    final Map<String, List<String>> selected = configuredFields(fullTextFields, label);
    if (selected == null) {
      // A replace that names only property paths selects no shared field, which otherwise falls
      // back to every field. Keep just those properties.
      final SearchFields searchFields = getSearchFields(label);
      if (searchFields != null && searchFields.isValid() && !searchFields.getReplace().isEmpty()) {
        final Set<SearchFieldConfig> properties = selectStructuredProperties(fields, searchFields);
        if (!properties.isEmpty()) {
          return properties;
        }
      }
      return fields;
    }
    final Map<String, Set<String>> destinations = new HashMap<>();
    fullTextFields.forEach(
        (name, roots) ->
            roots.forEach(
                root -> destinations.computeIfAbsent(root, k -> new HashSet<>()).add(name)));
    final Set<SearchFieldConfig> result =
        fields.stream()
            .filter(field -> !StructuredPropertyUtils.isStructuredPropertyField(field.fieldName()))
            .filter(
                field -> {
                  final String fieldName = field.fieldName();
                  final String name = baseName(fieldName);
                  if (isSharedAnalyzerField(fieldName, fullTextFields.keySet())) {
                    return selected.containsKey(name);
                  }
                  if (selected.containsKey(name)) {
                    return true;
                  }
                  return destinations.getOrDefault(name, Set.of(OTHER)).stream()
                      .anyMatch(selected::containsKey);
                })
            .collect(Collectors.toCollection(HashSet::new));
    // A property path starts with structuredProperties., which baseName would collapse to the
    // category, and it must not fall through to other.
    final SearchFields searchFields = getSearchFields(label);
    if (searchFields != null) {
      result.addAll(selectStructuredProperties(fields, searchFields));
    }
    return result;
  }

  protected List<String> fqnMatchFields() {
    // On V3 an id is matched analyzed only in other
    return List.of(textField(QUALIFIED_NAME), textField(OTHER));
  }

  protected String descriptionPhraseField() {
    return textField(DESCRIPTION);
  }

  protected boolean isDelimitedIdentityField(@Nonnull SearchFieldConfig cfg) {
    return IDENTITY_TEXT_FIELDS.contains(cfg.fieldName());
  }

  @Nonnull
  private Map<String, List<String>> cachedFullTextFields(@Nonnull EntityRegistry entityRegistry) {
    return V3SearchFieldModel.forSearchableEntities(entityRegistry).fullTextFields();
  }

  /**
   * The root fields of these standard fields that a V3 index searches, and the analyzed subfields
   * its labels declare. A label without {@code text} is not a full-text target. A root field that
   * is not a shared label has no analyzed subfields on V3.
   */
  @Nonnull
  private Set<SearchFieldConfig> withSharedFields(
      @Nonnull final Set<SearchFieldConfig> standardFields,
      @Nonnull final Map<String, List<String>> fullTextFields,
      @Nonnull final Map<String, Set<String>> analyzerSubfields,
      @Nonnull final OperationContext opContext,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchFieldConfig> fields =
        standardFields.stream()
            .filter(
                field ->
                    !field.isDelimitedSubfield()
                        && !field.isWordGramSubfield()
                        && !URN_SEARCH_ANALYZER.equals(field.analyzer()))
            .map(
                field ->
                    field.toBuilder()
                        .hasDelimitedSubfield(false)
                        .hasWordGramSubfields(false)
                        // Exact terms still read this root. The fuzzy query uses the shared
                        // analyzed fields, whose weight is already scaled by the partial factor.
                        .isQueryByDefault(false)
                        .build())
            .collect(Collectors.toCollection(HashSet::new));
    for (String name : fullTextFields.keySet()) {
      final Set<String> subfields = analyzerSubfields.getOrDefault(name, Set.of());
      final float boost = weight(entitySpecs, name) * partialConfiguration.getFactor();
      final String indexed = name;
      if (subfields.contains(TEXT)) {
        fields.add(sharedField(indexed, name, TEXT, TEXT_SEARCH_ANALYZER, boost));
      }
      if (subfields.contains(STEMMED)) {
        fields.add(
            sharedField(indexed, name, STEMMED, STEMMED_SEARCH_ANALYZER, boost * STEMMED_FACTOR));
      }
    }
    final float factor = partialConfiguration.getFactor();
    for (StructuredPropertySearchCatalog.IndexedProperty property :
        structuredPropertySearchCatalog.indexed(opContext, entitySpecs)) {
      fields.add(
          propertyField(
              property.fieldPath(),
              property.fullTextPath(),
              TEXT,
              TEXT_SEARCH_ANALYZER,
              property.textWeight() * factor,
              property.queryByDefault()));
      fields.add(
          propertyField(
              property.fieldPath(),
              property.fullTextPath(),
              STEMMED,
              STEMMED_SEARCH_ANALYZER,
              property.stemmedWeight() * factor,
              property.queryByDefault()));
    }
    return fields;
  }

  @Nonnull
  private Map<String, Set<String>> cachedAnalyzerSubfields(@Nonnull EntityRegistry entityRegistry) {
    return V3SearchFieldModel.forSearchableEntities(entityRegistry).analyzerSubfields();
  }

  @Nonnull
  private SearchFieldResolver resolver(@Nonnull Collection<EntitySpec> entitySpecs) {
    return v3SearchFieldResolver != null
        ? v3SearchFieldResolver
        : V3SearchFieldModel.of(entitySpecs).resolver();
  }

  @Nonnull
  private Set<SearchFieldConfig> supportedFields(
      @Nonnull Set<SearchFieldConfig> fields, @Nonnull Collection<EntitySpec> entitySpecs) {
    SearchFieldResolver fieldResolver = resolver(entitySpecs);
    return fields.stream()
        .filter(
            field ->
                !fieldResolver.contains(field.fieldName())
                    || fieldResolver.supports(
                        field.fieldName(),
                        field.fieldName().endsWith(TEXT_SUFFIX)
                                || field.fieldName().endsWith(STEMMED_SUFFIX)
                            ? Intent.FULL_TEXT
                            : Intent.EXACT))
        .collect(Collectors.toSet());
  }

  private static boolean isSharedAnalyzerField(
      @Nonnull final String fieldName, @Nonnull final Set<String> sharedFields) {
    for (String sharedField : sharedFields) {
      final String sharedPath = path(sharedField);
      if (fieldName.equals(sharedPath + TEXT_SUFFIX)
          || fieldName.equals(sharedPath + STEMMED_SUFFIX)) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private static SearchFieldConfig propertyField(
      @Nonnull final String fieldPath,
      @Nonnull final String fullTextPath,
      @Nonnull final String subfield,
      @Nonnull final String analyzer,
      final float boost,
      final boolean queryByDefault) {
    return SearchFieldConfig.builder()
        .fieldName(fullTextPath + "." + subfield)
        // The property path, so a name-focused light query does not treat it as a name field and
        // a field configuration can name this property without collapsing to the category. A URN
        // property's analyzed fields live on the identity sibling; the short name stays the
        // property path.
        .shortName(fieldPath)
        .isDelimitedSubfield(true)
        .boost(boost)
        .analyzer(analyzer)
        .isQueryByDefault(queryByDefault)
        .build();
  }

  /**
   * Properties the configuration keeps. {@code structuredProperties} names every opted-in property.
   * A full path, with or without {@code .text} or {@code .stemmed}, names one. Added and replaced
   * properties are queried even when their own {@code queryByDefault} is false.
   */
  @Nonnull
  private static Set<SearchFieldConfig> selectStructuredProperties(
      @Nonnull Set<SearchFieldConfig> fields, @Nonnull SearchFields searchFields) {
    final List<SearchFieldConfig> properties =
        fields.stream()
            .filter(field -> StructuredPropertyUtils.isStructuredPropertyField(field.fieldName()))
            .toList();
    if (!searchFields.getReplace().isEmpty()) {
      return properties.stream()
          .filter(field -> namesSelect(searchFields.getReplace(), field))
          .map(field -> field.toBuilder().isQueryByDefault(true).build())
          .collect(Collectors.toSet());
    }
    final Set<SearchFieldConfig> selected = new HashSet<>();
    properties.stream()
        .filter(SearchFieldConfig::isQueryByDefault)
        .filter(field -> !namesSelect(searchFields.getRemove(), field))
        .forEach(selected::add);
    properties.stream()
        .filter(field -> namesSelect(searchFields.getAdd(), field))
        .map(field -> field.toBuilder().isQueryByDefault(true).build())
        .forEach(selected::add);
    return selected;
  }

  private static boolean namesSelect(
      @Nonnull List<String> names, @Nonnull SearchFieldConfig field) {
    final String path = withoutAnalyzedSuffix(field.shortName());
    for (String name : names) {
      final String normalized = withoutAnalyzedSuffix(stripSearchPrefix(name));
      if (V3SearchFields.STRUCTURED_PROPERTIES.equals(normalized) || normalized.equals(path)) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private static String stripSearchPrefix(@Nonnull String name) {
    return name.startsWith("_search.") ? name.substring("_search.".length()) : name;
  }

  @Nonnull
  private static String withoutAnalyzedSuffix(@Nonnull String name) {
    for (String suffix : List.of(TEXT_SUFFIX, STEMMED_SUFFIX)) {
      if (name.endsWith(suffix)) {
        return name.substring(0, name.length() - suffix.length());
      }
    }
    return name;
  }

  /**
   * One simple query per analyzer, split so structured-property fields that may be unmapped on some
   * indices of a multi-index search are on their own lenient clause. Shared fields stay on a strict
   * clause.
   */
  private static void addSimpleQuery(
      @Nonnull DisMaxQueryBuilder disMax,
      @Nonnull String query,
      @Nonnull String analyzer,
      @Nonnull Collection<SearchFieldConfig> fields,
      float boostMultiplier,
      boolean fuzzy,
      int maxExpansions) {
    Map<Boolean, List<SearchFieldConfig>> groups = new LinkedHashMap<>();
    for (SearchFieldConfig field : fields) {
      groups
          .computeIfAbsent(
              StructuredPropertyUtils.isStructuredPropertyField(field.fieldName()),
              key -> new ArrayList<>())
          .add(field);
    }
    for (Map.Entry<Boolean, List<SearchFieldConfig>> group : groups.entrySet()) {
      SimpleQueryStringBuilder builder = QueryBuilders.simpleQueryStringQuery(query);
      builder.analyzer(analyzer);
      builder.defaultOperator(Operator.OR);
      if (fuzzy) {
        builder.fuzzyPrefixLength(0);
        builder.fuzzyMaxExpansions(maxExpansions);
        builder.fuzzyTranspositions(true);
      }
      if (group.getKey()) {
        builder.lenient(true);
      }
      for (SearchFieldConfig field : group.getValue()) {
        builder.field(field.fieldName(), field.boost() * boostMultiplier);
      }
      disMax.add(builder);
    }
  }

  @Nonnull
  private static SearchFieldConfig sharedField(
      @Nonnull final String indexedName,
      @Nonnull final String shortName,
      @Nonnull final String subfield,
      @Nonnull final String analyzer,
      final float boost) {
    return SearchFieldConfig.builder()
        .fieldName(path(indexedName) + "." + subfield)
        // After the field name, which resets them: the Stage 1 query picks its name and analyzed
        // clauses by these
        .shortName(STAGE_1_NAMES.getOrDefault(shortName, shortName))
        .isDelimitedSubfield(true)
        .boost(boost)
        .analyzer(analyzer)
        .isQueryByDefault(true)
        .build();
  }

  /**
   * The shared fields of {@code fields} that the field configuration {@code fieldConfigLabel}
   * selects, or null when none applies: no configuration, an invalid one, or one that selects
   * nothing.
   */
  @Nullable
  private Map<String, List<String>> configuredFields(
      @Nonnull final Map<String, List<String>> fields, @Nullable final String fieldConfigLabel) {
    final SearchFields searchFields = getSearchFields(fieldConfigLabel);
    if (searchFields == null) {
      return null;
    }
    if (!searchFields.isValid()) {
      log.error(
          "Invalid field configuration for label: {}. Replace cannot be used with add/remove.",
          fieldConfigLabel);
      return null;
    }
    if (!searchFields.getReplace().isEmpty()) {
      final Map<String, List<String>> replaced = named(fields, searchFields.getReplace());
      if (replaced.isEmpty()) {
        log.warn(
            "Field configuration replace resulted in no valid fields for label: {}. "
                + "Using base fields instead.",
            fieldConfigLabel);
        return null;
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
      return null;
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
                    || (!OTHER.equals(field.getKey())
                        && field.getValue().stream().anyMatch(baseNames::contains)))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, LinkedHashMap::new));
  }

  @Nonnull
  private static String baseName(@Nonnull final String configuredName) {
    final String name = stripSearchPrefix(configuredName);
    final int subfield = name.indexOf('.');
    return subfield > 0 ? name.substring(0, subfield) : name;
  }

  @Nonnull
  private static String textField(@Nonnull final String field) {
    return path(field) + TEXT_SUFFIX;
  }

  /**
   * Keyword fields the exact boost term-queries. A parsed URN root term-queries the logical field.
   * {@code other} term-queries each contributing {@code __urn_name}. A structured-property text
   * field term-queries {@code __urn_name.lowercase}.
   */
  @Nonnull
  private List<String> exactKeywordFields(
      @Nonnull final SearchFieldResolver v3FieldResolver,
      @Nonnull final SearchFieldConfig searchFieldConfig,
      final boolean canExactMatch,
      @Nullable final AspectRetriever aspectRetriever) {
    final List<String> keywordFields = new ArrayList<>();
    final String fieldName = searchFieldConfig.fieldName();
    if (canExactMatch) {
      keywordFields.add(
          v3FieldResolver.contains(fieldName)
              ? v3FieldResolver.exactPath(fieldName, Intent.EXACT)
              : ESUtils.toV3KeywordField(null, fieldName, aspectRetriever));
    }
    if (v3FieldResolver != null && fieldName.endsWith(TEXT_SUFFIX)) {
      for (String parsed : v3FieldResolver.parsedKeywordPaths(baseName(fieldName))) {
        if (!keywordFields.contains(parsed)) {
          keywordFields.add(parsed);
        }
      }
    }
    if (fieldName.contains(StructuredPropertyUrnIdentity.FIELD_SUFFIX)
        && fieldName.endsWith(TEXT_SUFFIX)) {
      final String lowercase =
          fieldName.substring(0, fieldName.length() - TEXT_SUFFIX.length())
              + "."
              + V3SearchFields.LOWERCASE;
      if (!keywordFields.contains(lowercase)) {
        keywordFields.add(lowercase);
      }
    }
    if (fieldName.contains(V3SearchFields.PARSED_URN_SUFFIX) && fieldName.endsWith(TEXT_SUFFIX)) {
      final String sibling = fieldName.substring(0, fieldName.length() - TEXT_SUFFIX.length());
      if (!keywordFields.contains(sibling)) {
        keywordFields.add(sibling);
      }
    }
    return keywordFields;
  }

  /**
   * Value of an exact-match term. Normalized keywords and {@code .lowercase} store the
   * keyword_normalizer token, so the term has to be folded first. A case-preserving keyword, such
   * as {@code name} or a structured-property {@code __urn_name}, keeps the query text.
   */
  @Nonnull
  private static String exactTermValue(
      @Nonnull final SearchFieldResolver resolver,
      @Nonnull final String field,
      @Nonnull final String query) {
    if (field.endsWith("." + V3SearchFields.LOWERCASE)
        || (!StructuredPropertyUtils.isStructuredPropertyField(field)
            && (field.contains(V3SearchFields.PARSED_URN_SUFFIX)
                || (resolver.contains(field)
                    && resolver.resolve(field, Intent.EXACT).isParsedKeywordLabel())))) {
      return ESUtils.keywordNormalize(query);
    }
    return query;
  }
}
