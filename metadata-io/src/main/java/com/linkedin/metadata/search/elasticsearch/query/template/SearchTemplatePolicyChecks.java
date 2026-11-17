package com.linkedin.metadata.search.elasticsearch.query.template;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.SearchFieldResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFieldModel;
import com.linkedin.metadata.search.elasticsearch.query.v3.ranking.RankingFormulaCheck;
import com.linkedin.metadata.search.elasticsearch.query.v3.ranking.RankingSignalRules;
import com.linkedin.search.template.RankingSignal;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateBoost;
import com.linkedin.search.template.SearchTemplateLadder;
import com.linkedin.search.template.SearchTemplateLadderStop;
import com.linkedin.search.template.SearchTemplateNormalization;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalAnchor;
import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyQuery;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Structural limits on a template before its body is rendered. */
public final class SearchTemplatePolicyChecks {
  private SearchTemplatePolicyChecks() {}

  /**
   * {@code merged} is true for the parent chain merged into one template. Rules over sections that
   * a child can inherit separately run only then.
   */
  public static void check(
      boolean merged,
      @Nonnull SearchTemplateProperties properties,
      int maxRoutes,
      int maxRoutePatternLength,
      int maxRankingSignals,
      int maxRankingWindow,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever,
      @Nullable java.util.Collection<EntitySpec> entitySpecs,
      @Nonnull SearchTemplateComponentRegistry registry) {
    SearchTemplateCapabilities.requireDeclared(properties);
    rejectCombined(properties);
    rejectRouterBody(properties);
    if (properties.hasRoutes()) {
      SearchTemplateRoutes.compile(properties.getRoutes(), maxRoutes, maxRoutePatternLength);
    }
    rejectListMembershipSnippet(
        properties.hasFunctionScore() ? properties.getFunctionScore() : null);
    checkFieldReferences(properties, context, retriever);
    checkBoosts(properties, context, retriever);
    if (entitySpecs != null) {
      checkOverrideMembership(
          properties.hasFields() ? properties.getFields() : null, entitySpecs, context, retriever);
    }
    if (properties.hasStructuredPropertyQuery()
        && properties.getStructuredPropertyQuery() != null) {
      checkStructuredPropertyQuery(properties.getStructuredPropertyQuery());
    }
    SearchTemplateScoreDefaults.checkFieldValueScores(
        properties.hasFieldValueScores() ? properties.getFieldValueScores() : null);
    SearchTemplateScoreDefaults.checkFieldFactors(
        properties.hasFieldFactors() ? properties.getFieldFactors() : null);
    SearchTemplateScoreDefaults.checkTermWeights(
        properties.hasTermWeights() ? properties.getTermWeights() : null);
    SearchTemplateScoreDefaults.checkEntityBoosts(
        properties.hasEntityTypeBoosts() ? properties.getEntityTypeBoosts() : null);
    if (properties.hasNormalization() && properties.getNormalization() != null) {
      checkNormalization(properties.getNormalization());
    }
    if (properties.hasRanking() && properties.getRanking() != null) {
      checkRanking(merged, properties, maxRankingSignals, maxRankingWindow, entitySpecs);
    }
    SearchTemplateComponents.checkWrite(properties, registry);
    if (properties.hasRetrieval() && properties.getRetrieval() != null) {
      checkRetrieval(properties.getRetrieval());
    }
    if (properties.hasLadder() && properties.getLadder() != null) {
      checkLadder(
          properties.getLadder(), properties.hasRetrieval() ? properties.getRetrieval() : null);
    }
    if (properties.hasQueryFragments()
        && properties.getQueryFragments() != null
        && properties.getQueryFragments().containsKey(SearchTemplateHoles.EMBEDDING_CLAUSES_JSON)) {
      throw new SearchTemplateException(
          "Search template queryFragments cannot name "
              + SearchTemplateHoles.EMBEDDING_CLAUSES_JSON);
    }
    if (properties.getProfiles() != null) {
      SearchTemplateProfilesPolicy.check(properties.getProfiles());
    }
    if (properties.getScope() != null) {
      SearchTemplateRoutes.names("scope.entityTypes", properties.getScope().getEntityTypes());
    }
    if (merged) {
      checkMergedSections(properties);
    }
  }

  /**
   * Rules over sections a child inherits separately: similarity with {@code knn} or {@code next},
   * the native embedding placeholders with a combine or {@code vectorBody}, and {@code profiles} on
   * a keyword rung that cites no placeholder.
   */
  static void checkMergedSections(@Nonnull SearchTemplateProperties properties) {
    SearchTemplateRetrieval retrieval = properties.getRetrieval();
    String combine = SearchTemplateCapabilities.combine(retrieval);
    boolean similarity =
        retrieval != null
            && SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY.equals(
                retrieval.getQuerySource());
    if (similarity && StringUtils.isNotBlank(properties.getKnn())) {
      throw new SearchTemplateException("Search template similarity cannot set knn");
    }
    if (similarity
        && properties.hasLadder()
        && properties.getLadder() != null
        && properties.getLadder().hasNext()
        && properties.getLadder().getNext() != null) {
      throw new SearchTemplateException("Search template similarity cannot set next");
    }
    boolean placeholders = citesEmbeddingPlaceholders(properties);
    boolean vectorBody =
        retrieval != null
            && retrieval.hasVectorBody()
            && StringUtils.isNotBlank(retrieval.getVectorBody());
    if (placeholders && (combine != null || vectorBody)) {
      throw new SearchTemplateException(
          "Search template embedding placeholders cannot be combined with combine or vectorBody");
    }
    if (properties.hasProfiles()
        && !SearchTemplateLoader.emptyProfiles(properties.getProfiles())
        && combine == null
        && !placeholders) {
      throw new SearchTemplateException("Search template profiles requires a vector leg");
    }
  }

  // Scans the same sections as the bind check in SearchTemplateCapabilities, so a template that
  // bind would reject is rejected when it is written.
  private static boolean citesEmbeddingPlaceholders(@Nonnull SearchTemplateProperties properties) {
    if (Stream.of(
            properties.getQuery(),
            properties.getClauses(),
            properties.getFunctionScore(),
            properties.getRescore(),
            properties.getKnn(),
            properties.getAggregations(),
            properties.getSort())
        .anyMatch(SearchTemplateCapabilities::citesEmbeddingPlaceholders)) {
      return true;
    }
    if (properties.getQueryFragments() != null) {
      for (String fragment : properties.getQueryFragments().values()) {
        if (SearchTemplateCapabilities.citesEmbeddingPlaceholders(fragment)) {
          return true;
        }
      }
    }
    return false;
  }

  public static void checkStructuredPropertyQuery(@Nonnull StructuredPropertyQuery section) {
    if (StructuredPropertyClauses.empty(section)) {
      return;
    }
    checkPropertyClause(
        "analyzedValue", section.hasAnalyzedValue() ? section.getAnalyzedValue() : null, false);
    checkPropertyClause(
        "exactValue", section.hasExactValue() ? section.getExactValue() : null, false);
    checkPropertyClause(
        "nameAndValue", section.hasNameAndValue() ? section.getNameAndValue() : null, true);
  }

  public static void checkPropertyClause(
      @Nonnull String name, @Nullable StructuredPropertyClause clause, boolean weightRequired) {
    if (clause == null) {
      return;
    }
    boolean running = StructuredPropertyClauses.runs(clause);
    if (clause.hasWeight() && clause.getWeight() != null) {
      float weight = clause.getWeight();
      if (!Float.isFinite(weight) || !(weight > 0f)) {
        throw new SearchTemplateException("Search template " + name + " weight must be positive");
      }
    } else if (running && weightRequired) {
      throw new SearchTemplateException("Search template " + name + " weight must be positive");
    }
  }

  public static void checkNormalization(@Nonnull SearchTemplateNormalization section) {
    if (SearchTemplateNormalizationPolicy.empty(section)) {
      return;
    }
    requireRange(
        "wildcardMinimumLength",
        section.hasWildcardMinimumLength() ? section.getWildcardMinimumLength() : null,
        1,
        SearchTemplateNormalizationPolicy.MAX_WILDCARD_MINIMUM_LENGTH);
    requireRange(
        "synonymCap",
        section.hasSynonymCap() ? section.getSynonymCap() : null,
        0,
        SearchTemplateNormalizationPolicy.MAX_SYNONYM_CAP);
    requireRange(
        "fuzzyTokenThreshold",
        section.hasFuzzyTokenThreshold() ? section.getFuzzyTokenThreshold() : null,
        1,
        SearchTemplateNormalizationPolicy.MAX_FUZZY_TOKEN_THRESHOLD);
    requireRange(
        "broadQueryTokenCap",
        section.hasBroadQueryTokenCap() ? section.getBroadQueryTokenCap() : null,
        1,
        SearchTemplateNormalizationPolicy.MAX_BROAD_QUERY_TOKEN_CAP);
    requireRange(
        "shortFuzzyEdits",
        section.hasShortFuzzyEdits() ? section.getShortFuzzyEdits() : null,
        0,
        SearchTemplateNormalizationPolicy.MAX_FUZZY_EDITS);
    requireRange(
        "mediumFuzzyEdits",
        section.hasMediumFuzzyEdits() ? section.getMediumFuzzyEdits() : null,
        0,
        SearchTemplateNormalizationPolicy.MAX_FUZZY_EDITS);
    Integer medium =
        section.hasMediumFuzzyTokenThreshold() ? section.getMediumFuzzyTokenThreshold() : null;
    if (medium != null) {
      int floor =
          section.hasFuzzyTokenThreshold() && section.getFuzzyTokenThreshold() != null
              ? section.getFuzzyTokenThreshold()
              : 1;
      requireRange(
          "mediumFuzzyTokenThreshold",
          medium,
          floor,
          SearchTemplateNormalizationPolicy.MAX_FUZZY_TOKEN_THRESHOLD
              + SearchTemplateNormalizationPolicy.MEDIUM_THRESHOLD_GAP);
    }
  }

  public static void requireRange(@Nonnull String name, @Nullable Integer value, int min, int max) {
    if (value == null) {
      return;
    }
    if (value < min || value > max) {
      throw new SearchTemplateException(
          "Search template " + name + " must be from " + min + " to " + max);
    }
  }

  public static void checkRanking(
      boolean merged,
      @Nonnull SearchTemplateProperties properties,
      int maxRankingSignals,
      int maxRankingWindow,
      @Nullable Collection<EntitySpec> entitySpecs) {
    SearchTemplateRanking ranking = properties.getRanking();
    if (ranking == null || SearchTemplateLoader.emptyRanking(ranking)) {
      return;
    }
    if (StringUtils.isNotBlank(ranking.getPreset())) {
      requireClosed("ranking preset", ranking.getPreset(), SearchTemplateCapabilities.PRESETS);
    }
    if (StringUtils.isNotBlank(ranking.getWindowSizer())) {
      requireClosed(
          "ranking windowSizer", ranking.getWindowSizer(), SearchTemplateCapabilities.SIZERS);
    }
    boolean reference = reference(ranking);
    boolean signals = signals(ranking);
    boolean patches = patches(ranking);
    boolean formula = formula(ranking);
    // add and remove patch a reference. They are not inline ranking fields.
    if (reference && inline(ranking)) {
      throw new SearchTemplateException("Search template ranking is a reference or it is inline");
    }
    if (ranking.hasSignals() && patches) {
      throw new SearchTemplateException(
          "Search template ranking signals cannot be combined with add or remove");
    }
    if (signals && StringUtils.isNotBlank(ranking.getPreset())) {
      throw new SearchTemplateException("Search template ranking signals require preset unset");
    }
    if (SearchTemplateCapabilities.Preset.NONE.is(ranking.getPreset())
        && (signals || reference || patches)) {
      throw new SearchTemplateException(
          "Search template ranking preset none does not take signals");
    }
    if (ranking.hasSignals()
        && ranking.getSignals() != null
        && ranking.getSignals().isEmpty()
        && SearchTemplateCapabilities.WindowSizer.TOKEN_CURVE.is(ranking.getWindowSizer())) {
      throw new SearchTemplateException("Search template ranking tokenCurve requires signals");
    }
    RankingSignalRules.checkWindow(
        ranking.hasWindow() ? ranking.getWindow() : null, maxRankingWindow);
    SearchFieldResolver resolver =
        entitySpecs == null ? null : V3SearchFieldModel.of(entitySpecs).resolver();
    if (signals) {
      RankingSignalRules.checkSignals(
          ranking.getSignals(), resolver, entitySpecs, maxRankingSignals);
    }
    if (ranking.hasAdd() && ranking.getAdd() != null && !ranking.getAdd().isEmpty()) {
      RankingSignalRules.checkSignals(ranking.getAdd(), resolver, entitySpecs, maxRankingSignals);
    }
    if (ranking.hasRemove() && ranking.getRemove() != null) {
      for (String name : ranking.getRemove()) {
        if (name == null || name.isBlank()) {
          throw new SearchTemplateException("Search template ranking remove name is not valid");
        }
      }
    }
    if (reference || signals) {
      requireCapability(properties, "rankingSignals");
      rejectEngineRescore(properties);
      requireKeyword(merged, properties);
    }
    if (formula) {
      requireCapability(properties, "rankingFormula");
      Set<String> names = new LinkedHashSet<>();
      if (signals) {
        for (RankingSignal signal : ranking.getSignals()) {
          names.add(signal.getName());
        }
      }
      RankingFormulaCheck.check(ranking.getFormula(), names);
    }
  }

  /** Ranking signals or a ranking reference need keyword retrieval on the merged template. */
  static void requireKeywordForRanking(@Nonnull SearchTemplateProperties properties) {
    SearchTemplateRanking ranking = properties.getRanking();
    if (ranking == null || SearchTemplateLoader.emptyRanking(ranking)) {
      return;
    }
    if (reference(ranking) || signals(ranking)) {
      requireKeyword(true, properties);
    }
  }

  private static boolean reference(SearchTemplateRanking ranking) {
    return StringUtils.isNotBlank(ranking.getTemplate());
  }

  private static boolean signals(SearchTemplateRanking ranking) {
    return ranking.hasSignals() && ranking.getSignals() != null && !ranking.getSignals().isEmpty();
  }

  private static boolean patches(SearchTemplateRanking ranking) {
    boolean add = ranking.hasAdd() && ranking.getAdd() != null && !ranking.getAdd().isEmpty();
    boolean remove =
        ranking.hasRemove() && ranking.getRemove() != null && !ranking.getRemove().isEmpty();
    return add || remove;
  }

  private static boolean formula(SearchTemplateRanking ranking) {
    return StringUtils.isNotBlank(ranking.getFormula());
  }

  private static boolean inline(SearchTemplateRanking ranking) {
    boolean preset = StringUtils.isNotBlank(ranking.getPreset());
    boolean window = ranking.hasWindow() && ranking.getWindow() != null;
    boolean sizer = StringUtils.isNotBlank(ranking.getWindowSizer());
    boolean parameters =
        ranking.hasParameters()
            && ranking.getParameters() != null
            && !ranking.getParameters().isEmpty();
    return preset || window || sizer || signals(ranking) || formula(ranking) || parameters;
  }

  private static void requireCapability(SearchTemplateProperties properties, String name) {
    if (!properties.hasCapabilities()
        || properties.getCapabilities() == null
        || !properties.getCapabilities().contains(name)) {
      throw new SearchTemplateException("Search template capability is required: " + name);
    }
  }

  private static void rejectEngineRescore(SearchTemplateProperties properties) {
    if (StringUtils.isNotBlank(properties.getRescore())) {
      throw new SearchTemplateException(
          "Search template rescore is not combined with ranking signals");
    }
  }

  /**
   * Ranking runs on keyword retrieval. keywordRerank is keyword retrieval, so its {@code knn} blend
   * may sit beside ranking. One aspect that sets no retrieval may inherit that combine, so the
   * {@code knn} rule waits for the merged check.
   */
  private static void requireKeyword(boolean merged, SearchTemplateProperties properties) {
    SearchTemplateRetrieval retrieval = properties.getRetrieval();
    String combine = SearchTemplateCapabilities.combine(retrieval);
    boolean keywordRerank = SearchTemplateCapabilities.COMBINE_KEYWORD_RERANK.equals(combine);
    if (StringUtils.isNotBlank(properties.getKnn())
        && !keywordRerank
        && (merged || retrieval != null)) {
      throw new SearchTemplateException("Search template ranking requires keyword retrieval");
    }
    if ((combine != null && !keywordRerank)
        || (retrieval != null
            && StringUtils.isNotBlank(retrieval.getMode())
            && !"keyword".equals(retrieval.getMode()))) {
      throw new SearchTemplateException("Search template ranking requires keyword retrieval");
    }
  }

  /**
   * Rules inside one retrieval section, which a child replaces as a whole. Each rejects a template
   * that is invalid in every build. A later combine, a second embedding profile, and {@code
   * vectorBody} stay valid to store and fail when selected by a build that does not run them.
   */
  public static void checkRetrieval(@Nonnull SearchTemplateRetrieval retrieval) {
    if (SearchTemplateLoader.emptyRetrieval(retrieval)) {
      return;
    }
    String mode = text(retrieval.getMode());
    if (mode != null) {
      requireClosed("retrieval mode", mode, SearchTemplateCapabilities.MODES);
    }
    String combineField = text(retrieval.getCombine());
    if (combineField != null) {
      requireClosed("retrieval combine", combineField, SearchTemplateCapabilities.COMBINES);
      boolean vectorPair =
          "vector".equals(mode) && SearchTemplateCapabilities.COMBINE_VECTOR.equals(combineField);
      boolean keywordPair =
          "keyword".equals(mode)
              && SearchTemplateCapabilities.COMBINE_KEYWORD_RERANK.equals(combineField);
      if (mode != null && !vectorPair && !keywordPair) {
        throw new SearchTemplateException(
            "Search template retrieval mode " + mode + " conflicts with combine " + combineField);
      }
    }
    String combine = SearchTemplateCapabilities.combine(retrieval);
    boolean keywordRung = combine == null;
    boolean vector = SearchTemplateCapabilities.COMBINE_VECTOR.equals(combine);
    if (retrieval.getCandidateCap() != null && retrieval.getCandidateCap() < 1) {
      throw new SearchTemplateException(
          "Search template retrieval candidateCap must be at least 1");
    }
    String onFailure = text(retrieval.getOnFailure());
    if (onFailure != null) {
      requireClosed("retrieval onFailure", onFailure, SearchTemplateCapabilities.ON_FAILURES);
    }
    String querySource = text(retrieval.getQuerySource());
    if (querySource != null) {
      requireClosed("retrieval querySource", querySource, SearchTemplateCapabilities.QUERY_SOURCES);
      if (keywordRung) {
        throw new SearchTemplateException(
            "Search template retrieval querySource requires a vector leg");
      }
      if (SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY.equals(querySource) && !vector) {
        throw new SearchTemplateException(
            "Search template retrieval querySource anchorEntity requires combine vector");
      }
    }
    if (retrieval.getAnchor() != null) {
      SearchTemplateRetrievalAnchor anchor = retrieval.getAnchor();
      if (!SearchTemplateCapabilities.QUERY_SOURCE_ANCHOR_ENTITY.equals(querySource)) {
        throw new SearchTemplateException(
            "Search template retrieval anchor requires querySource anchorEntity");
      }
      String urnParameter = text(anchor.getUrnParameter());
      if (urnParameter != null) {
        requireClosed(
            "retrieval anchor urnParameter",
            urnParameter,
            SearchTemplateCapabilities.ANCHOR_URN_PARAMETERS);
      }
    }
    String profileScore = text(retrieval.getProfileScore());
    if (profileScore != null) {
      requireClosed(
          "retrieval profileScore", profileScore, SearchTemplateCapabilities.PROFILE_SCORES);
      if (keywordRung) {
        throw new SearchTemplateException(
            "Search template retrieval profileScore requires a vector leg");
      }
    }
    boolean vectorBody = text(retrieval.getVectorBody()) != null;
    if (vectorBody && keywordRung) {
      throw new SearchTemplateException(
          "Search template retrieval vectorBody requires a vector leg");
    }
    if (vectorBody && profileScore != null) {
      throw new SearchTemplateException(
          "Search template retrieval vectorBody cannot be combined with profileScore");
    }
    if (retrieval.getMinSimilarity() != null) {
      float floor = retrieval.getMinSimilarity();
      if (!Float.isFinite(floor) || floor < -1f || floor > 1f) {
        throw new SearchTemplateException(
            "Search template retrieval minSimilarity must be from -1 to 1");
      }
      if (keywordRung || SearchTemplateCapabilities.COMBINE_KEYWORD_RERANK.equals(combine)) {
        throw new SearchTemplateException(
            "Search template retrieval minSimilarity requires a vector-admitting combine");
      }
      if (SearchTemplateCapabilities.PROFILE_SCORE_WEIGHTED_SUM.equals(profileScore)) {
        throw new SearchTemplateException(
            "Search template retrieval minSimilarity cannot be combined with profileScore"
                + " weightedSum");
      }
    }
  }

  @Nullable
  private static String text(@Nullable String value) {
    return value == null || value.isBlank() ? null : value;
  }

  public static void requireClosed(
      @Nonnull String field, @Nonnull String value, @Nonnull Set<String> allowed) {
    if (!allowed.contains(value)) {
      throw new SearchTemplateException("Search template " + field + " is not valid: " + value);
    }
  }

  public static void checkLadder(
      @Nonnull SearchTemplateLadder ladder, @Nullable SearchTemplateRetrieval retrieval) {
    if (SearchTemplateLoader.emptyLadder(ladder)) {
      return;
    }
    boolean parallel =
        ladder.hasParallel() && ladder.getParallel() != null && !ladder.getParallel().isEmpty();
    boolean stop = hasStop(ladder);
    boolean next = ladder.hasNext() && ladder.getNext() != null;
    if (next && !parallel && !stop) {
      throw new SearchTemplateException("Search template next requires a stop condition");
    }
    if (parallel && (!ladder.hasJoin() || ladder.getJoin() == null || ladder.getJoin().isBlank())) {
      throw new SearchTemplateException("Search template parallel requires a join");
    }
    SearchTemplateLadderJoins join = null;
    if (ladder.hasJoin() && ladder.getJoin() != null && !ladder.getJoin().isBlank()) {
      join = SearchTemplateLadderJoins.parse(ladder.getJoin());
    }
    if (join == SearchTemplateLadderJoins.MERGE && next && !stop) {
      throw new SearchTemplateException("Search template merge next requires a stop condition");
    }
    if (ladder.hasFusion() && ladder.getFusion() != null && !ladder.getFusion().isBlank()) {
      requireClosed("fusion", ladder.getFusion(), SearchTemplateCapabilities.FUSIONS);
    }
    if (ladder.hasFacets() && ladder.getFacets() != null && !ladder.getFacets().isBlank()) {
      requireClosed("facets", ladder.getFacets(), SearchTemplateCapabilities.FACETS);
      boolean vector =
          SearchTemplateCapabilities.COMBINE_VECTOR.equals(
              SearchTemplateCapabilities.combine(retrieval));
      if ("candidates".equals(ladder.getFacets()) && !vector) {
        throw new SearchTemplateException(
            "Search template facets candidates requires vector retrieval");
      }
    }
    if (ladder.hasSelect() && ladder.getSelect() != null) {
      for (String name : ladder.getSelect()) {
        if (SearchTemplateLadderPredicates.base(name) == null) {
          throw new SearchTemplateException(
              "Search template select predicate is not valid: " + name);
        }
      }
    }
    if (ladder.hasStop() && ladder.getStop() != null) {
      for (SearchTemplateLadderStop condition : ladder.getStop()) {
        boolean minHits = condition.hasMinHits() && condition.getMinHits() != null;
        boolean emptyWhen =
            condition.hasEmptyWhen()
                && condition.getEmptyWhen() != null
                && !condition.getEmptyWhen().isBlank();
        if (!minHits && !emptyWhen) {
          throw new SearchTemplateException("Search template stop condition is empty");
        }
        if (emptyWhen && SearchTemplateLadderPredicates.base(condition.getEmptyWhen()) == null) {
          throw new SearchTemplateException(
              "Search template emptyWhen predicate is not valid: " + condition.getEmptyWhen());
        }
      }
    }
  }

  public static boolean hasStop(@Nonnull SearchTemplateLadder ladder) {
    if (!ladder.hasStop() || ladder.getStop() == null || ladder.getStop().isEmpty()) {
      return false;
    }
    for (SearchTemplateLadderStop condition : ladder.getStop()) {
      if (condition.hasMinHits() && condition.getMinHits() != null) {
        return true;
      }
      if (condition.hasEmptyWhen()
          && condition.getEmptyWhen() != null
          && !condition.getEmptyWhen().isBlank()) {
        return true;
      }
    }
    return false;
  }

  public static void rejectRouterBody(@Nonnull SearchTemplateProperties properties) {
    if (!hasDefaultTarget(properties)) {
      return;
    }
    if (present(properties.hasQuery() ? properties.getQuery() : null)
        || presentFragment(properties)
        || present(properties.hasClauses() ? properties.getClauses() : null)
        || present(properties.hasFunctionScore() ? properties.getFunctionScore() : null)
        || present(properties.hasRescore() ? properties.getRescore() : null)
        || present(properties.hasKnn() ? properties.getKnn() : null)
        || (properties.hasRanking() && !SearchTemplateLoader.emptyRanking(properties.getRanking()))
        || (properties.hasRetrieval()
            && !SearchTemplateLoader.emptyRetrieval(properties.getRetrieval()))
        || (properties.hasLadder() && !SearchTemplateLoader.emptyLadder(properties.getLadder()))
        || (properties.hasProfiles()
            && !SearchTemplateLoader.emptyProfiles(properties.getProfiles()))
        || (properties.hasScope() && !SearchTemplateLoader.emptyScope(properties.getScope()))
        || present(properties.hasAggregations() ? properties.getAggregations() : null)
        || present(properties.hasSort() ? properties.getSort() : null)
        || (properties.hasFields() && !SearchTemplateLoader.emptyFields(properties.getFields()))
        || (properties.hasHighlightFields()
            && !SearchTemplateLoader.emptyHighlights(properties.getHighlightFields()))
        || (properties.hasStructuredPropertyQuery()
            && !SearchTemplateLoader.emptyStructuredPropertyQuery(
                properties.getStructuredPropertyQuery()))
        || (properties.hasFieldValueScores()
            && properties.getFieldValueScores() != null
            && !properties.getFieldValueScores().isEmpty())
        || (properties.hasFieldFactors()
            && properties.getFieldFactors() != null
            && !properties.getFieldFactors().isEmpty())
        || (properties.hasTermWeights()
            && properties.getTermWeights() != null
            && !properties.getTermWeights().isEmpty())
        || properties.hasScoreMode()
        || properties.hasBoostMode()
        || (properties.hasEntityTypeBoosts()
            && properties.getEntityTypeBoosts() != null
            && !properties.getEntityTypeBoosts().isEmpty())
        || (properties.hasComponent() && !SearchTemplateComponents.empty(properties.getComponent()))
        || (properties.hasBoosts()
            && properties.getBoosts() != null
            && !properties.getBoosts().isEmpty())
        || (properties.hasParameters()
            && properties.getParameters() != null
            && !properties.getParameters().isEmpty())) {
      throw new SearchTemplateException(
          "Search template default route cannot be combined with a body");
    }
  }

  public static boolean hasDefaultTarget(@Nonnull SearchTemplateProperties properties) {
    return properties.hasDefaultRoute()
        && properties.getDefaultRoute() != null
        && properties.getDefaultRoute().hasTemplate()
        && properties.getDefaultRoute().getTemplate() != null;
  }

  public static boolean presentFragment(@Nonnull SearchTemplateProperties properties) {
    if (!properties.hasQueryFragments() || properties.getQueryFragments() == null) {
      return false;
    }
    for (String value : properties.getQueryFragments().values()) {
      if (value != null && !value.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static boolean present(@Nullable String value) {
    return value != null && !value.isBlank();
  }

  public static void rejectCombined(@Nonnull SearchTemplateProperties properties) {
    if (properties.hasFields()) {
      SearchFieldOverrides fields = properties.getFields();
      boolean replace = fields.hasReplace() && !fields.getReplace().isEmpty();
      boolean add = fields.hasAdd() && !fields.getAdd().isEmpty();
      boolean remove = fields.hasRemove() && !fields.getRemove().isEmpty();
      boolean override = fields.hasOverride() && !fields.getOverride().isEmpty();
      if (replace && (add || remove || override)) {
        throw new SearchTemplateException(
            "Search template fields.replace cannot be combined with add, remove, or override");
      }
    }
    if (properties.hasHighlightFields()) {
      var highlights = properties.getHighlightFields();
      boolean replace = highlights.hasReplace() && !highlights.getReplace().isEmpty();
      boolean add = highlights.hasAdd() && !highlights.getAdd().isEmpty();
      boolean remove = highlights.hasRemove() && !highlights.getRemove().isEmpty();
      if (replace && (add || remove)) {
        throw new SearchTemplateException(
            "Search template highlightFields.replace cannot be combined with add or remove");
      }
    }
  }

  public static void rejectListMembershipSnippet(@Nullable String functionScore) {
    if (functionScore == null) {
      return;
    }
    for (String injectable : SearchTemplateViewer.INJECTABLES) {
      if (functionScore.contains(injectable)) {
        throw new SearchTemplateException(
            "Search template functionScore cannot reference " + injectable);
      }
    }
  }

  public static void checkFieldReferences(
      @Nonnull SearchTemplateProperties properties,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (!properties.hasFields() || properties.getFields() == null) {
      return;
    }
    SearchFieldOverrides fields = properties.getFields();
    StructuredPropertyReferences.Resolver resolver =
        StructuredPropertyReferences.resolver(context, retriever);
    resolveBoosts(fields.hasAdd() ? fields.getAdd() : null, resolver, context, retriever);
    resolveBoosts(fields.hasOverride() ? fields.getOverride() : null, resolver, context, retriever);
    resolveBoosts(fields.hasReplace() ? fields.getReplace() : null, resolver, context, retriever);
    if (fields.hasRemove() && fields.getRemove() != null) {
      for (String name : fields.getRemove()) {
        resolver.resolveRemove(name);
      }
    }
  }

  public static void resolveBoosts(
      @Nullable SearchFieldBoostArray boosts,
      @Nonnull StructuredPropertyReferences.Resolver resolver,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (boosts == null) {
      return;
    }
    for (SearchFieldBoost boost : boosts) {
      String subfield = boost.hasSubfield() ? boost.getSubfield() : null;
      if (subfield != null
          && !subfield.isBlank()
          && !StructuredPropertyReferences.KEYWORD.equals(subfield)) {
        throw new SearchTemplateException("Search template subfield is not valid: " + subfield);
      }
      resolver.resolve(boost.getFieldName(), subfield);
    }
  }

  public static void checkBoosts(
      @Nonnull SearchTemplateProperties properties,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (!properties.hasBoosts() || properties.getBoosts() == null) {
      return;
    }
    StructuredPropertyReferences.Resolver resolver =
        StructuredPropertyReferences.resolver(context, retriever);
    for (SearchTemplateBoost boost : properties.getBoosts()) {
      if (!SearchTemplateViewer.INJECTABLES.contains(boost.getValues())) {
        throw new SearchTemplateException(
            "Search template boost values are not valid: " + boost.getValues());
      }
      resolver.resolve(boost.getField(), boost.hasSubfield() ? boost.getSubfield() : null);
    }
  }

  public static void checkOverrideMembership(
      @Nullable SearchFieldOverrides fields,
      @Nonnull Collection<EntitySpec> entitySpecs,
      @Nullable OperationFingerprint context,
      @Nullable AspectRetriever retriever) {
    if (fields == null || !fields.hasOverride() || fields.getOverride().isEmpty()) {
      return;
    }
    if (fields.hasReplace() && !fields.getReplace().isEmpty()) {
      return;
    }
    StructuredPropertyReferences.Resolver resolver =
        StructuredPropertyReferences.resolver(context, retriever);
    Set<String> queried = new LinkedHashSet<>();
    for (var field : SearchTemplateFields.pdlQueryFields(entitySpecs)) {
      queried.add(field.fieldName());
    }
    if (fields.hasRemove() && fields.getRemove() != null) {
      for (String name : fields.getRemove()) {
        queried.remove(resolver.resolveRemove(name));
      }
    }
    if (fields.hasAdd() && fields.getAdd() != null) {
      for (SearchFieldBoost boost : fields.getAdd()) {
        queried.add(
            resolver.resolve(
                boost.getFieldName(), boost.hasSubfield() ? boost.getSubfield() : null));
      }
    }
    for (SearchFieldBoost boost : fields.getOverride()) {
      String name =
          resolver.resolve(boost.getFieldName(), boost.hasSubfield() ? boost.getSubfield() : null);
      if (!queried.contains(name)) {
        throw new SearchTemplateException(
            "Search template fields.override is not in the queried set: " + boost.getFieldName());
      }
    }
  }
}
