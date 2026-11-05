package com.linkedin.metadata.search.elasticsearch.query.template;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.EntitySpec;
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
import com.linkedin.search.template.StructuredPropertyClause;
import com.linkedin.search.template.StructuredPropertyQuery;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Structural limits on a template before its body is rendered. */
public final class SearchTemplatePolicyChecks {
  private SearchTemplatePolicyChecks() {}

  public static void check(
      @Nonnull SearchTemplateProperties properties,
      int maxRoutes,
      int maxRoutePatternLength,
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
      checkRanking(properties);
    }
    SearchTemplateComponents.checkWrite(properties, registry);
    if (properties.hasRetrieval() && properties.getRetrieval() != null) {
      checkRetrieval(properties.getRetrieval());
    }
    if (properties.hasLadder() && properties.getLadder() != null) {
      checkLadder(
          properties.getLadder(), properties.hasRetrieval() ? properties.getRetrieval() : null);
    }
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

  public static void checkRanking(@Nonnull SearchTemplateProperties properties) {
    SearchTemplateRanking ranking = properties.getRanking();
    if (ranking == null || SearchTemplateLoader.emptyRanking(ranking)) {
      return;
    }
    if (ranking.hasPreset() && ranking.getPreset() != null && !ranking.getPreset().isBlank()) {
      requireClosed("ranking preset", ranking.getPreset(), SearchTemplateCapabilities.PRESETS);
    }
    if (ranking.hasWindowSizer()
        && ranking.getWindowSizer() != null
        && !ranking.getWindowSizer().isBlank()) {
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
    if (signals
        && ranking.hasPreset()
        && ranking.getPreset() != null
        && !ranking.getPreset().isBlank()) {
      throw new SearchTemplateException("Search template ranking signals require preset unset");
    }
    if (ranking.hasPreset()
        && "none".equals(ranking.getPreset())
        && (signals || reference || patches)) {
      throw new SearchTemplateException(
          "Search template ranking preset none does not take signals");
    }
    if (ranking.hasSignals()
        && ranking.getSignals() != null
        && ranking.getSignals().isEmpty()
        && ranking.hasWindowSizer()
        && "tokenCurve".equals(ranking.getWindowSizer())) {
      throw new SearchTemplateException("Search template ranking tokenCurve requires signals");
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
      requireKeyword(properties);
    }
    if (formula) {
      requireCapability(properties, "rankingFormula");
    }
  }

  private static boolean reference(SearchTemplateRanking ranking) {
    return ranking.hasTemplate()
        && ranking.getTemplate() != null
        && !ranking.getTemplate().isBlank();
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
    return ranking.hasFormula() && ranking.getFormula() != null && !ranking.getFormula().isBlank();
  }

  private static boolean inline(SearchTemplateRanking ranking) {
    boolean preset =
        ranking.hasPreset() && ranking.getPreset() != null && !ranking.getPreset().isBlank();
    boolean window = ranking.hasWindow() && ranking.getWindow() != null;
    boolean sizer =
        ranking.hasWindowSizer()
            && ranking.getWindowSizer() != null
            && !ranking.getWindowSizer().isBlank();
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
    if (properties.hasRescore()
        && properties.getRescore() != null
        && !properties.getRescore().isBlank()) {
      throw new SearchTemplateException(
          "Search template rescore is not combined with ranking signals");
    }
  }

  private static void requireKeyword(SearchTemplateProperties properties) {
    if (properties.hasKnn() && properties.getKnn() != null && !properties.getKnn().isBlank()) {
      throw new SearchTemplateException("Search template ranking requires keyword retrieval");
    }
    if (properties.hasRetrieval()
        && properties.getRetrieval() != null
        && properties.getRetrieval().hasMode()
        && properties.getRetrieval().getMode() != null
        && !properties.getRetrieval().getMode().isBlank()
        && !"keyword".equals(properties.getRetrieval().getMode())) {
      throw new SearchTemplateException("Search template ranking requires keyword retrieval");
    }
  }

  public static void checkRetrieval(@Nonnull SearchTemplateRetrieval retrieval) {
    if (SearchTemplateLoader.emptyRetrieval(retrieval)) {
      return;
    }
    if (retrieval.hasMode() && retrieval.getMode() != null && !retrieval.getMode().isBlank()) {
      requireClosed("retrieval mode", retrieval.getMode(), SearchTemplateCapabilities.MODES);
    }
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
          retrieval != null && retrieval.hasMode() && "vector".equals(retrieval.getMode());
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
