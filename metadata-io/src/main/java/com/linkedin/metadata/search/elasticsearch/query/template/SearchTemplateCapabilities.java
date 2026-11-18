package com.linkedin.metadata.search.elasticsearch.query.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.search.template.SearchTemplateComponentMode;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalBridge;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Sections this process can run. Keyword retrieval and engine rescore are supported. A stored
 * section outside that set fails the search after parent merge. {@link #API_VERSION} is the
 * template and Mustache-context contract. {@link #SUPPORTED} is the capability names a template may
 * declare.
 */
public final class SearchTemplateCapabilities {
  /** Template and Mustache-context contract stored when a write omits it. */
  public static final int API_VERSION = 1;

  /**
   * Contract that adds the semantic fields: retrieval {@code combine}, {@code querySource}, {@code
   * anchor}, {@code profileScore}, {@code minSimilarity}, and {@code vectorBody}, plus {@code
   * profiles}, {@code scope}, and route {@code match}. A template that sets one must resolve to
   * this version, so a release that does not read those fields rejects the template instead of
   * running it without them.
   */
  public static final int SEMANTIC_API_VERSION = 2;

  /** Contracts this process implements. */
  public static final Set<Integer> API_VERSIONS = Set.of(API_VERSION, SEMANTIC_API_VERSION);

  /**
   * Capability names a template may declare. A name outside this set is rejected at write and at
   * load. Declaring a name does not turn on a section the template did not store.
   */
  public static final Set<String> SUPPORTED =
      Set.copyOf(
          List.of(
              "keyword",
              "rescore",
              "functionScore",
              "routes",
              "ladder",
              "aggregations",
              "structuredPropertyQuery",
              "fieldValueScores",
              "fieldFactors",
              "termWeights",
              "entityTypeBoosts",
              "normalization",
              "fieldOverrides",
              "highlights",
              "sort",
              "boosts",
              "queryFragments",
              "component",
              "rankingSignals",
              "rankingFormula"));

  /** Stored ranking preset. The wire value is what a template writes. */
  public enum Preset {
    NONE("none"),
    USAGE("usage"),
    USAGE_AND_MATCH_TIER("usageAndMatchTier");

    private final String wire;

    Preset(String wire) {
      this.wire = wire;
    }

    @Nonnull
    public String wire() {
      return wire;
    }

    public boolean is(@Nullable String value) {
      return wire.equals(value);
    }
  }

  /** How a ranking window is chosen. The wire value is what a template writes. */
  public enum WindowSizer {
    FIXED("fixed"),
    TOKEN_CURVE("tokenCurve");

    private final String wire;

    WindowSizer(String wire) {
      this.wire = wire;
    }

    @Nonnull
    public String wire() {
      return wire;
    }

    public boolean is(@Nullable String value) {
      return wire.equals(value);
    }
  }

  public static final Set<String> PRESETS =
      Set.of(Preset.NONE.wire(), Preset.USAGE.wire(), Preset.USAGE_AND_MATCH_TIER.wire());
  public static final Set<String> SIZERS =
      Set.of(WindowSizer.FIXED.wire(), WindowSizer.TOKEN_CURVE.wire());
  public static final Set<String> FUSIONS =
      Set.of("interleave", "weightedBlend", "fillFreeSlots", "keywordPreferred");
  public static final Set<String> FACETS = Set.of("firstMember", "patchFromMembers", "candidates");
  public static final Set<String> MODES = Set.of("keyword", "vector");

  public static final String COMBINE_VECTOR = "vector";
  public static final String COMBINE_KEYWORD_RERANK = "keywordRerank";

  /** Closed {@code retrieval.combine} values. Unset is keyword. */
  public static final Set<String> COMBINES =
      Set.of(COMBINE_VECTOR, COMBINE_KEYWORD_RERANK, "vectorThenKeyword", "vectorFilter");

  public static final String QUERY_SOURCE_ANCHOR_ENTITY = "anchorEntity";
  public static final Set<String> QUERY_SOURCES = Set.of("text", QUERY_SOURCE_ANCHOR_ENTITY);
  public static final String PROFILE_SCORE_WEIGHTED_SUM = "weightedSum";
  public static final Set<String> PROFILE_SCORES = Set.of("best", PROFILE_SCORE_WEIGHTED_SUM);
  public static final Set<String> ANCHOR_URN_PARAMETERS = Set.of("raw");

  /**
   * {@code embeddingClausesJson} and {@code embeddingField} under any Mustache tag: variable,
   * unescaped, triple braces, section, inverted section, or section end.
   */
  private static final Pattern EMBEDDING_PLACEHOLDER =
      Pattern.compile("\\{\\{\\{?\\s*[&#^/]?\\s*(?:embeddingClausesJson|embeddingField)\\s*}?}}");

  private SearchTemplateCapabilities() {}

  /**
   * Stores contract version 1 when the caller omits {@code apiVersion}. An explicit value is left
   * unchanged so a later check can reject it.
   */
  public static void stampApiVersion(@Nonnull SearchTemplateProperties properties) {
    if (!properties.hasApiVersion() || properties.getApiVersion() == null) {
      properties.setApiVersion(API_VERSION);
    }
  }

  public static void requireDeclared(@Nonnull SearchTemplateProperties properties) {
    Integer apiVersion = properties.hasApiVersion() ? properties.getApiVersion() : null;
    Collection<String> capabilities =
        properties.hasCapabilities() ? properties.getCapabilities() : null;
    requireDeclared(apiVersion, capabilities);
  }

  public static void requireContract(@Nonnull MergedSearchTemplate template) {
    requireDeclared(template.getApiVersion(), template.getCapabilities());
  }

  public static void requireDeclared(
      @Nullable Integer apiVersion, @Nullable Collection<String> capabilities) {
    if (apiVersion != null && !API_VERSIONS.contains(apiVersion)) {
      throw new SearchTemplateException(
          "Search template apiVersion "
              + apiVersion
              + " is not supported. This release implements "
              + API_VERSION
              + " and "
              + SEMANTIC_API_VERSION);
    }
    if (capabilities == null) {
      return;
    }
    for (String capability : capabilities) {
      if (capability == null || capability.isBlank() || !SUPPORTED.contains(capability)) {
        throw new SearchTemplateException(
            "Search template capability is not supported: " + capability);
      }
    }
  }

  /**
   * Scroll drops {@code knn}, so a knn section does not fail a scroll. On a search, document rerank
   * is advertised only when the request includes the Document entity.
   */
  public static void requireSupported(
      @Nonnull MergedSearchTemplate template,
      boolean documentRerank,
      boolean scroll,
      @Nonnull SearchTemplateComponentRegistry registry,
      @Nonnull SearchTemplateGates gates) {
    requireContract(template);
    String code = rejectionCode(template, documentRerank, scroll, registry, gates);
    if (code != null) {
      throw unsupported(rejectionDetail(template, code));
    }
    com.linkedin.search.template.SearchTemplateComponent component = template.getComponent();
    if (!SearchTemplateComponents.empty(component)) {
      registry
          .require(component.getName(), component.getVersion(), component.getMode())
          .validate(component.getMode(), SearchTemplateComponents.config(component));
    }
  }

  /**
   * Fails a template whose resolved embedding profiles this build cannot run for the requested
   * entity types. Checked after {@link #requireSupported}, when the request is known.
   */
  public static void requireResolvable(
      @Nonnull MergedSearchTemplate template,
      @Nonnull SearchTemplateGates gates,
      @Nullable Collection<String> entityTypes,
      @Nullable String system,
      boolean documentRerank) {
    String code = gates.bindingRejection(template, entityTypes, system, documentRerank);
    if (code != null) {
      throw unsupported(rejectionDetail(template, code));
    }
  }

  /**
   * Bounded reason a merged template cannot run, or null when its sections are runnable. {@code
   * rankingPreset}, {@code rankingWindow}, {@code knn}, {@code mergeJoin}, {@code retrievalMode},
   * {@code retrievalBridge}, or a code from {@link SearchTemplateGates#rejectionCode}.
   */
  @Nullable
  public static String rejectionCode(
      @Nonnull MergedSearchTemplate template,
      boolean documentRerank,
      boolean scroll,
      @Nonnull SearchTemplateComponentRegistry registry,
      @Nonnull SearchTemplateGates gates) {
    com.linkedin.search.template.SearchTemplateComponent component = template.getComponent();
    if (!SearchTemplateComponents.empty(component)) {
      String name = component.hasName() ? component.getName() : null;
      Integer version = component.hasVersion() ? component.getVersion() : null;
      if (name == null
          || name.isBlank()
          || version == null
          || !component.hasMode()
          || component.getMode() == null
          || registry.find(name, version).isEmpty()) {
        return "component";
      }
      if (component.getMode() == SearchTemplateComponentMode.OWN_RUNG && scroll) {
        return "componentScroll";
      }
    }
    SearchTemplateRanking ranking = template.getRanking();
    if (ranking != null) {
      boolean signals =
          ranking.hasSignals() && ranking.getSignals() != null && !ranking.getSignals().isEmpty();
      String preset = ranking.hasPreset() ? ranking.getPreset() : null;
      if (!signals && StringUtils.isNotBlank(preset) && !Preset.NONE.is(preset)) {
        return "rankingPreset";
      }
      String sizer = ranking.hasWindowSizer() ? ranking.getWindowSizer() : null;
      if (!signals && StringUtils.isNotBlank(sizer) && !WindowSizer.FIXED.is(sizer)) {
        return "rankingWindow";
      }
    }
    // keywordRerank serves keyword order when the rerank cannot run, so it is not a capability
    // failure. Only the legacy shape, a knn with no combine that is not read as keywordRerank, runs
    // its knn through document rerank. With a combine, knn carries the blend or the deprecated
    // profile names, and the gates decide.
    if (template.getKnn() != null
        && !scroll
        && !documentRerank
        && effectiveCombine(template) == null) {
      return "knn";
    }
    if (template.getLadder() != null
        && template.getLadder().hasJoin()
        && template.getLadder().getJoin() != null
        && "merge".equalsIgnoreCase(template.getLadder().getJoin())) {
      return "mergeJoin";
    }
    String gate = gates.rejectionCode(template, documentRerank);
    if (gate != null) {
      return gate;
    }
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval == null) {
      return null;
    }
    // A legacy vector mode is the vector combine, which the gates decide.
    String mode = retrieval.hasMode() ? retrieval.getMode() : null;
    if (mode != null
        && !mode.isBlank()
        && !"keyword".equals(mode)
        && !COMBINE_VECTOR.equals(mode)) {
      return "retrievalMode";
    }
    SearchTemplateRetrievalBridge bridge = retrieval.hasBridge() ? retrieval.getBridge() : null;
    if (bridge != null && !SearchTemplateLoader.emptyRetrieval(retrievalWithBridge(bridge))) {
      return "retrievalBridge";
    }
    return null;
  }

  @Nonnull
  private static String rejectionDetail(
      @Nonnull MergedSearchTemplate template, @Nonnull String code) {
    SearchTemplateRanking ranking = template.getRanking();
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    return switch (code) {
      case "component" -> "component";
      case "componentScroll" -> "component scroll";
      case "rankingPreset" -> "ranking preset " + ranking.getPreset();
      case "rankingWindow" -> "ranking windowSizer " + ranking.getWindowSizer();
      case "knn" -> "knn";
      case "mergeJoin" -> "merge join";
      case "retrievalMode" -> "retrieval mode " + retrieval.getMode();
      case SearchTemplateGates.KEYWORD_PROFILES -> "profiles on a keyword rung";
      case SearchTemplateGates.PROFILE_SCOPE -> "profiles that do not cover the scope";
      case SearchTemplateGates.MIN_SIMILARITY -> "minSimilarity on a profile that is not cosine";
      case "retrievalBridge" -> "retrieval bridge";
      default ->
          SearchTemplateGates.CAPABILITIES.contains(code)
              ? "capability " + code + ", which this build does not run"
              : code;
    };
  }

  /**
   * The effective combine: {@code retrieval.combine}, else {@code vector} when {@code mode} is
   * vector, else null for keyword.
   */
  @Nullable
  public static String combine(@Nullable SearchTemplateRetrieval retrieval) {
    if (retrieval == null) {
      return null;
    }
    if (retrieval.hasCombine() && StringUtils.isNotBlank(retrieval.getCombine())) {
      return retrieval.getCombine();
    }
    if (retrieval.hasMode() && COMBINE_VECTOR.equals(retrieval.getMode())) {
      return COMBINE_VECTOR;
    }
    return null;
  }

  /**
   * The combine a merged template runs: {@link #combine(SearchTemplateRetrieval)}, else
   * keywordRerank for a knn section that names both {@code model} and {@code field}, the earlier
   * keywordRerank shape. Those names stay opaque, so the rerank resolves the entity default. A knn
   * with only a field stays the legacy document rerank.
   */
  @Nullable
  public static String effectiveCombine(@Nonnull MergedSearchTemplate template) {
    String combine = combine(template.getRetrieval());
    if (combine != null) {
      return combine;
    }
    JsonNode knn = SearchTemplateGates.readKnn(template.getKnn());
    return knn != null && namesText(knn, "model") && namesText(knn, "field")
        ? COMBINE_KEYWORD_RERANK
        : null;
  }

  private static boolean namesText(@Nonnull JsonNode node, @Nonnull String key) {
    return node.path(key).isTextual() && StringUtils.isNotBlank(node.path(key).asText());
  }

  /**
   * True when a body cites the native embedding placeholders, which only the later native mixed
   * query renders.
   */
  public static boolean citesEmbeddingPlaceholders(@Nullable String body) {
    return body != null && EMBEDDING_PLACEHOLDER.matcher(body).find();
  }

  // Every body section is scanned: a citation outside the native query sections never renders,
  // so it must fail closed rather than serve an empty value. The write check in
  // SearchTemplatePolicyChecks scans the same sections.
  static boolean citesEmbeddingPlaceholders(@Nonnull MergedSearchTemplate template) {
    if (citesEmbeddingPlaceholders(template.getQuery())
        || citesEmbeddingPlaceholders(template.getClauses())
        || citesEmbeddingPlaceholders(template.getFunctionScore())
        || citesEmbeddingPlaceholders(template.getRescore())
        || citesEmbeddingPlaceholders(template.getKnn())
        || citesEmbeddingPlaceholders(template.getAggregations())
        || citesEmbeddingPlaceholders(template.getSort())) {
      return true;
    }
    for (String fragment : template.getQueryFragments().values()) {
      if (citesEmbeddingPlaceholders(fragment)) {
        return true;
      }
    }
    return false;
  }

  /** Rejects a merged template that sets a semantic field but resolves to an earlier contract. */
  public static void requireSemanticApiVersion(@Nonnull MergedSearchTemplate template) {
    if (template.getApiVersion() < SEMANTIC_API_VERSION && usesSemanticFields(template)) {
      throw new SearchTemplateException(
          "Search template semantic fields require apiVersion " + SEMANTIC_API_VERSION);
    }
  }

  private static boolean usesSemanticFields(@Nonnull MergedSearchTemplate template) {
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval != null
        && ((retrieval.hasCombine() && StringUtils.isNotBlank(retrieval.getCombine()))
            || (retrieval.hasQuerySource() && StringUtils.isNotBlank(retrieval.getQuerySource()))
            || retrieval.hasAnchor()
            || (retrieval.hasProfileScore() && StringUtils.isNotBlank(retrieval.getProfileScore()))
            || retrieval.hasMinSimilarity()
            || (retrieval.hasVectorBody() && StringUtils.isNotBlank(retrieval.getVectorBody())))) {
      return true;
    }
    if (template.getProfiles() != null
        || template.isProfilesSet()
        || template.getScope() != null
        || citesEmbeddingPlaceholders(template)) {
      return true;
    }
    for (SearchTemplateRoutes.Compiled route : template.getRoutes()) {
      if (route.match() != null) {
        return true;
      }
    }
    return false;
  }

  public static boolean keywordRerank(@Nonnull MergedSearchTemplate template) {
    return COMBINE_KEYWORD_RERANK.equals(effectiveCombine(template));
  }

  /**
   * Bridge presence is the capability. Other retrieval fields may be unset, so emptiness is checked
   * on a retrieval that contains only the bridge.
   */
  @Nonnull
  private static SearchTemplateRetrieval retrievalWithBridge(
      @Nonnull SearchTemplateRetrievalBridge bridge) {
    SearchTemplateRetrieval retrieval = new SearchTemplateRetrieval();
    retrieval.setBridge(bridge);
    return retrieval;
  }

  @Nonnull
  private static SearchTemplateException unsupported(@Nonnull String section) {
    return new SearchTemplateException("Search template section is not supported: " + section);
  }
}
