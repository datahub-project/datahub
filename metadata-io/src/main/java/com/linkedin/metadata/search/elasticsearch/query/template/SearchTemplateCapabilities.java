package com.linkedin.metadata.search.elasticsearch.query.template;

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
  /** Template and Mustache-context contract this process implements. */
  public static final int API_VERSION = 1;

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

  /** {@code embeddingClausesJson} and {@code embeddingField}, in any Mustache tag form. */
  private static final Pattern EMBEDDING_PLACEHOLDER =
      Pattern.compile("\\{\\{\\{?\\s*&?\\s*(?:embeddingClausesJson|embeddingField)\\s*}?}}");

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
    if (apiVersion != null && apiVersion != API_VERSION) {
      throw new SearchTemplateException(
          "Search template apiVersion "
              + apiVersion
              + " is not supported. This release implements "
              + API_VERSION);
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

  public static void requireSupported(
      @Nonnull MergedSearchTemplate template,
      boolean documentRerank,
      @Nonnull SearchTemplateComponentRegistry registry) {
    requireSupported(template, documentRerank, false, registry);
  }

  /**
   * Scroll drops {@code knn}, so a knn section does not fail a scroll. On a search, document rerank
   * is advertised only when the request includes the Document entity.
   */
  public static void requireSupported(
      @Nonnull MergedSearchTemplate template,
      boolean documentRerank,
      boolean scroll,
      @Nonnull SearchTemplateComponentRegistry registry) {
    requireContract(template);
    String code = rejectionCode(template, documentRerank, scroll, registry);
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
   * Bounded reason a merged template cannot run, or null when its sections are runnable. {@code
   * rankingPreset}, {@code rankingWindow}, {@code knn}, {@code mergeJoin}, {@code retrievalMode},
   * or {@code retrievalBridge}.
   */
  @Nullable
  public static String rejectionCode(
      @Nonnull MergedSearchTemplate template,
      boolean documentRerank,
      boolean scroll,
      @Nonnull SearchTemplateComponentRegistry registry) {
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
    // failure. This release reads that shape and does not execute the rerank.
    if (template.getKnn() != null && !scroll && !documentRerank && !keywordRerank(template)) {
      return "knn";
    }
    if (template.getLadder() != null
        && template.getLadder().hasJoin()
        && template.getLadder().getJoin() != null
        && "merge".equalsIgnoreCase(template.getLadder().getJoin())) {
      return "mergeJoin";
    }
    if (template.getScope() != null) {
      return "scope";
    }
    if (citesEmbeddingPlaceholders(template)) {
      return "nativeQuery";
    }
    if (template.getProfiles() != null && combine(template.getRetrieval()) == null) {
      return "profiles";
    }
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval == null) {
      return null;
    }
    String mode = retrieval.hasMode() ? retrieval.getMode() : null;
    if (mode != null && !mode.isBlank() && !"keyword".equals(mode)) {
      return "retrievalMode";
    }
    String combine = combine(retrieval);
    if (combine != null && !COMBINE_KEYWORD_RERANK.equals(combine)) {
      return "retrievalCombine";
    }
    if (retrieval.hasVectorBody() && StringUtils.isNotBlank(retrieval.getVectorBody())) {
      return "vectorBody";
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
      case "retrievalCombine" -> "retrieval combine " + retrieval.getCombine();
      case "vectorBody" -> "retrieval vectorBody";
      case "scope" -> "scope";
      case "profiles" -> "profiles on a keyword rung";
      case "nativeQuery" -> "native embedding placeholders";
      case "retrievalBridge" -> "retrieval bridge";
      default -> code;
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
   * True when a body cites the native embedding placeholders, which only the later native mixed
   * query renders.
   */
  public static boolean citesEmbeddingPlaceholders(@Nullable String body) {
    return body != null && EMBEDDING_PLACEHOLDER.matcher(body).find();
  }

  private static boolean citesEmbeddingPlaceholders(@Nonnull MergedSearchTemplate template) {
    if (citesEmbeddingPlaceholders(template.getQuery())
        || citesEmbeddingPlaceholders(template.getClauses())
        || citesEmbeddingPlaceholders(template.getRescore())) {
      return true;
    }
    for (String fragment : template.getQueryFragments().values()) {
      if (citesEmbeddingPlaceholders(fragment)) {
        return true;
      }
    }
    return false;
  }

  public static boolean keywordRerank(@Nonnull MergedSearchTemplate template) {
    return COMBINE_KEYWORD_RERANK.equals(combine(template.getRetrieval()));
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
