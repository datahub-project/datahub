package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.search.template.SearchTemplateRanking;
import com.linkedin.search.template.SearchTemplateRetrieval;
import com.linkedin.search.template.SearchTemplateRetrievalBridge;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Sections this process can run. OSS advertises keyword retrieval and engine rescore. A stored
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
              "normalization",
              "fieldOverrides",
              "highlights",
              "sort",
              "boosts",
              "queryFragments"));

  public static final Set<String> PRESETS = Set.of("none", "usage", "usageAndMatchTier");
  public static final Set<String> SIZERS = Set.of("fixed", "tokenCurve");
  public static final Set<String> FUSIONS =
      Set.of("interleave", "weightedBlend", "fillFreeSlots", "keywordPreferred");
  public static final Set<String> FACETS = Set.of("firstMember", "patchFromMembers", "candidates");
  public static final Set<String> MODES = Set.of("keyword", "vector");

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
      @Nonnull MergedSearchTemplate template, boolean documentRerank) {
    requireSupported(template, documentRerank, false);
  }

  /**
   * Scroll drops {@code knn}, so a knn section does not fail a scroll. On a search, document rerank
   * is advertised only when the request includes the Document entity.
   */
  public static void requireSupported(
      @Nonnull MergedSearchTemplate template, boolean documentRerank, boolean scroll) {
    requireContract(template);
    String code = rejectionCode(template, documentRerank, scroll);
    if (code != null) {
      throw unsupported(rejectionDetail(template, code));
    }
  }

  /**
   * Bounded reason a merged template cannot run, or null when its sections are runnable. {@code
   * rankingPreset}, {@code rankingWindow}, {@code knn}, {@code mergeJoin}, {@code retrievalMode},
   * or {@code retrievalBridge}.
   */
  @Nullable
  public static String rejectionCode(
      @Nonnull MergedSearchTemplate template, boolean documentRerank, boolean scroll) {
    SearchTemplateRanking ranking = template.getRanking();
    if (ranking != null) {
      String preset = ranking.hasPreset() ? ranking.getPreset() : null;
      if (preset != null && !preset.isBlank() && !"none".equals(preset)) {
        return "rankingPreset";
      }
      String sizer = ranking.hasWindowSizer() ? ranking.getWindowSizer() : null;
      if (sizer != null && !sizer.isBlank() && !"fixed".equals(sizer)) {
        return "rankingWindow";
      }
    }
    if (template.getKnn() != null && !scroll && !documentRerank) {
      return "knn";
    }
    if (template.getLadder() != null
        && template.getLadder().hasJoin()
        && template.getLadder().getJoin() != null
        && "merge".equalsIgnoreCase(template.getLadder().getJoin())) {
      return "mergeJoin";
    }
    SearchTemplateRetrieval retrieval = template.getRetrieval();
    if (retrieval == null) {
      return null;
    }
    String mode = retrieval.hasMode() ? retrieval.getMode() : null;
    if (mode != null && !mode.isBlank() && !"keyword".equals(mode)) {
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
      case "rankingPreset" -> "ranking preset " + ranking.getPreset();
      case "rankingWindow" -> "ranking windowSizer " + ranking.getWindowSizer();
      case "knn" -> "knn";
      case "mergeJoin" -> "merge join";
      case "retrievalMode" -> "retrieval mode " + retrieval.getMode();
      case "retrievalBridge" -> "retrieval bridge";
      default -> code;
    };
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
